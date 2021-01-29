#!/usr/bin/python3
# pylint: disable=bad-continuation,stop-iteration-return
"""
Author: Konstantin Glukhov

This script convert CSV file to QIF

Tipical usage:

csv2qif.py --help

csv2qif.py \
        --csv input.csv,encoding=sjis,slice=8:,dtFmt=%Y年%m月%d日 \
        --qif type=CCard,encoding=utf8 \
        --fieldMap Date:0,Amount:3,Payee:4,Memo:5,Cleared:R

QIF (Quicken Interchange Format):

!Type:[type identifier string]
[single character line code]Literal String Data
...
^
[single character line code]Literal String Data
...
^
Each record ends with a ^

Code        Description
!Type:Cash  Cash Flow: Cash Account
!Type:Bank  Cash Flow: Checking & Savings Account
!Type:CCard Cash Flow: Credit Card Account
!Type:Invst Investing: Investment Account

Code Description
D    Date
T    Amount
U    Identical to T
M    Memo
C    Cleared status. blank (not cleared), "*" or "c" (cleared) and "X" or "R" (reconciled)
N    Number of the check. Can also be "Deposit", "Transfer", "Print", "ATM", "EFT"
P    Payee
A    Address of Payee. Up to 5 address lines are allowed. A 6th address line is a message
     that prints on the check.
L    Category or Transfer and (optionally) Class. The literal values are those defined in
     the Quicken Category list.
     SubCategories can be indicated by a colon (":") followed by the subcategory literal.
     If the Quicken file uses Classes, this can be indicated by a slash ("/") followed by
     the class literal.
     For Investments, MiscIncX or MiscExpX actions, Category/class or transfer/class.
     (40 characters maximum)
F    Flag this transaction as a reimbursable business expense.
S    Split category. Same format as L (Categorization) field. (40 characters maximum)
E    Split memo any text to go with this split item.
$    Amount for this split of the item. Same format as T field.
%    Percent. Optional used if splits are done by percentage.
N    Investment Action (Buy, Sell, etc.).
Y    Security name
I    Price
Q    Quantity of shares (or split ratio, if Action is StkSplit).
O    Commission cost (generally found in stock trades)
$    Amount transferred, if cash is moved between accounts
B    Budgeted amount - may be repeated many times for monthly budgets

Example:

!Type:CCard
D12/17/2018
T-67.02
PSAIBU GAS FUKUOKA
MThis is a memo
C*
^
"""

import argparse
import csv
import itertools
import logging
import os
from datetime import datetime
from typing import Any, List, Dict, Generator, Iterator, Callable, Tuple

# Predefined CSV formats
companies = {
    'JP-Post': {
        'Bank': {
            'encoding': 'sjis',
            'dtFmt': '%Y%m%d',
            'slice': '7:',
            'fieldMap': {
                'Date': 0,
                'Credit': 2,
                'Debit': 3,
                'Payee': 4,
                'Memo': 5,
            },
        },
    },
    'Shinsei': {
        'Bank': {
            'encoding': 'sjis',
            'dtFmt': '%Y/%m/%d',
            'slice': '1:',
            'fieldMap': {'Date': 0, 'Memo': 1, 'Debit': 2, 'Credit': 3},
        },
    },
    'EPOS': {
        'CCard': {
            'encoding': 'sjis',
            'dtFmt': '%Y年%m月%d日',
            'slice': '2:-4',
            'fieldMap': {'Date': 1, 'Payee': 2, 'Debit': 4},
        },
    },
}

# Valid QIF codes
FIELD_CODE = {
    'Date': 'D',
    'Amount': 'T',
    'Debit': 'T-',
    'Credit': 'T',
    'Payee': 'P',
    'Memo': 'M',
    'Action': 'N',
    'Security': 'Y',
    'Price': 'I',
    'Quantity': 'Q',
    'Commission': 'O',
    'Cleared': 'C',
}

logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO,
)
LOG = logging.getLogger()

MSG_BAD_VAL = "'%s' is an invalid value for option %s\nValid values: %s"

class ArgumentError(argparse.ArgumentError):
    "Can be raised when calling ArgumentParser methods"

    def __init__(self, message):
        super().__init__(None, message)


class ArgumentParser(argparse.ArgumentParser):
    "Override argparse.ArgumentParser.error() to call print_help()"
    def error(self, message):
        "Can be called by a ArgumentParser instance"

        print('\nError: ' + message + '\n')
        self.print_help()
        print()
        exit(1)


def parse_dict(  # pylint: disable=R0913
    arg_value: str,
    arg_name: str,
    valid_values: Tuple,
    value_type: Any = str,
    item_sep: str = ',',
    val_sep: str = ':',
    tuple_item_getter: Callable = lambda x: x,
) -> Dict[str, Any]:
    "Parse argument string into dictionary and raise ArgumentError if arg_name has bad_items"
    try:
        dic = {
            key: value_type(val)
            for key, val in (
                element.split(val_sep)
                for element in arg_value.split(item_sep)
            )
        }
        bad_items = []
        for item in dic.items():
            if tuple_item_getter(item) not in valid_values:
                bad_items.append(item)
        if bad_items:
            raise ValueError

        return dic
    except ValueError:
        valid_values_str = ', '.join(':'.join(x) for x in valid_values)
        raise ArgumentError(MSG_BAD_VAL % (arg_value, arg_name, valid_values_str))


def parse_file_options(
    arg_value: str,
    arg_name: str,
    valid_dict: Dict,
    item_sep: str = ',',
    val_sep: str = ':',
) -> Dict[str, Any]:
    '''
    Parse argument string into dictionary
    key without value gets 'file' key, and key becomes value
    '''
    options = {**valid_dict}
    valid_values = tuple(valid_dict)
    for option in arg_value.split(item_sep):
        key, _, val = option.partition(val_sep)
        if val:
            if key not in valid_dict:
                raise ArgumentError(MSG_BAD_VAL % (key, arg_name, valid_values))
            options[key] = val
        else:
            options['file'] = key
    return options


class ParseArgs:  # pylint: disable=too-few-public-methods
    "Argument Parser"

    def __init__(self):

        # File options
        file_options = {
            'file': None,
            'encoding': 'utf8',
            'dtFmt': '%Y-%m-%d',
        }

        # Valid QIF file types
        qif_types = (
            'Cash',
            'Bank',
            'CCard',
            'Invst',
        )

        class NameType:
            "Data class to hold name:type pair from a dictonary with one element"

            def __init__(self, singleDict: Dict[str, str]):
                if len(singleDict) > 1:
                    raise ArgumentError("Only one name:type is accepted with --company.")
                ((self.name, self.type),) = singleDict.items()

            def __str__(self):
                return f"{self.name}:{self.type}"

        self.csv: Dict[str, Any]
        self.qif: Dict[str, Any]
        self.fieldMap: Dict[str, str]  # pylint: disable=invalid-name
        self.company: NameType
        self.debug: int

        parser = ArgumentParser()

        parser.add_argument('-d', '--debug', action='count', help=argparse.SUPPRESS)

        parser.add_argument(
            '--csv',
            type=lambda v: parse_file_options(
                arg_value=v,
                arg_name='--csv',
                valid_dict={**file_options, **{'slice': None}},
                val_sep='=',
            ),
            help=(
                'CSV input file name followed by comma-separated key=value pairs of options'
                f'{tuple(file_options)}, e.g. data.csv,encoding=sjis,slice=1:,dtFmt=%%Y%%m%%d'
            ),
            required=True,
        )

        options = {**file_options, **{'type': 'CCard'}}
        parser.add_argument(
            '--qif',
            type=lambda v: parse_file_options(
                arg_value=v,
                arg_name='--qif',
                valid_dict=options,
                val_sep='=',
            ),
            default=options,
            help=(
                'QIF output file name followed by comma-separated key=value pairs of options,'
                'e.g. my.qif,encoding=sjis'
            ),
        )

        mutually_exclusive = parser.add_mutually_exclusive_group(required=True)

        mutually_exclusive.add_argument(
            '--fieldMap',
            type=lambda v: parse_dict(
                arg_value=v,
                arg_name='--fieldMap',
                valid_values=FIELD_CODE,
                tuple_item_getter=lambda x: x[0],
            ),
            help=(
                f'comma-separated fieldName:position pairs. Valid fieldMap {tuple(FIELD_CODE)}'
                ', e.g. Date:0,Amount:2,Payee:5,Memo:9.'
            ),
        )

        mutually_exclusive.add_argument(
            '--company',
            type=lambda v: NameType(
                parse_dict(
                    arg_value=v,
                    arg_name='--company',
                    valid_values=tuple((k, x) for k, v in companies.items() for x in v),
                )
            ),
            help='The predefined Financial Institution name:type pair, e.g. Chase:Bank',
        )

        parser.parse_args(namespace=self)

        if self.debug:
            LOG.setLevel(logging.DEBUG)

        LOG.debug('--company %s', self.company)
        if self.company:
            self.qif['type'] = self.company.type
            self.csv['encoding'] = companies[self.company.name][self.company.type][
                'encoding'
            ]
            self.csv['slice'] = companies[self.company.name][self.company.type]['slice']
            self.csv['dtFmt'] = companies[self.company.name][self.company.type]['dtFmt']
            self.fieldMap = companies[self.company.name][self.company.type]['fieldMap']

        if self.qif['file'] is None:
            self.qif['file'] = (
                '%s.qif' % os.path.splitext(
                    os.path.abspath(self.csv['file']))[0]
            )

        LOG.debug('--csv %s', ','.join(f'{k}:{v}' for k, v in self.csv.items()))
        LOG.debug('--qif %s', ','.join(f'{k}:{v}' for k, v in self.qif.items()))
        LOG.debug('--fieldMap %s', ','.join(f'{k}:{v}' for k, v in self.fieldMap.items()))

        if self.csv['slice'] is not None:
            self.csv['slice'] = slice(
                *[int(x) if x != '' else None for x in self.csv['slice'].split(':')]
            )
            if self.csv['slice'].step is not None:
                parser.error('--csv slice=START:STOP:STEP (step is not implemented)')
        else:
            self.csv['slice'] = slice(None)

        if self.qif['type'] not in qif_types:
            parser.error(MSG_BAD_VAL % (self.qif['type'], '--qif type:', qif_types))

        fields = set(self.fieldMap)
        if 'Amount' in fields and set('Credit', 'Debit').issubset(fields):
            parser.error('Amount is mutually exclusive with Debit and Credit')


def csv2qif(
    qif_type: str,
    csv_iterator: Iterator,
    field_map: Dict,
    csv_date_fmt: str,
    qif_date_fmt: str,
) -> Generator[str, None, None]:
    "Convert CSV iterator to QIF iterator"
    yield f'!Type:{qif_type}'
    row_cnt = 0
    constant_fields = {}
    csv_fields = {}
    for key, val in field_map.items():
        if isinstance(val, int) or (isinstance(val, str) and val.isnumeric()):
            csv_fields[key] = int(val)
        else:
            constant_fields[key] = val
    for row in csv_iterator:
        fields = {f: row[i] for f, i in csv_fields.items()}
        if not (  # Skip if all amounts are missing
            fields.get('Amount', False)
            or fields.get('Credit', False)
            or fields.get('Debit', False)
        ):
            continue
        # Convert CSV date format to QIF format if differ
        if csv_date_fmt != qif_date_fmt:
            fields['Date'] = datetime.strftime(
                datetime.strptime(fields['Date'], csv_date_fmt), qif_date_fmt
            )
        fields.update(constant_fields)
        LOG.debug(fields)
        for key, val in fields.items():
            # Debit and Credit are mutually exclusive
            # No reason to have an empty entry in QIF file, skip it
            if key in ('Debit', 'Credit') and not val:
                continue
            yield f'{FIELD_CODE[key]}{val}'
        yield '^'
        row_cnt += 1
    LOG.debug('QIF records: %d', row_cnt)


def slicer(iterator: Iterator, start: int, stop: int = None) -> Generator[Any, None, None]:
    "Slice generator function"
    if stop == 0:
        return
    if start is None:
        start = 0

    if stop is None or stop > 0:
        for element in itertools.islice(iterator, start, stop):
            yield element
    else:
        try:
            for _ in range(start):  # skip 'start' elements of 'iterator'
                next(iterator)
            # split iterator into 'elements' and 'counter'
            elements, counter = itertools.tee(iterator)
            for _ in range(0 - stop):  # skip 'stop' elements in 'counter'
                next(counter)
            for _ in counter:  # iterate remaining elements but no more than 'counter'
                yield next(elements)
        except StopIteration:
            return


def csv_reader(csv_iter: Iterator[str]) -> Generator[List[str], None, None]:
    "Converts an string iterator to CSV iterator"
    # Initialize csv.reader
    reader = csv.reader(csv_iter, dialect='excel', skipinitialspace=True)
    row_cnt = 0
    for row in reader:
        row_cnt += 1
        yield row
    LOG.debug('CSV records: %d', row_cnt)


def file_writer(iterator: Iterator, file_name: str, encoding: str) -> None:
    "Writes iterator to file"
    # Open output file
    with open(file_name, mode='w', encoding=encoding) as file_out:
        for line in iterator:
            print(line, file=file_out)


def main(arg: ParseArgs) -> None:
    "Converts CSV to QIF file"
    # Open CSV file
    with open(arg.csv['file'], mode='r', encoding=arg.csv['encoding']) as csv_file:
        file_writer(  # Write QIF file (Step 4)
            iterator=csv2qif(  # Convert CSV to QIF (Step 3)
                qif_type=arg.qif['type'],
                csv_iterator=csv_reader(  # Parse slice as CSV (Step 2)
                    csv_iter=slicer(  # Read and slice CSV file (Step 1)
                        iterator=csv_file,
                        start=arg.csv['slice'].start,
                        stop=arg.csv['slice'].stop,
                    ),
                ),
                field_map=arg.fieldMap,
                csv_date_fmt=arg.csv['dtFmt'],
                qif_date_fmt=arg.qif['dtFmt'],
            ),
            file_name=arg.qif['file'],
            encoding=arg.qif['encoding'],
        )


def main_alt(arg: ParseArgs) -> None:
    "Converts CSV to QIF file"
    # Open CSV file for reading and QIF file for writing
    with open(arg.csv['file'], mode='r', encoding=arg.csv['encoding']) as (
        csv_file
    ), open(arg.qif['file'], mode='w', encoding=arg.qif['encoding']) as (
        qif_file
    ):
        iterator = csv2qif(  # Convert CSV to QIF (Step 3)
            qif_type=arg.qif['type'],
            csv_iterator=csv_reader(  # Parse slice as CSV (Step 2)
                csv_iter=slicer(  # Read and slice CSV file (Step 1)
                    iterator=csv_file,
                    start=arg.csv['slice'].start,
                    stop=arg.csv['slice'].stop,
                ),
            ),
            field_map=arg.fieldMap,
            csv_date_fmt=arg.csv['dtFmt'],
            qif_date_fmt=arg.qif['dtFmt'],
        )
        for line in iterator:  # Write QIF file (Step 4)
            print(line, file=qif_file)


if __name__ == '__main__':
    try:
        ARG = ParseArgs()
        main_alt(ARG)
    except ArgumentError:
        pass
