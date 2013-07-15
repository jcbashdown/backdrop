import csv
from .errors import ParseError


def parse_csv(incoming_data, schema=None):
    return list(
        parse_rows(
            ignore_comment_column(unicode_csv_dict_reader(
                ignore_comment_lines(lines(incoming_data)), 'utf-8')
            ), schema
        )
    )


def lines(stream):
    for candidate_line in stream:
        for line in candidate_line.splitlines(True):
            yield line


def is_empty_row(row):
    return all(not v for v in row.values())


def parse_rows(data, schema):
    schema_validator = _process_schema_definition(schema)

    for datum in data:
        if None in datum.keys():
            raise ParseError(
                'Some rows ins the CSV file contain more values than columns')
        if None in datum.values():
            raise ParseError(
                'Some rows in the CSV file contain fewer values than columns')
        if not is_empty_row(datum):
            if schema_validator:
                if schema_validator.is_valid(datum):
                    yield schema_validator.typed(datum)
                else:
                    raise ParseError(
                        "Invalid data did not match the expected schema %s"
                        % schema_validator.errors)
            else:
                yield datum


def ignore_comment_lines(reader):
    for line in reader:
        if not line.startswith('#'):
            yield line


def ignore_comment_column(data):
    for d in data:
        if "comment" in d:
            del d["comment"]
        yield d


def _process_schema_definition(schema):
    if schema is None:
        return None

    return CsvSchemaValidator(schema)


class UnicodeCsvReader(object):
    def __init__(self, reader, encoding):
        self._reader = reader
        self._encoding = encoding

    def next(self):
        try:
            return [self._decode(cell) for cell in self._reader.next()]
        except UnicodeError:
            raise ParseError("Non-UTF8 characters found.")

    @property
    def line_num(self):
        return self._reader.line_num

    def _decode(self, cell):
        return unicode(cell, self._encoding)


def unicode_csv_dict_reader(incoming_data, encoding):
    r = csv.DictReader(incoming_data)
    r.reader = UnicodeCsvReader(r.reader, encoding)
    return r


import collections
import re
import datetime

MAPPING_FUNCTIONS = collections.defaultdict(lambda: lambda x: x)
MAPPING_FUNCTIONS["int"] = int
MAPPING_FUNCTIONS["date"] = lambda val: \
    datetime.datetime.strptime(val, "%Y-%m-%d")
MAPPING_FUNCTIONS["datetime"] = lambda val: \
    datetime.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S")

VALID_FORMATS = collections.defaultdict(lambda: lambda raw_val: True)
VALID_FORMATS["int"] = lambda raw_val: re.match("^-?\d+$", raw_val)
VALID_FORMATS["date"] = lambda raw_val: \
    re.match("^\d{4}-\d{2}-\d{2}$", raw_val)
VALID_FORMATS["datetime"] = lambda raw_val: \
    re.match("^\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}$", raw_val)


class CsvSchemaValidator(object):
    """
    PoC to show how we can validate incoming data (and thus reject malformed
    content, keep our IL level suitably low, etc.
    """
    def __init__(self, schema):
        self.mapping_functions = [MAPPING_FUNCTIONS[x.get("type", "string")]
                                  for x in schema]
        self.validation_functions = [VALID_FORMATS[x.get("type", "string")]
                                     for x in schema]
        self.errors = []

    def is_valid(self, datum):
        """
        Iterate each field in the datum, checking that it is valid according
        to the field definition
        """
        self.errors = [value for validate, value in
                       zip(self.validation_functions, datum.values())
                       if not validate(value)]
        return len(self.errors) == 0

    def typed(self, datum):
        """
        Iterate each field in the datum, converting to a concrete type where
        applicable
        """
        # return datum
        return dict(zip(datum.keys(),
                    [convert(value) for convert, value in
                    zip(self.mapping_functions, datum.values())]))
