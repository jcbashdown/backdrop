import csv
from .errors import ParseError, ValidationError


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
                yield schema_validator.validate(datum)
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

import re
import datetime
from decimal import Decimal


class BaseField:
    def __init__(self, definition):
        self.definition = definition
        self.converted_value = None

    def is_valid(self, raw_value):
        pass

    def convert(self, raw_value):
        pass


class StringField(BaseField):
    def is_valid(self, raw_value):
        return True

    def convert(self, raw_value):
        return raw_value


class IntField(BaseField):
    def is_valid(self, raw_value):
        if not re.match("^-?\d+$", raw_value):
            return False
        self.converted_value = int(raw_value)

        # Should we collect errors and pass all back? For now, first failure
        # causes an exception.
        min_value = self.definition.get('min', None)
        if min_value is not None and self.converted_value < min_value:
            raise ValidationError(u"Expected to be >= %d but was %d"
                                  % (min_value, self.converted_value))

        max_value = self.definition.get('max', None)
        if max_value is not None and self.converted_value > max_value:
            raise ValidationError(u"Expected to be <= %d but was %d"
                                  % (max_value, self.converted_value))

        return True

    def convert(self, raw_value):
        return self.converted_value


class DecimalField(BaseField):
    def is_valid(self, raw_value):
        if not re.match("^-?\d*.?\d*$", raw_value):
            return False
        self.converted_value = Decimal(raw_value)

        return True

    def convert(self, raw_value):
        return self.converted_value


class DateField(BaseField):
    def is_valid(self, raw_value):
        return re.match("^\d{4}-\d{2}-\d{2}$", raw_value)

    def convert(self, raw_value):
        return datetime.datetime.strptime(raw_value, "%Y-%m-%d")


class DatetimeField(BaseField):
    def is_valid(self, raw_value):
        return re.match("^\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}$", raw_value)

    def convert(self, raw_value):
        return datetime.datetime.strptime(raw_value, "%Y-%m-%dT%H:%M:%S")

import collections

FIELD_HANDLERS = collections.defaultdict(lambda:
                                         lambda options: StringField(options))
FIELD_HANDLERS["int"] = lambda options: IntField(options)
FIELD_HANDLERS["decimal"] = lambda options: DecimalField(options)
FIELD_HANDLERS["date"] = lambda options: DateField(options)
FIELD_HANDLERS["datetime"] = lambda options: DatetimeField(options)


class CsvSchemaValidator(object):
    """
    PoC to show how we can validate incoming data (and thus reject malformed
    content, keep our IL level suitably low, etc.
    """
    def __init__(self, schema):
        self.field_handlers = [FIELD_HANDLERS[x.get("type", "string")](x)
                               for x in schema]
        self.errors = []

    def validate(self, datum):
        if self._is_valid(datum):
            return self._typed(datum)

        raise ValidationError(
            "Invalid data did not match the expected schema %s"
            % self.errors)

    def _is_valid(self, datum):
        """
        Iterate each field in the datum, checking that it is valid according
        to the field definition
        """
        self.errors = [value for handler, value in
                       zip(self.field_handlers, datum.values())
                       if not handler.is_valid(value)]
        return len(self.errors) == 0

    def _typed(self, datum):
        """
        Iterate each field in the datum, converting to a concrete type where
        applicable
        """
        return dict(zip(datum.keys(),
                    [handler.convert(value) for handler, value in
                    zip(self.field_handlers, datum.values())]))
