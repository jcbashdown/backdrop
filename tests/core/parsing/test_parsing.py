import unittest
from hamcrest import only_contains, assert_that
from backdrop.core.errors import ParseError
from backdrop.core.parsing import make_records


class TestMakeRecords(unittest.TestCase):
    def test_make_records_from_rows(self):
        rows = [
            ["name", "size"],
            ["bottle", 123],
            ["screen", 567],
            ["mug", 12],
        ]

        records = make_records(rows)

        assert_that(records, only_contains(
            {"name": "bottle", "size": 123},
            {"name": "screen", "size": 567},
            {"name": "mug", "size": 12},
        ))

    def test_fail_if_a_row_contains_more_values_than_the_first_row(self):
        rows = [
            ["name", "size"],
            ["bottle", 123],
            ["screen", 567, 8],
        ]

        with self.assertRaises(ParseError):
            list(make_records(rows))

    def test_fail_if_a_row_contains_fewer_values_than_the_first_row(self):
        rows = [
            ["name", "size"],
            ["bottle", 123],
            ["screen"],
        ]

        with self.assertRaises(ParseError):
            list(make_records(rows))

    def test_works_if_given_an_iterator(self):
        def rows():
            yield ("name", "size")
            yield ("bottle", 123)
            yield ("screen", 567)

        records = list(make_records(rows()))

        assert_that(records, only_contains(
            {"name": "bottle", "size": 123},
            {"name": "screen", "size": 567},
        ))
