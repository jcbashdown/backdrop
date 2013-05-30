import logging
from bson import Code
import pymongo
from pymongo.errors import AutoReconnect
from backdrop.monitoring import statsd
from backdrop.core import backdrop_time as backdrop_time


class Database(object):
    def __init__(self, host, port, name):
        self._mongo = pymongo.MongoClient(host, port)
        self.name = name

    def alive(self):
        return self._mongo.alive()

    def get_repository(self, bucket_name):
        return Repository(MongoDriver(self._mongo[self.name][bucket_name]))

    @property
    def connection(self):
        return self._mongo[self.name]


class MongoDriver(object):
    def __init__(self, collection):
        self._collection = collection
        self.sort_options = {
            "ascending": pymongo.ASCENDING,
            "descending": pymongo.DESCENDING
        }

    def _apply_sorting(self, cursor, key, direction):
        if direction not in self.sort_options.keys():
            raise InvalidSortError(direction)

        cursor.sort(key, self.sort_options[direction])

    def find(self, query, sort, limit):
        cursor = self._collection.find(query)
        self._apply_sorting(cursor, sort[0], sort[1])
        if limit:
            cursor.limit(limit)
        return cursor

    def _ignore_docs_without_grouping_keys(self, keys, query):
        for key in keys:
            if key not in query:
                query[key] = {"$ne": None}
        return query

    def group(self, keys, query, collect):
        return self._collection.group(
            key=keys,
            condition=self._ignore_docs_without_grouping_keys(keys, query),
            initial=self._build_accumulator_initial_state(collect),
            reduce=self._build_reducer_function(collect)
        )

    def _build_collector_code(self, collect):
        template = "if (current.{c} !== undefined) " \
                   "{{ previous.{c}.push(current.{c}); }}"
        code = [template.format(c=collect_me) for collect_me in collect]
        return "\n".join(code)

    def _build_accumulator_initial_state(self, collect):
        initial = {'_count': 0}
        for collect_me in collect:
            initial.update({collect_me: []})
        return initial

    def _build_reducer_function(self, collect):
        reducer_skeleton = "function (current, previous)" + \
                           "{{ previous._count++; {collectors} }}"
        reducer_code = reducer_skeleton.format(
            collectors=self._build_collector_code(collect)
        )
        reducer = Code(reducer_code)
        return reducer

    def save(self, obj, tries=3):
        try:
            self._collection.save(obj)
        except AutoReconnect:
            logging.warning("AutoReconnect on save")
            statsd.incr("db.AutoReconnect", bucket=self._collection.name)
            if tries > 1:
                self.save(obj, tries - 1)
            else:
                raise


class Repository(object):
    def __init__(self, mongo):
        self._mongo = mongo

    def _validate_sort(self, sort):
        if len(sort) != 2:
            raise InvalidSortError("Expected a key and direction")

        if sort[1] not in ["ascending", "descending"]:
            raise InvalidSortError(sort[1])

    def find(self, query, sort=None, limit=None):
        if not sort:
            sort = ["_timestamp", "ascending"]

        self._validate_sort(sort)

        return self._mongo.find(query.to_mongo_query(), sort, limit)

    def group(self, group_by, query, sort=None, limit=None, collect=None):
        if sort:
            self._validate_sort(sort)
        return self._group(
            [group_by],
            query.to_mongo_query(),
            sort,
            limit,
            collect or [])

    def save(self, obj):
        obj['_updated_at'] = backdrop_time.now()
        self._mongo.save(obj)

    def multi_group(self, key1, key2, query,
                    sort=None, limit=None, collect=None):
        if key1 == key2:
            raise GroupingError("Cannot group on two equal keys")
        results = self._group(
            [key1, key2],
            query.to_mongo_query(),
            sort,
            limit,
            collect or [])

        return results

    def _require_keys_in_query(self, keys, query):
        for key in keys:
            if key not in query:
                query[key] = {"$ne": None}
        return query

    def _group(self, keys, query, sort=None, limit=None, collect=None):
        results = self._mongo.group(keys, query, collect)

        results = nested_merge(keys, collect, results)

        if sort:
            sorters = {
                "ascending": lambda a, b: cmp(a, b),
                "descending": lambda a, b: cmp(b, a)
            }
            sorter = sorters[sort[1]]
            try:
                results.sort(cmp=sorter, key=lambda a: a[sort[0]])
            except KeyError:
                raise InvalidSortError('Invalid sort key {0}'.format(sort[0]))
        if limit:
            results = results[:limit]

        return results


class GroupingError(ValueError):
    pass


class InvalidSortError(ValueError):
    pass


def extract_collected_values(collect, result):
    collected = {}
    for collect_field in collect:
        collected[collect_field] = result.pop(collect_field)
    return collected, result


def insert_collected_values(collected, group):
    for collect_field in collected.keys():
        if collect_field not in group:
            group[collect_field] = set()
        group[collect_field].update(collected[collect_field])


def convert_collected_values_to_list(collect, groups):
    for group in groups:
        for collected_field in collect:
            group[collected_field] = sorted(list(group[collected_field]))


def nested_merge(keys, collect, results):
    groups = []
    for result in results:
        collected, result = extract_collected_values(collect, result)

        groups, group = _merge(groups, keys, result)

        insert_collected_values(collected, group)

    convert_collected_values_to_list(collect, groups)
    return groups


def _merge(groups, keys, result):
    keys = list(keys)
    key = keys.pop(0)
    is_leaf = (len(keys) == 0)
    value = result.pop(key)

    group = _find_group(group for group in groups if group[key] == value)
    if not group:
        if is_leaf:
            group = _new_leaf_node(key, value, result)
        else:
            group = _new_branch_node(key, value)
        groups.append(group)

    if not is_leaf:
        _merge_and_sort_subgroup(group, keys, result)
        _add_branch_node_counts(group)
    return groups, group


def _find_group(items):
    """Return the first item in an iterator or None"""
    try:
        return next(items)
    except StopIteration:
        return


def _new_branch_node(key, value):
    """Create a new node that has further sub-groups"""
    return {
        key: value,
        "_subgroup": []
    }


def _new_leaf_node(key, value, result):
    """Create a new node that has no further sub-groups"""
    result[key] = value
    return result


def _merge_and_sort_subgroup(group, keys, result):
    group['_subgroup'], _ = _merge(group['_subgroup'], keys, result)
    group['_subgroup'].sort(key=lambda d: d[keys[0]])


def _add_branch_node_counts(group):
    group['_count'] = sum(doc.get('_count', 0) for doc in group['_subgroup'])
    group['_group_count'] = len(group['_subgroup'])
