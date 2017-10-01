import operator
import os
import pathlib
import io
import pickle

from ctypes import *
from ctypes import LittleEndianStructure, c_uint32, c_ubyte, c_bool

import collections
from functools import total_ordering
from sortedcontainers import SortedDict


# A "virtual" column
# This is meant to be abstract, and implement any functionality common among all column types

class VColumn:
    def __init__(self, *args, **kwargs):
        # A property to delegate reads and writes to the value in a row to our column object
        class Property:
            def __get__(prop, instance, owner):
                return self.get(instance)

            def __set__(prop, instance, value):
                self.set(instance, value)

                # if we edit a column, make the row to be written next time we save
                # keep this after the set in case an exception is thrown
                instance._dirty = True

        self.Property = Property

    # this is our earliest chance to capture which table we're on
    def __set_name__(self, owner, name):
        self.name = name
        self.table = owner

    def __eq__(self, other):
        return ColEq(self, other)

    def __lt__(self, other):
        return ColLt(self, other)

    def __gt__(self, other):
        return ColGt(self, other)

    def __le__(self, other):
        return ColLe(self, other)

    def __ge__(self, other):
        return ColGe(self, other)

    # return the value of this column on the given row
    # should be implemented on any non-abstract column
    def get(self, row):
        raise NotImplementedError()

    # update the value of this column on the given row
    # can be ignored to make a column read only
    def set(self, row, value):
        raise NotImplementedError()


# A "concrete" column. That is, one that is actually stored on disk

class Column(VColumn):
    # save the arguments to make this column to allow us to serialize
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        super().__init__(*args, **kwargs)

    # we store the value on the Python object for speed of access.
    def get(self, row):
        return getattr(row, '_' + self.name)

    # store the value, and update relevant indexes
    def set(self, row, value):
        # find all indices on this column
        indices = [index for index in row.table.indices if self.name in index.keys]
        # if the row isn't yet loaded that means it hasn't been indexed
        if row._loaded:
            for index in indices:
                # remove us from the indexes
                index.remove(row)
        setattr(row, '_' + self.name, value)
        if row._loaded:
            for index in indices:
                # then put us back on
                index.add(row)

    # called once when a row is loaded from disk
    # used to bridge from raw byte types to Python data
    def load(self, row):
        pass

    # called once when a row is about to be written to disk
    # used to bridge from raw byte types to Python data
    def dump(self, row):
        pass

    # called once when a table is loaded from disk
    def load_col(self):
        pass

    # called once when a table is about to be written to disk
    def dump_col(self):
        pass

    def __getstate__(self):
        return {
            'args': self.args,
            'kwargs': self.kwargs,
            'name': self.name
        }

    def __setstate__(self, state):
        self.__init__(*state['args'], **state['kwargs'])
        self.name = state['name']


# A Column wrapping a ctypes primitive
class StructColumn(Column):
    def get(self, row):
        return getattr(row, '__' + self.name).value

    # Since we override the default storage we re-implement the index logic
    # Maybe replace this with a context manager?
    def set(self, row, value):
        indices = [index for index in row.table.indices if self.name in index.keys]
        if row._loaded:
            for index in indices:
                index.remove(row)
        getattr(row, '__' + self.name).value = value
        if row._loaded:
            for index in indices:
                index.add(row)


# Column wrapping an int32
class IntColumn(StructColumn):
    class Struct(LittleEndianStructure):
        _fields_ = [('value', c_int)]


class BoolColumn(StructColumn):
    class Struct(LittleEndianStructure):
        _fields_ = [('value', c_bool)]


# Abstract base class for a column that references
class RemoteColumn(VColumn):
    def __init__(self, remote, *args):
        # If we're passed a string, treat it as a name
        # Otherwise, treat it as the remote table
        if not isinstance(remote, str):
            self._table = remote
            remote = remote.__name__
        else:
            self._table = None
        self.remote = remote
        super().__init__(remote, *args)

    # Find and load our table
    def get_table(self, row):
        for table in row.table.db.tables:
            if table.__name__ == self.remote:
                self._table = table
                break
        return self._table


# Column that holds a foreign key
class ForeignColumn(StructColumn, RemoteColumn):
    class Struct(LittleEndianStructure):
        _fields_ = [('value', c_int)]

    def get(self, row):
        if not self._table:
            self.get_table(row)

        id = getattr(row, '__' + self.name).value
        return self._table.find(self._table.id == id)

    def set(self, row, value):
        if value is None:
            value = 0
        else:
            value = value.id
        return super().set(row, value)


# A column that contains a byte sequence
# This is meant to be abstract parent for classes that require variable length data
class BytesColumn(Column):
    def __init__(self, length, *args, **kwargs):
        class Struct(LittleEndianStructure):
            _fields_ = [('length', c_uint32), ('data', c_ubyte * length)]

        self.length = length
        self.Struct = Struct
        super().__init__(length, *args, **kwargs)

    def store_bytes(self, row, value):
        length = len(value)
        if length > self.length:
            raise ValueError("Received string of {} bytes, maximum {}".format(length, self.length))
        struct = getattr(row, '__' + self.name)
        # save the length
        struct.length = length
        # copy the data into a BytesIO for ease of access
        b = io.BytesIO()
        b.write(struct)

        # Overwrite the data with the new stuff
        offset = self.Struct.data.offset
        b.getbuffer()[offset:offset + length] = value

        # and copy it back into the struct
        b.seek(0)
        b.readinto(struct)

    def load(self, row):
        # copy the bytes out of the struct
        b = io.BytesIO()
        length = getattr(row, '__' + self.name).length
        b.write(getattr(row, '__' + self.name))
        buffer = b.getvalue()[4:4 + length]
        return buffer


# A column that stores a byte sequence in an external file
# This is meant to be abstract parent for classes that require variable length data
class BlobColumn(Column):
    def __init__(self, *args, **kwargs):
        class Struct(LittleEndianStructure):
            _fields_ = []

        self.Struct = Struct
        super().__init__(*args, **kwargs)

    def load_col(self):
        path = self.table.db.path.joinpath(self.table.__name__ + '_' + self.name)
        os.makedirs(path, exist_ok=True)

    def dump_col(self):
        self.load_col()

    def store_bytes(self, row, value):
        path = self.table.db.path.joinpath(self.table.__name__ + '_' + self.name, '{}.dat'.format(row._offset))
        if value is None:
            os.remove(path)
            return
        file = open(path, 'wb')
        file.write(value)

    def load(self, row):
        path = self.table.db.path.joinpath(self.table.__name__ + '_' + self.name, '{}.dat'.format(row._offset))
        try:
            file = open(path, 'rb')
        except IOError:
            return None
        return file.read()


# A column encoded a unicode string
class AbstractStrColumn:
    def set(self, row, value):
        if value is None:
            bytes = b''
        else:
            bytes = value.encode()
        self.store_bytes(row, bytes)
        super().set(row, value)

    def load(self, row):
        buffer = super().load(row)
        if buffer is None:
            string = ''
        else:
            string = buffer.decode()
        setattr(row, '_' + self.name, string)


class StrColumn(AbstractStrColumn, BytesColumn):
    pass


class StrBlobColumn(AbstractStrColumn, BlobColumn):
    pass


# A column storing a pickled object
class AbstractPickleColumn:
    type = None
    default = None

    def set(self, row, value):
        if value is not None and self.type and not isinstance(value, self.type):
            raise ValueError("Expected {}, got {}".format(self.type, value.__class__.__name__))
        if value is None:
            bytes = None
        else:
            bytes = pickle.dumps(value, 4)
        self.store_bytes(row, bytes)
        super().set(row, value)

    def load(self, row):
        body = super().load(row)
        if not body:
            if callable(self.default):
                value = self.default()
            else:
                value = self.default
        else:
            value = pickle.loads(body)
        if value is not None and self.type and not isinstance(value, self.type):
            raise ValueError("Expected {}, got {}".format(self.type, value.__class__.__name__))
        setattr(row, '_' + self.name, value)


class PickleColumn(AbstractPickleColumn, BytesColumn):
    def __init__(self, length=64, type=None, default=None):
        self.type = type
        self.default = default
        super().__init__(length, type)


class PickleBlobColumn(AbstractPickleColumn, BlobColumn):
    def __init__(self, type=None, default=None):
        self.type = type
        self.default = default
        super().__init__(type)


# A Virtual column to access elements through remote columns
class Through(VColumn):
    def __init__(self, *chain):
        self.chain = chain
        super().__init__()

    def get(self, row):
        target = row
        for next in self.chain:
            if isinstance(target, collections.Iterable):
                target = (getattr(i, next) for i in target)
            else:
                target = getattr(target, next)
        return target


# A Virtual column to reverse a ForeignColumn
class Belongs(RemoteColumn, VColumn):
    def __init__(self, remote, key):
        self.key = key
        super().__init__(remote)

    def get(self, row):
        if not self._table:
            self.get_table(row)

        key = getattr(self._table, self.key)
        return self._table.where(key == row)


# An index to allow quick searching of the database
# Mostly a thin wrapper over a skiplist.
class Index:
    def __init__(self, *keys):
        self.keys = keys
        self.keyer = operator.attrgetter(*keys)
        self.list = SortedDict()

    def reindex(self):
        rows = [i for i in self.list.values()]
        self.list = SortedDict()
        for i in rows:
            self.add(i)

    def add(self, object):
        key = self.keyer(object)
        self.list[key] = object

    def remove(self, object):
        key = self.keyer(object)
        del self.list[key]

    def find(self, start, reverse=False):
        if reverse:
            return (self.list[i] for i in self.list.irange(maximum=start, reverse=True))
        return (self.list[i] for i in self.list.irange(start))


# Deep breath...
class Database:
    # our file location
    path = None

    # using inner classes here to cleanly tie our tables to their database
    class Table:
        # whether we need to do a full write rather than incremental
        # generally only true following a migration
        full_dump_needed = False

        # use a metaclass to make defining tables clean
        def __init_subclass__(cls, **kwargs):
            cls.rows = []
            cls.db.tables.append(cls)
            cls.columns = []
            cls.vcolumns = []
            # copy all the columns off the child
            for name, column in cls.__dict__.items():
                if isinstance(column, Column):
                    cls.columns.append(column)
                elif isinstance(column, VColumn):
                    cls.vcolumns.append(column)

            # every table has an implicit ID
            cls.id = IntColumn()
            cls.id.__set_name__(cls, 'id')
            cls.columns.insert(0, cls.id)

            # the fields for our C structure
            fields = []
            for column in cls.columns:
                fields.append(('__' + column.name, column.Struct))

            # little endian structure to ensure portability while being fast on most systems
            # backs the row on a C structure for efficient storage and easy dumping to/from a byte stream
            @total_ordering
            class Row(LittleEndianStructure):
                # fields for our
                _fields_ = fields
                table = cls

                # True means we've changed and need to be written to disk
                __dirty = False

                # True means not yet on disk
                _new = True

                @property
                def _dirty(self):
                    return self.__dirty

                @_dirty.setter
                def _dirty(self, value):
                    if value == self.__dirty:
                        return
                    if self._loaded:
                        cls._dirty_index.remove(self)
                    self.__dirty = value
                    if self._loaded:
                        cls._dirty_index.add(self)

                def __init__(self):
                    super().__init__()
                    # used to mark us as not yet ready for indexing
                    self._loaded = False
                    # take the next available ID
                    cls.max_id += 1
                    self.id = cls.max_id
                    # actually store us on the table
                    self.table.rows.append(self)
                    # _offset is where we're written to disk
                    # we should be tightly packed, so next spot is just past the last
                    self._offset = self.table.max('_offset', -1) + 1
                    # index ourselves
                    for column in cls.columns:
                        column.load(self)
                    for index in self.table.indices:
                        index.add(self)
                    self._loaded = True
                    # dirty because we're a new row so we aren't on disk yet
                    self._dirty = True

                # delete the row
                def destroy(self):
                    for index in cls.indices:
                        index.remove(self)
                    cls.rows.remove(self)
                    # give our spot on disk to the last entry
                    offset_index = cls.find_index('_offset')
                    try:
                        max = next(iter(offset_index.find(None, True)))
                        # this should only be false if we're the very last row
                        if max._offset > self._offset:
                            offset_index.remove(max)
                            # so now we write them to our spot
                            max._offset = self._offset
                            # and mark them to be written next write
                            max._dirty = True
                            offset_index.add(max)
                    except StopIteration:
                        # this means the db is empty. Oh well!
                        pass

                # these two are so we can use a row as on index key
                # useful to index foreigncolumns
                def __eq__(self, other):
                    return other is not None and self.id == other.id

                def __lt__(self, other):
                    # need to deal with None columns
                    if other is None:
                        return False
                    else:
                        return self.id < other.id

                def __hash__(self):
                    return hash(cls.__name__ + str(self.id))

            # attach properties to the row for easy access
            for column in cls.columns + cls.vcolumns:
                setattr(Row, column.name, column.Property())
            cls.Row = Row
            cls.max_id = 0
            indices = getattr(cls, 'indices', [])

            # all indices end with id to guarantee unique keys
            # default indices
            cls.indices = [Index('id'), Index('_offset', 'id'), Index('_dirty', 'id')]
            # and the ones from the subclass
            for index in indices:
                cls.indices.append(Index(*index, 'id'))

            # for easy access
            cls._dirty_index = cls.find_index('_dirty')

            # save our schema for migrations
            cls.col_dump = pickle.dumps(cls.columns, 4)

        @classmethod
        def load(cls, dir):
            path = dir.joinpath(cls.__name__ + '.tbl')
            for column in cls.columns:
                column.load_col()
            try:
                file = open(path, 'rb')
            except IOError:
                # file doesn't exist, means we're new
                cls.rows = []
                return
            # read 4 bytes for the header length
            schema_len = int.from_bytes(file.read(4), byteorder='little')
            # then read the schema that is the rest of the header
            schema = file.read(schema_len)
            # if your schema doesn't match, we need to do a complex import.
            if schema != cls.col_dump:
                cls.import_data(dir, schema)
                return
            # keep track of the offset each row came from
            offset = 0
            while file.peek():
                row = cls.Row()
                file.readinto(row)
                row._offset = offset
                offset += 1
                # load all the columns to let them do any processing they need
                for column in cls.columns:
                    column.load(row)
                # index the row
                for index in cls.indices:
                    index.add(row)
                # the row just came off the disk, so we know it hasn't been changed
                row._dirty = False
                row._new = False
            # and our max id is the highest ID loaded
            cls.max_id = max((i.id for i in cls.rows), default=0)

        # here we make a new table from the schema and copy it over
        @classmethod
        def import_data(cls, path, schema):
            columns = pickle.loads(schema)
            d = {column.name: column for column in columns}
            # subclassing Table will make its own id, so get rid of the dupe
            del d['id']
            copy = type(cls.__name__, cls.__bases__, d)
            # Get out of my db imposter! >:O
            cls.db.tables.remove(copy)
            # Since this is made from the file schema, it shouldn't need to import
            copy.load(path)
            their_columns = set(d.keys())
            their_columns.add('id')
            for column in cls.columns:
                column.load_col()
            for row in copy.rows:
                ours = cls.Row()
                for column in cls.columns:
                    # copy any repeat columns
                    if column.name in their_columns:
                        setattr(ours, column.name, getattr(row, column.name))
                    # and we need to load all ours
                    column.load(ours)
                ours._new = False
            # and on save we need to overwrite the schema and save every row
            cls.full_dump_needed = True

        @classmethod
        def save(cls, path):
            path = path.joinpath(cls.__name__ + '.tbl')
            # save everything if the schema changed
            if cls.full_dump_needed:
                return cls.save_all(path)
            try:
                file = open(path, 'r+b')
            except FileNotFoundError:
                # save everything if the file doesn't exist
                return cls.save_all(path)
            # byte length of each row for seeking purposes
            size = sizeof(cls.Row)
            # list of all dirty columns
            dirty = [i for i in cls._dirty_index.find((True,))]
            # and skip over the header
            header_offset = len(cls.col_dump) + 4
            for column in cls.columns:
                column.dump_col()
            for row in dirty:
                file.seek(row._offset * size + header_offset)
                for column in cls.columns:
                    column.dump(row)
                file.write(row)
                row._dirty = False
                row._new = False
            highest_offset = cls.max('_offset', -1) + 1
            # and if the file got shorter, cut off the unneeded bytes
            file.truncate(highest_offset * size + header_offset)

        @classmethod
        def save_all(cls, path):
            file = open(path, 'w+b')
            length = len(cls.col_dump)
            file.write(length.to_bytes(4, byteorder='little'))
            file.write(cls.col_dump)
            ordered = [i for i in cls.find_index('_offset').find(None)]
            for row in ordered:
                for column in cls.columns:
                    column.dump(row)
                file.write(row)
                row._dirty = False
            cls.full_dump_needed = False

        # find the best index for a search
        # we define best as the most columns we can index the query against
        @classmethod
        def find_index(cls, keys, cmpkeys=[]):
            match = -1
            best_index = None
            for index in cls.indices:
                strength = 0
                for key in index.keys:
                    # we can match any number of equality  keys
                    if key in keys:
                        strength += 1
                    # but only one comparison key
                    elif key in cmpkeys:
                        strength += 1
                        break
                    else:
                        break
                if strength > match:
                    match = strength
                    best_index = index
            return best_index

        # query the db
        @classmethod
        def where(cls, *comparisons, **kwargs):
            eq = [i for i in comparisons if isinstance(i, ColEq)]
            cmp = [i for i in comparisons if not isinstance(i, ColEq)]
            for key, value in kwargs.items():
                column = getattr(cls, key)
                eq.append(column == value)
            eq_names = {i.col.name: i for i in eq}
            cmp_names = {i.col.name: i for i in cmp}
            index = cls.find_index(eq_names, cmp_names)
            # our starting point
            start = []
            # the tests that should always be true in the chosen range
            # if any fail that means we've moved past any possible matches and can quit
            matches = []
            for key in index.keys:
                # if any keyed equalities fail, that means we've moved past
                if key in eq_names:
                    comp = eq_names[key]
                    start.append(comp.value)
                    matches.append(comp)
                elif key in cmp_names:
                    # we can only use one comparison key, so we can break after this
                    comp = cmp_names[key]
                    # for a < search, we start from the far left (by not adding to start)
                    # and as soon as we find a key not < the value, that means we've moved past
                    if isinstance(comp, (ColLt, ColLe)):
                        matches.append(comp)
                    else:
                        # for a > search, we start at the minimum value
                        start.append(comp.value)
                    break
                else:
                    break

            if len(index.keys) == 1:
                if start:
                    start = start[0]
                else:
                    # if we have nothing to index off, use None to start at the very beginning of the table
                    start = None
            else:
                start = tuple(start)
            for entry in index.find(start):
                # these will always be true until we exit the range where a match is possible
                if not all(i.match(entry) for i in matches):
                    break
                # and here we verify the entry matches every test
                if all(i.match(entry) for i in comparisons):
                    yield entry

        # simple wrapper for where to return 1 entry or None
        @classmethod
        def find(cls, *comparisons, **kwargs):
            for i in cls.where(*comparisons, **kwargs):
                return i
            return None

        # for internal use; rebuilds every database
        # can be used if index columns may have changed in tricky ways to ensure our indices are right
        @classmethod
        def reindex(cls):
            for index in cls.indices:
                index.reindex()

        # finds the maximum of an indexed value in the table, or a default
        @classmethod
        def max(cls, key, default=None):
            try:
                index = cls.find_index(key)
                max = next(iter(index.find(None, True)))
                return getattr(max, key)
            except StopIteration:
                return default

    def __init__(self):
        self.Table.db = self
        self.tables = []

    # connect the db to a directory
    def connect(self, path):
        self.path = pathlib.Path(path)
        os.makedirs(self.path, exist_ok=True)
        for table in self.tables:
            table.load(self.path)
        # and because RemoteColumns may not work until all tables are loaded, we need to reindex
        # could probably optimize this by only using indexes or RemoteColumns
        for table in self.tables:
            table.reindex()

    def save(self):
        for table in self.tables:
            table.save(self.path)


class ColCmp:
    def __init__(self, col, value):
        self.key = col.name
        self.col = col
        self.value = value


# simple comparison classes for queries
class ColEq(ColCmp):
    def match(self, row):
        return self.col.get(row) == self.value


class ColGt(ColCmp):
    def match(self, row):
        return self.col.get(row) > self.value


class ColLt(ColCmp):
    def match(self, row):
        return self.col.get(row) < self.value


class ColGe(ColCmp):
    def match(self, row):
        return self.col.get(row) >= self.value


class ColLe(ColCmp):
    def match(self, row):
        return self.col.get(row) <= self.value
