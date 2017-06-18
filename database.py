import operator
import os
import pathlib
import io
import pickle

from ctypes import *
from skiplistcollections import SkipListDict


class Column:
    def __init__(self, *args, **kwargs):
        class Property:
            def __get__(prop, instance, owner):
                return self.get(instance)

            def __set__(prop, instance, value):
                instance._dirty = True
                return self.set(instance, value)

        self.Property = Property
        self.args = args
        self.kwargs = kwargs

    def __set_name__(self, owner, name):
        self.name = name
        self.table = owner

    def get(self, row):
        return getattr(row, '_' + self.name)

    def set(self, row, value):
        indices = [index for index in row.table.indices if self.name in index.keys]
        if row._loaded:
            for index in indices:
                index.remove(row)
        setattr(row, '_' + self.name, value)
        if row._loaded:
            for index in indices:
                index.add(row)

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

    def load(self, row):
        pass

    def dump(self):
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


class StructColumn(Column):
    def get(self, row):
        return getattr(row, '__' + self.name).value

    def set(self, row, value):
        getattr(row, '__' + self.name).value = value
        return super().set(row, value)


class IntColumn(StructColumn):
    class Struct(LittleEndianStructure):
        _fields_ = [('value', c_int)]


class Index:
    def __init__(self, *keys):
        self.keys = keys
        self.keyer = operator.attrgetter(*keys)
        self.list = SkipListDict()

    def add(self, object):
        key = self.keyer(object)
        self.list[key] = object

    def remove(self, object):
        key = self.keyer(object)
        del self.list[key]

    def find(self, start, reverse=False):
        return self.list.values(start, reverse)


class Database:
    path = None

    class Table:
        full_dump_needed = False
        id = IntColumn()

        def __init_subclass__(cls, **kwargs):
            cls.rows = []
            cls.db.tables.append(cls)
            cls.columns = []
            for name, column in cls.__dict__.items():
                if not isinstance(column, Column):
                    continue
                cls.columns.append(column)
            cls.id = IntColumn()
            cls.id.__set_name__(cls, 'id')
            cls.columns.insert(0, cls.id)
            fields = []
            for column in cls.columns:
                fields.append(('__' + column.name, column.Struct))

            class Row(LittleEndianStructure):
                _fields_ = fields
                table = cls

                __dirty = False

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
                    self._loaded = False
                    cls.max_id += 1
                    self.id = cls.max_id
                    self.table.rows.append(self)
                    self._offset = self.table.max('_offset', -1) + 1
                    for index in self.table.indices:
                        index.add(self)
                    self._loaded = True
                    self._dirty = True

                def destroy(self):
                    for index in cls.indices:
                        index.remove(self)
                    cls.rows.remove(self)
                    offset_index = cls.find_index('_offset')
                    try:
                        max = next(iter(offset_index.find(None, True)))
                        if max._offset > self._offset:
                            offset_index.remove(max)
                            max._offset = self._offset
                            max._dirty = True
                            offset_index.add(max)
                    except StopIteration:
                        pass

                def __repr__(self):
                    return '< {}: ' + ' '.join('{}: {},'.format(i.name, i.get(self)) for i in cls.columns) + ' >'

            for column in cls.columns:
                setattr(Row, column.name, column.Property())
            cls.Row = Row
            cls.max_id = -1
            indices = getattr(cls, 'indices', [])
            cls.indices = [Index('id'), Index('_offset', 'id'), Index('_dirty', 'id')]
            for index in indices:
                cls.indices.append(Index(*index, 'id'))

            cls._dirty_index = cls.find_index('_dirty')

            cls.col_dump = pickle.dumps(cls.columns)

        @classmethod
        def load(cls, dir):
            path = dir.joinpath(cls.__name__ + '.tbl')
            try:
                file = open(path, 'rb')
            except IOError:
                cls.rows = []
                return
            schema_len = int.from_bytes(file.read(4), byteorder='little')
            schema = file.read(schema_len)
            if schema != cls.col_dump:
                cls.import_data(dir, schema)
                return
            offset = 0
            while file.peek():
                row = cls.Row()
                file.readinto(row)
                row._offset = offset
                offset += 1
                for column in cls.columns:
                    column.load(row)
                for index in cls.indices:
                    index.add(row)
                row._dirty = False
            cls.max_id = max((i.id for i in cls.rows), default=0)

        @classmethod
        def import_data(cls, path, schema):
            columns = pickle.loads(schema)
            d = {column.name: column for column in columns}
            del d['id']
            copy = type(cls.__name__, cls.__bases__, d)
            cls.db.tables.remove(copy)
            copy.load(path)
            their_columns = set(d.keys())
            their_columns.add('id')
            for row in copy.rows:
                ours = cls.Row()
                for column in cls.columns:
                    if column.name in their_columns:
                        setattr(ours, column.name, getattr(row, column.name))
                    column.load(ours)
            cls.full_dump_needed = True

        @classmethod
        def save(cls, path):
            path = path.joinpath(cls.__name__ + '.tbl')
            if cls.full_dump_needed:
                return cls.save_all(path)
            try:
                file = open(path, 'r+b')
            except FileNotFoundError:
                return cls.save_all(path)
            size = sizeof(cls.Row)
            dirty = [i for i in cls._dirty_index.find((True,))]
            header_offset = len(cls.col_dump) + 4
            for row in dirty:
                file.seek(row._offset * size + header_offset)
                file.write(row)
                row._dirty = False
            highest_offset = cls.max('_offset', -1) + 1
            file.truncate(highest_offset * size + header_offset)

        @classmethod
        def save_all(cls, path):
            file = open(path, 'w+b')
            length = len(cls.col_dump)
            file.write(length.to_bytes(4, byteorder='little'))
            file.write(cls.col_dump)
            ordered = [i for i in cls.find_index('_offset').find(None)]
            for row in ordered:
                file.write(row)
                row._dirty = False
            cls.full_dump_needed = False

        @classmethod
        def find_index(cls, keys, cmpkeys=[]):
            match = -1
            best_index = None
            for index in cls.indices:
                strength = 0
                for key in index.keys:
                    if key in keys:
                        strength += 1
                    elif key in cmpkeys:
                        strength += 1
                        break
                    else:
                        break
                if strength > match:
                    match = strength
                    best_index = index
            return best_index

        @classmethod
        def where(cls, *comparisons):
            eq = [i for i in comparisons if isinstance(i, ColEq)]
            cmp = [i for i in comparisons if not isinstance(i, ColEq)]
            eq_names = {i.col.name: i for i in eq}
            cmp_names = {i.col.name: i for i in cmp}
            index = cls.find_index(eq_names, cmp_names)
            reverse = False
            start = []
            matches = []
            for key in index.keys:
                if key in eq_names:
                    comp = eq_names[key]
                    start.append(comp.value)
                    matches.append(comp.key)
                elif key in cmp_names:
                    comp = cmp_names[key]
                    if isinstance(comp, (ColLt, ColLe)):
                        matches.append(comp)
                    else:
                        start.append(comp.value)
                    break
                else:
                    break

            if len(index.keys) == 1:
                if start:
                    start = start[0]
                else:
                    start = None
            else:
                start = tuple(start)
            for entry in index.find(start, reverse):
                if not all(i.match(entry) for i in matches):
                    break
                if all(i.match(entry) for i in comparisons):
                    yield entry

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

    def connect(self, path):
        self.path = pathlib.Path(path)
        os.makedirs(self.path, exist_ok=True)
        for table in self.tables:
            table.load(self.path)

    def save(self):
        for table in self.tables:
            table.save(self.path)


class ColCmp:
    def __init__(self, col, value):
        self.key = col.name
        self.col = col
        self.value = value


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


class StrColumn(Column):
    def __init__(self, length):
        class Struct(LittleEndianStructure):
            _fields_ = [('length', c_uint32), ('data', c_ubyte * length)]

        self.length = length
        self.Struct = Struct
        super().__init__(length)

    def set(self, row, value):
        encoded = value.encode()
        length = len(encoded)
        if length > self.length:
            raise ValueError("Received string of {} bytes, maximum {}".format(length, self.length))
        struct = getattr(row, '__' + self.name)
        struct.length = length
        b = io.BytesIO()
        b.write(struct)
        offset = self.Struct.data.offset
        b.getbuffer()[offset:offset + length] = encoded
        b.seek(0)
        b.readinto(struct)
        return super().set(row, value)

    def load(self, row):
        b = io.BytesIO()
        length = getattr(row, '__' + self.name).length
        b.write(getattr(row, '__' + self.name))
        buffer = b.getvalue()[4:4 + length]
        string = buffer.decode()
        setattr(row, '_' + self.name, string)