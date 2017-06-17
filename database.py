import operator
import os
import pathlib
import io

from ctypes import *
from skiplistcollections import SkipListDict


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
        def __init_subclass__(cls, **kwargs):
            cls.rows = []
            cls.db.tables.append(cls)
            cls.columns = []
            fields = [("id", c_uint32)]
            for name, column in cls.__dict__.items():
                if not isinstance(column, Column):
                    continue
                cls.columns.append(column)
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
                    cls._dirty_index.remove(self)
                    self.__dirty = value
                    cls._dirty_index.add(self)

                def __init__(self):
                    cls.max_id += 1
                    self.id = cls.max_id
                    self.table.rows.append(self)
                    self._offset = self.table.max('_offset', -1) + 1
                    for index in self.table.indices:
                        index.add(self)
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

            for column in cls.columns:
                setattr(Row, column.name, column.Property())
            cls.Row = Row
            cls.max_id = -1

            cls.indices = [Index('id'), Index('_offset', 'id'), Index('_dirty', 'id')]

            cls._dirty_index = cls.find_index('_dirty')

        @classmethod
        def load(cls, path):
            path = path.joinpath(cls.__name__ + '.tbl')
            try:
                file = open(path, 'rb')
            except IOError:
                cls.rows = []
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
        def save(cls, path):
            path = path.joinpath(cls.__name__ + '.tbl')
            file = open(path, 'r+b')
            size = sizeof(cls.Row)
            dirty = [i for i in cls._dirty_index.find((True,))]
            for row in dirty:
                file.seek(row._offset * size)
                file.write(row)
                row._dirty = False
            highest_offset = cls.max('_offset', -1) + 1
            file.truncate(highest_offset * size)

        @classmethod
        def find_index(cls, keys):
            match = -1
            best_index = None
            for index in cls.indices:
                strength = 0
                for key in index.keys:
                    if key in keys:
                        strength += 1
                    else:
                        break
                if strength > match:
                    match = strength
                    best_index = index
            return best_index

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


class Column:
    def __init__(self):
        class Property:
            def __get__(prop, instance, owner):
                return self.get(instance)

            def __set__(prop, instance, value):
                instance._dirty = True
                return self.set(instance, value)

        self.Property = Property

    def __set_name__(self, owner, name):
        self.name = name
        self.table = owner

    def get(self, row):
        return getattr(row, '_' + self.name)

    def set(self, row, value):
        indices = [index for index in row.table.indices if self.name in index.keys]
        for index in indices:
            index.remove(row)
        setattr(row, '_' + self.name, value)
        for index in indices:
            index.add(row)

    def load(self, row):
        pass

    def dump(self):
        pass


class StructColumn(Column):
    def get(self, row):
        return getattr(row, '__' + self.name).value

    def set(self, row, value):
        getattr(row, '__' + self.name).value = value
        return super().set(row, value)


class IntColumn(StructColumn):
    class Struct(LittleEndianStructure):
        _fields_ = [('value', c_int)]


class StrColumn(Column):
    def __init__(self, length):
        class Struct(LittleEndianStructure):
            _fields_ = [('length', c_uint32), (('data'), c_ubyte * length)]

        self.length = length
        self.Struct = Struct
        super().__init__()

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


db = Database()


class TestTable(db.Table):
    num = IntColumn()
    str = StrColumn(255)


db.connect('test_database')
row = TestTable.rows[0]
print(row.id)
db.save()
