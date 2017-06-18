# Seaslug
Seaslug is a pure Python embedded database for small use cases


# Usage

All your data is stored inside a `Database` object.

```
from seaslug import *

db = Datadase()
```

Tables are defined by subclassing `Database.Table`. Columns are specified with `Column` subclasses

```
class Numbers(db.Table):
  number = IntColumn()
```

All data is stored in a directory you `connect` the Database to.

```
db.connect('data-directory/')
```

Once your db is connected you can create `Row`s for your tables:

```
row = Numbers.Row()
row.number = 7
```

All Rows have an implicit `id` column:

```
row = Numbers.Row()
row.id # Some int > 0
```

You can save your data with `Database.save`.
Saves are incremental: When you save, only the columns changed since your last write are written.
No creations, saves, or deletions will be persisted until you save.

```
db.save()
```

Rows can be deleted witH destroy()

```
row.destroy()
```

Database migrations are automatic; When you connecrt a database, if the current schema doesn't match the old, data will be imported:
* If a column exists with no column of the same name before, it is initialized as blank.
* If a column exists with the same name as a previous column, the new value is set to equal the old
* If a column does not exist when it previously did, it is dropped.

```
class Numbers(db.Table):
  number = PickleColumn(type=int)
```

Variable-length columns have a length in bytes. Attempts to excede this length will throw a `ValueError`
```
class Tribble(db.Table):
  name = StrColumn(length=8)
  
 # ...
 
 trib = Tribble.Row()
 trib.name = 'overy long name.' # ValueError
 ```
 
 `StrColumn` contains a unicode string. `PickleColumn` contains any pickleable object
 
 ```
class Tribble(db.Table):
  name = StrColumn(length=64)
  weight = PickleColumn()
  
 # ...
 
 trib = Tribble.Row()
 trib.name = '名前'
 trib.weight = Decimal('.131')
 ```
 
 `ForeignColumn` contains a reference to a row in a table. NOte that changing the `id` of a row can break Columns pointing to it.
 
 ```
 class Human(db.Table):
  name = StrColumn(length=64)
 
class Tribble(db.Table):
  name = StrColumn(length=64)
  owner = ForeignColumn('Human')
  
 # ...
 
 kirk = Human.Row()
 kirk.name = 'Kirk'
 
 
 trib = Tribble.Row()
 trib.name = 'Tribbor' # ValueError
 trib.owner = kirk
 ```
 
 `ForeignColumn` can also hold `None`
 
 ```
 trib2 = Tribble.Row()
 trib2.owner # None
 trib.owner = None # Disowned :(
 ```
 
 Tables can be queried.
 
 ```
 Tribble.where(name = 'Tribbor') # Generator
 Tribble.find(name = 'Tribbor') # Single row or null
 Trible.where(weight < 5) # Generator
 ```
 
 Tables can specify indices to speed queries:
 
 ```
 class Wizard(db.Table):
  power_level = IntColumn()
  indices = [
    [ 'power_level' ]
  ]
  
#...

Wizard.where(power_level > 7000) #this is efficient!

```

You can also specify "Virtual columns" that to simplify interfaces.

```
class Human(db.Table):
  name = StrColumn(length=64)
  tribbles = Belongs('Tribble', 'owner') # reverses a ForeignColumn
  tribble_names = Through('tribbles', 'name') # Lets you specify a 
 
class Tribble(db.Table):
  name = StrColumn(length=64)
  owner = ForeignColumn('Human')
  
  indices = [
    ['owner'] #looking up a human's tribbles will be slow without this
  ]
  
#...

kirk.tribbles # generator of Tribble.Rows
kirk.tribble_names # generator of strings
```
