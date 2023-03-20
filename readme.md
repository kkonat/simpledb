### A simple flat file key, value database package

---

A simple, sequential flat-file key, value database,
intended for non-demanding in-app data storage
with memory cache

Currently uses borsh (Binary Object Representation Serializer for Hashing) for binary encoding of values. I tried out gob encoding, but due to the nature of sequential writes to the database, it would require some heavy wrangling to get rid of type definition
data included in the binary form

The database holds in-memory index of key hashes and indices pointing to individual data items in the database file (map [hash] []index). This index is rebuilt when the database is opened. New data items (key, value pairs) are added to the database by appending them at the end of the file. The database also holds an in-memory list of deleted data items (map[index]bool). When an item is deleted, a corresponding entry is added to this list. Data items are updated by  appending the updated value at the end of the database file and marking the previous version as deleted. The database also maintains an in-memory cache of a pre-defined size with recently accessed data items. The cache uses a LIFO queue to determine the oldest data items, which will be discarded from the cache to make romm for new data. If data item is accessed it is moved to the beginning of the queue. Data is saved to disk on each append operation. On database close data in the database file is reorganized. This means a new file is created with persisting data items copied from the old file and all deleted itemsskipped. Data is read from the disk on two occassions: if a data item is not available in the cache and on database open operation, when the whole datbase file is scanned to rebuild the index file. Locating a key value pair in the database involves a single read from the map[hash] []index. For a given key hash a list of data items is obtained from the map and then linearly searched to find the exact match. Hashes are 32-bit long which means 4 billion potential values, and considering the fact that this is a "simple database" i.e. it will not store large sets of data, collisions are expected to be infrequent. Currently crc and superfast hash algos are implemented.


Here are actual performnce results of various encoding types:
| encoding | performance |
| --- | -- |
| borsh | 4094 ns/op 216 B/op 12 allocs/op |
| json | 7653 ns/op 404 B/op 11 allocs/op |
| gob | 29126 ns/op 7356 B/op 193 allocs/op |

Each database block in the file hast the following structure:

```
- Offset    4 bytes         - Offset to the next block in the file (i.e. block lenght)
- ID        4 bytes         - Object ID
- KeyHash   4 bytes         - hash of the key
- KeyLen    4 bytes
- Key       variable length - key
- Value     variable length - payload
```

The database features a simple LIFO cache

The Get operation works as follows:

- checking if the item is cached, and getting it from the cache if it's there
  or
- seeking to the data item in the file (using offests map)
- reading the data item
- parsing item data and payload

The memory cache improves data access speeds up to 150x times (on my SSD):

Benchmark results for **borsh** encoding:
| Cache size | benchmark result |
| ----------------------------------------------- | :------------------------------- |
| No cache | 7890 ns/op 416 B/op 22 allocs/o |
| ~10% hit rate (small cache) | 7469 ns/op 391 B/op 20 allocs/op |
| ~50% hit rate (cache half the dataset size ) | 4080 ns/op 216 B/op 12 allocs/op |
| ~75% hit rate (cache half the dataset size ) | 2085 ns/op 115 B/op 6 allocs/op |
| 100% hit rate (cache the same size the dataset) | 124.2 ns/op 16 B/op 2 allocs/op |

The following operations are supported as of now:

| Operation  | Description  |
| ---------- | :---------------------------------------------------------------- |
| Open       | creates and opens the database if it does not exist or opens if it does |
| Append     | appends data item to the database|
| Update     | updates data item with the given key |
| Get        | gets data item from the database by key |
| Delete     | deletes data item by id |
| Close      | closes the database|
| Destroy    | deletes  database files (requires full path name to db.file for security) |

I wrote this package try some of the following go fetures out:

- file io, path handling, file operations
- error handling (wrapping)
- data marshalling / encoding (json, gob, binary, borsh)
- TDD, benchmarking
- simple data structures (maps, queue)
- profiling 
- modules

TODO: 
- explore other hash functions
  http://www.partow.net/programming/hashfunctions/index.html
  https://stackoverflow.com/questions/2351087/what-is-the-best-32bit-hash-function-for-short-strings-tag-names
- add disk write cache, i.e. group disk writes in blocks
- or write in background
- add block read write for db reorganization on close
- add an option to persist index in a separate file