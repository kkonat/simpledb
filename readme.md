### A simple flat file key, value database package

---

A simple, sequential flat-file key, value database,
intended for non-demanding in-app data storage
with memory cache

Currently uses borsh (Binary Object Representation Serializer for Hashing) for binary encoding of values. I tried out gob encoding, but due to the nature of sequential writes to the database, it would require some heavy wrangling to get rid of type definition
data included in the binary form

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
- timestamp 8 bytes         - timestamp
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
| Open       | opens the database|
| Append     | appends data item to the database|
| Update     | updates data item with the given key |
| Get        | gets data item from the database by key |
| Delete     | deletes data item by id |
| Close      | closes the database|
| Destroy    | closes and deletes all database files (useful for starting tests) |

I wrote this package try some of the following go fetures out:

- file io, path handling, file operations
- error handling (wrapping)
- data marshalling / encoding (json, gob, binary, borsh)
- TDD, benchmarking
- simple data structures (maps, queue)
- profiling 
- modules