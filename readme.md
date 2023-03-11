### A simple flat file key, value database package

---

A simple, sequential flat-file key, value database,
intended for non-demanding in-app data storage
with memory cache

Currently uses json encoding for values. I tried out gob encoding to make the data file fully, binary,
but on the first attempt it seemed an overkill for such a simple database.
First, probably due to generics support I was unable to force gob enconding not to encode field names (data structure) in each record. I will try it again later, but for now, I will be focusing on core functionality
In my recent implementation, json performed better:
7653 ns/op 404 B/op 11 allocs/op
compared to gob with:
29126 ns/op 7356 B/op 193 allocs/op

So, currently json is almost 4x faster and 18x less memory intensive, but I will work on that

Each database item in the file hast the following structure:
const (
OffsPos = 0 // Offset to the next item in the file (i.e. item lenght)
OffsL = 4 // 4 bytes
IDPos = OffsPos + OffsL // Objec ID
IDL = 4 // 4 bytes
TimePos = IDPos + IDL // Timestamp
TimeL = 8 // 8 bytes
KeyHashPos = TimePos + TimeL // Key hash
KeyHashL = 4 // 4 bytes
KeyLenPos = KeyHashPos + KeyHashL // Key Length
KeyLenL = 2 // 2 bytes
KeyPos = KeyLenPos + KeyLenL // Key, variable lenght
// payload of variable leghts, goes here
)

The database features a simple LIFO cache

The Get operation works as follows:

- checking if the item is cached, and getting it from the cache if it's there
  or
- seeking to the data item in the file (using offests map)
- reading the data item
- parsing item data and payload

The memory cache improves data access speeds up to 150x times (on my SSD):
Benchmark results were:
No cache
8195 ns/op 433 B/op 12 allocs/op
~10% hit rate (small cache)
7653 ns/op 404 B/op 11 allocs/op
~50% hit rate (cache half the dataset size )
4323 ns/op 219 B/op 6 allocs/op
~75% hit rate (cache half the dataset size )
2166 ns/op 109 B/op 3 allocs/op
100% hit rate (cache the same size the dataset)
55 ns/op 0 B/op 0 allocs/op

The following operations are supported as of now:
Connect - connects to the database
Append - appends data item to the database and returns its id
GetById - gets data item from the database by id
DeleteById - deletes data item by id
Close - closes the database
Destroy - closes and deletes all database files (useful for starting from scratch in tests)

I wrote this package try some of the following go fetures out:

- file io, path handling, file operations
- error handling (wrapping)
- data marshalling / encoding (json, gob, binary)
- TDD, benchmarking
- simple data structures (maps, queue)
- modules
