### A simple flat file database package ###
-----------
A simple, flat-file, sequential database, intended for non-demanding in-app data storage with memory cache

The data file is a homebrew mix of binary data and json:

I tried out gob encoding to make the data file fully, binary, but it seems to be an overkill  for such a simple database
First, gob enconding still encodes field names, as they appear in Json, it also stores struc name, with every data entry.
So it doesn't bring any size benefits. And its slower than json. Benchmark results are:
For json encoding
    154090	      7653 ns/op	     404 B/op	      11 allocs/op
For gob
    41023 	     29126 ns/op	    7356 B/op	     193 allocs/op

So json is almost 4x faster and 18x less memory intensive

So each database item is saved as (uint16, json) pair, where uint16 contains the length of the json part
This ensures random access to any data Item and makes reading the data item easier in code. 
When the database is opened, it builds a map of offsets to all data entries in the file

The Get operation involves:
- checking if the item exists in the memory cache, and getting it from there if it deoes
  or
- seeking to the data item in the file using offests map
- reading thedata item lenght
- reading appropriate number of bytes containing the json data structure
- unmarshalling the data

The memory cache improves data access speeds up to 150x times (on my SSD):
No cache
    131604        8195 ns/op       433 B/op       12 allocs/op
For hit rate of ~10% (small cache)
    154090	      7653 ns/op	     404 B/op	      11 allocs/op
For hit rate of ~50% (cache half the dataset size )
    275769        4323 ns/op       219 B/op        6 allocs/op
For hit rate of ~75% (cache half the dataset size )
    551078        2166 ns/op       109 B/op        3 allocs/op
Full cache  (100% hit rate - cache the same size the dataset )   
  18717418        54.99 ns/op	       0 B/op	       0 allocs/op    

The cache uses a LIFO queue to get rid of the oldest data items if the cache overfills.
Each non-cached data is pushed at the end of the queue. If the queue is full, the oldest
of the queued data is discarded from the cache and from the queue.

The following operations are supported as of now:
Connect - connects to the database
Append - appends data item to the database and returns its id
Get - gets data item from the database by id
Close - closes the database
Kill - closes and deletes all database files (useful for starting from scratch in tests)

I wrote this package try some of the following go fetures out:
- file io, path handling
- error handing (wrapping)
- data marshalling / encoding (json, gob)
- TDD, benchmarking
- simple data structures (map, queue)
- module 
