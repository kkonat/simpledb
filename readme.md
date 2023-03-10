### A simple flat file database package ###
-----------
A simple, flat-file, sequential database, intended for non-demanding in-app data storage with memory cache

Saves two files:
  - info-filename.json
    containing database information
  - data-filename.json
    containing data

currently the data file is a mix of binary and json:
Tried gob encoding, but it seems to be too much of an overhead for such a simple database
Benchmark results are:

For json encoding
    154090	      7653 ns/op	     404 B/op	      11 allocs/op
For gob
    41023 	     29126 ns/op	    7356 B/op	     193 allocs/op

So json is almost 4x faster and 18x less memory intensive

So each database item is saved as (uint16, json) pair, where uint16 contains the length of the json part
This ensures random access to any data Item and makes reading the data item easier in code. 
The read operation consists of
- checking if the item exists in the memory cache, and getting it from there if it deoes
  or
- seeking to the beginning of the data item
- reading the uint16 data item lenght
- reading appropriate number of bytes with json data structure
- unmarshalling the data

Mem cache improves data access speeds:
No cache
    131604        8195 ns/op       433 B/op       12 allocs/op
For hit rate of ~10% (small cache)
    154090	      7653 ns/op	     404 B/op	      11 allocs/op
For hit rate of ~50% (cache half the dataset size )
    275769        4323 ns/op       219 B/op        6 allocs/op
For hit rate of ~75% (cache half the dataset size )
    551078        2166 ns/op       109 B/op        3 allocs/op
For hit rate of 100% (cache the same size the dataset )   
  18717418        54.99 ns/op	       0 B/op	       0 allocs/op 

The following operations are supported as of now
Connect - connects to the database
Append - appends data to the database and returns its id
Get - gets data from the database by id
Close - closes the database
Kill - closes and deletes all database files


Wrote this to try out the following language fetures:
- fil io, path handling
- error handing 
- marshalling / encoding (json, gob)
- TDD, benchmarking
- some data structures (map, queue)
- module 