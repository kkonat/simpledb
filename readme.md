### A simple flat file database package ###
-----------


A simple, flat file, sequential database, with memory cache

Saves two files:
  - info-filename.json
    containing database information
  - data-filename.json
    containing data

currently (will be switching to gob soon) the data file is a mix of binary and json:
it consists of uint16 + json pairs, where uint16 contains the length of the json part
This ensures random access to any data Item and makes reading the data item easier in code. 
The read operation consists of
- checking if the item exists in the memory cache, and getting it from there if it deoes
  or
- seeking to the beginning of the data item
- reading the uint16 data item lenght
- reading appropriate number of bytes with json data structure
- unmarshalling the data

The following operations are supported as of now
Connect - connects to the database
Append - appends data to the database and returns its id
Get - gets data from the database by id
Close - closes the database
Flush - closes and reopens the database to purge any filesystem  caching
Kill - closes and deletes all database files

