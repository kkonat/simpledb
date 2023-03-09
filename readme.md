### A simple flat file database ###
-----------


A simple, flat file, sequential database 

Saves two files:
  - info-filename.json
    containing database information
  - data-filename.json
    containing data

The data file is a mix of binary and json:
it consists of uint16 + json pairs, where uint16 contains the length of the json part
This ensures random access to any data Item and makes reading the data item easier in code. The read operation consists of
- seeking to the beginning of the data item
- reading the uint16 data item lenght
- reading appropriate number of bytes with json data
- unmarshalling the data



