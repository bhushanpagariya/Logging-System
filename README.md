# Logging-System

# UCI CS 223 - Logging System

This project is done as part of CS 223 - Transaction Management course at UC Irvine. 
Implemented Logging System supporting different types of payload such as XML, JSON and Text. Following APIs are implemented as part of this project:-

WriteLog - Write Transactional and Non-Transactional logs
QueryLog - Query logs using EQUAL, GT, LT, GTE, LTE and Range operator
DeleteLog - Delete logs satisfying user query
FlushLog - Flush logs from in-memory database to persistent database. Also has support for partial flushing in case of transactional logs

Used Redis as in-memory database and MongoDB as persistent database.
