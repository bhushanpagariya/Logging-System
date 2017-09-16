# Logging-System

UCI CS 223 - Logging System

This project is done as part of CS 223 - Transaction Management course at UC Irvine. 
Implemented Logging System supporting different types of payload such as XML, JSON and Text. Following APIs are implemented as part of this project:-

1. WriteLog - Write Transactional and Non-Transactional logs
2. QueryLog - Query logs using EQUAL, GT, LT, GTE, LTE and Range operator
3. DeleteLog - Delete logs satisfying user query
4. FlushLog - Flush logs from in-memory database to persistent database. Also has support for partial flushing in case of transactional logs

Example:
```
//Create an instance of the LoggingSystem class
LoggingSystem logsystem = new LoggingSystem();
List<Log> logList = new ArrayList<>();
//Create a new Log
Log log = new Log("<XML></XML>",CONSTANTS.LOG_TYPE_XML);
//Append to logList
logList.add(log)
//Write non-transactional log
List<Long> lsnList = logsystem.writeLog(logList);
//Flush logs
logsystem.flushLog(lsnList.get(0));
//Query Log
QueryGen query = new QueryGen();
query.append(new QuerySpec(CONSTANTS.FIELD_LSN,lsnList.get(0),QuerySpec.Operator.EQUAL);
List<Log> result = logsystem.queryLog(query);
//Delete Log
int success = logsystem.deleteLog(query);
```
Used Redis as in-memory database and MongoDB as persistent database.
