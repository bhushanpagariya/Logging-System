# Logging-System

UCI CS 223 - Logging System

This project is done as part of CS 223 - Transaction Management course at UC Irvine. 
Implemented Logging System supporting different types of payload such as XML, JSON and Text. Following APIs are implemented as part of this project:-

1. WriteLog - Write Transactional and Non-Transactional logs
2. QueryLog - Query logs using EQUAL, GT, LT, GTE, LTE and Range operator
3. DeleteLog - Delete logs satisfying user query
4. FlushLog - Flush logs from in-memory database to persistent database. Also has support for partial flushing in case of transactional logs

Example:
1. //Create an instance of the LoggingSystem class
2. LoggingSystem logsystem = new LoggingSystem();
3. List\<Log> logList = new ArrayList<\>();
4. //Create a new Log
5. Log log = new Log("\<XML>\</XML>",CONSTANTS.LOG_TYPE_XML);
6. //Append to logList
7. logList.add(log)
8. //Write non-transactional log
9. List\<Long> lsnList = logsystem.writeLog(logList);
10. //Flush logs
11. logsystem.flushLog(lsnList.get(0));
12. //Query Log
13. QueryGen query = new QueryGen();
14. query.append(new QuerySpec(CONSTANTS.FIELD_LSN,lsnList.get(0),QuerySpec.Operator.EQUAL);
15. List\<Log> result = logsystem.queryLog(query);
16. //Delete Log
17. int success = logsystem.deleteLog(query);

Used Redis as in-memory database and MongoDB as persistent database.
