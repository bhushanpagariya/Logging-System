package application;

import Util.*;
import com.google.gson.Gson;
import handlers.mongodbhandle.MongoDBHandler;
import handlers.redishandle.Transaction.RedisTransactionHandler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by bhushan on 5/15/17.
 */
public class LoggingSystem implements LoggingSystemInterface {
    private SequenceGenerator txSequenceGen;
    private SequenceGenerator lsnSequenceGen;
    private RedisTransactionHandler redisTransactionHandler;
    private MongoDBHandler mongoDBHandler;
    private Gson gson;

    public LoggingSystem() {
        this.txSequenceGen = new SequenceGenerator();
        this.lsnSequenceGen = new SequenceGenerator();
        this.redisTransactionHandler = new RedisTransactionHandler();
        this.mongoDBHandler = new MongoDBHandler();
        this.gson = new Gson();
    }

    /**
     * Begin method for transactional logs
     * @return unique transaction id
     */
    @Override
    public synchronized String begin() {
        return this.txSequenceGen.getNextSequence();
    }

    /**
     * Commit method for transactional logs
     * @param txid
     * @return 1 if success, 0 if failure
     */
    @Override
    public synchronized int commit(String txid) {
        return flushTransactionLogs(txid);
    }

    /**
     * Abort method for transaction logs
     * @param txid
     * @return 1 if success, 0 if failure
     */
    @Override
    public synchronized int abort(String txid) {
        List<Long> lsnlist = this.redisTransactionHandler.readAllRecordsForTransaction(txid);
        for(Long lsn : lsnlist)
            this.redisTransactionHandler.deleteRecord(lsn);
        return 1;
    }

    /**
     * Write log for non-transactional system
     * @param logs - list of logs
     * @return list of lsn
     */
    @Override
    public synchronized List<Long> writeLog(List<Log> logs) {
        List<Long> lsnList = new ArrayList<>();
        for(Log log : logs) {
            Long lsn = lsnSequenceGen.getNextSequenceNumber();
            lsnList.add(lsn);
            if(this.redisTransactionHandler.insertRecord(lsn, new Record(log, lsn))!=1)
                return null;
        }
        return lsnList;
    }

    /**
     * Write log for transactional system
     * @param logs
     * @param txid
     * @return list of lsn
     */
    @Override
    public synchronized List<Long> writeLog(List<Log> logs, String txid) {
        List<Long> lsnList = new ArrayList<>();
        for(Log log : logs) {
            Long lsn = lsnSequenceGen.getNextSequenceNumber();
            lsnList.add(lsn);
            if(this.redisTransactionHandler.insertRecord(lsn, new Record(txid, log, lsn))!=1)
                return null;
        }
        return lsnList;
    }

    /**
     * Flush logs till given LSN
     * NOTE: For now it should be used for Stateless txn only
     * @param lsn - highest lsn of logs to be flushed
     * @return 1 if success, 0 otherwise
     */
    @Override
    public synchronized int flushLog(Long lsn) {
        // Fetch all logs till given LSN from redis
        Map<String, List<Long>> mapTxLsn = this.redisTransactionHandler.readAllRecordsBeforeLsn(lsn);
        if(mapTxLsn.size() == 0)
            return 1;

        List<Long> dirtylogs = new ArrayList<>();
        Iterator it = mapTxLsn.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            String txid = ((String)pair.getKey());
            if(!txid.equals(CONSTANTS.NON_TRANSACTIONAL_ID)) {
                //Transactional logs
                this.mongoDBHandler.begin(txid);
                List<Long> lsnlist = (List<Long>)pair.getValue();
                for(Long lsn_entry : lsnlist) {
                    dirtylogs.add(lsn);
                    if(this.mongoDBHandler.insertRecord(this.redisTransactionHandler.readRecord(lsn_entry), txid)!=1) {
                        // Delete all dirty logs
                        for(Long dirtyLogLsn : dirtylogs) {
                            QueryGen dirtyLogQuery = new QueryGen();
                            dirtyLogQuery.append(new QuerySpec("lsn", dirtyLogLsn, QuerySpec.Operator.EQUAL));
                            this.mongoDBHandler.deleteRecord(dirtyLogQuery.toString());
                        }
                        // Raise error flag
                        return 0;
                    }
                }
                // Delete log from redis
                for(Long lsn_entry : lsnlist)
                    this.redisTransactionHandler.deleteRecord(lsn_entry);
            } else {
                // Non transactional logs
                List<Long> lsnlist = (List<Long>) pair.getValue();
                for (Long lsn_entry : lsnlist) {
                    this.mongoDBHandler.insertRecord(this.redisTransactionHandler.readRecord(lsn_entry));
                    this.redisTransactionHandler.deleteRecord(lsn_entry);
                }
            }
            it.remove();
        }
        return 1;
    }



    /**
     * Flush logs corresponding to transaction
     * @param txid
     * @return 1 if success, 0 otherwise
     */
    private synchronized int flushTransactionLogs(String txid) {
        List<Long> listLsn = this.redisTransactionHandler.readAllRecordsForTransaction(txid);
        //Atomic flushing
        this.mongoDBHandler.begin(txid);
        for(Long lsn_entry : listLsn) {
            if(this.mongoDBHandler.insertRecord(this.redisTransactionHandler.readRecord(lsn_entry), txid)!=1) {
                // Failure
                this.mongoDBHandler.abort(txid);
                return 0;
            }
        }
        // Successfully flushed all the logs
        if(this.mongoDBHandler.commit(txid)!=1) {
            this.mongoDBHandler.abort(txid);
            return 0;
        }
        // All transaction flushed to mongodb, delete logs from redis
        for(Long lsn_entry : listLsn) {
            this.redisTransactionHandler.deleteRecord(lsn_entry);
        }
        return 1;
    }

//    /**
//     * Query log based on any parameter in record
//     * Supported comparator operator: EQUAL, GT, GTE, LT, LTE
//     * @param record
//     * @param comparator
//     * @return list of logs satisfying conditions
//     */
//    @Override
//    public synchronized List<Log> queryLog(Record record, QueryComparator.Comparator comparator) {
//        String queryString = QueryComparator.createQueryString(record, comparator);
//        List<String> listLogJson = this.mongoDBHandler.readRecord(queryString);
//        Gson gson = new Gson();
//        List<Log> listLogs = new ArrayList<>();
//        for(String logJson : listLogJson) {
//            Record rec = gson.fromJson(logJson, Record.class);
//            listLogs.add(new Log(rec));
//        }
//        return listLogs;
//    }
//
//    /**
//     * Range query for logs
//     * @param lhsRecord
//     * @param rhsRecord
//     * @param lhsInclusive whether left value is inclusive or not
//     * @param rhsInclusive whether right value is inclusive or not
//     * @return list of logs satisfying conditions
//     */
//    @Override
//    public synchronized List<Log> queryLog(Record lhsRecord, Record rhsRecord, boolean lhsInclusive, boolean rhsInclusive) {
//        String queryString = QueryComparator.createQueryString(lhsRecord, rhsRecord, lhsInclusive, rhsInclusive);
//        List<String> listLogJson = this.mongoDBHandler.readRecord(queryString);
//        Gson gson = new Gson();
//        List<Log> listLogs = new ArrayList<>();
//        for(String logJson : listLogJson) {
//            Record record = gson.fromJson(logJson, Record.class);
//            listLogs.add(new Log(record));
//        }
//        return null;
//    }

    /**
     * Query logs from both in-memory and persistent database
     * Supported queries - EQUAL,LT,LTE,GT,GTE,Range Queries
     * @param query
     * @return list of logs
     */
    @Override
    public synchronized List<Log> queryLog(QueryGen query) {
        List<Log> listLogs = new ArrayList<>();
        // Redis Query
        List<String> listLogJson = this.redisTransactionHandler.queryRedis(query.getQuerySpecList());
        for(String logJson : listLogJson) {
            Record record = this.gson.fromJson(logJson, Record.class);
            listLogs.add(new Log(record));
        }
        // MongoDB Query
        listLogJson = this.mongoDBHandler.readRecord(query.toString());
        for(String logJson : listLogJson) {
            Record record = gson.fromJson(logJson, Record.class);
            listLogs.add(new Log(record));
        }
        return listLogs;
    }

    @Override
    public synchronized int deleteLog(Long lsn) {
        return this.redisTransactionHandler.deleteRecord(lsn);
    }

    @Override
    public synchronized int deleteLog(QueryGen query) {
        query.append(new QuerySpec("txid", CONSTANTS.NON_TRANSACTIONAL_ID, QuerySpec.Operator.EQUAL));
        List<String> listLogJson = this.redisTransactionHandler.queryRedis(query.getQuerySpecList());
        Record record;
        for(String log : listLogJson) {
            record = this.gson.fromJson(log, Record.class);
            this.redisTransactionHandler.deleteRecord(record.getLsn());
        }
        return 1;
    }

    @Override
    public synchronized int deleteLog(QueryGen query, String txid) {
        query.append(new QuerySpec("txid", txid, QuerySpec.Operator.EQUAL));
        List<String> listLogJson = this.redisTransactionHandler.queryRedis(query.getQuerySpecList());
        Record record;
        for(String log : listLogJson) {
            record = this.gson.fromJson(log, Record.class);
            this.redisTransactionHandler.deleteRecord(record.getLsn());
        }
        return 1;
    }

    // Test methods
    public List<String> getAllEntriesOfRedis() {
        return this.redisTransactionHandler.scanComplete();
    }

    public List<String> getAllEntriesOfMongoDB() {
        return this.mongoDBHandler.readRecord();
    }

}
