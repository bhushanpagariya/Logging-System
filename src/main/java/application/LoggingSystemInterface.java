package application;

import Util.QueryGen;

import java.util.List;

/**
 * Created by bhushan on 5/15/17.
 */
public interface LoggingSystemInterface {
    /**
     * Begin method for stateful transactions
     * @return transaction id
     */
    String begin();

    /**
     * Commits stateful transaction
     */
    int commit(String txid);

    /**
     * Aborts stateful transaction
     */
    int abort(String txid);

    /**
     * Accepts list of logs from non transactional system and dumps them into database
     * @param logs - list of logs
     * @return list of lsn
     */
    List<Long> writeLog(List<Log> logs);

    /**
     * Accepts list of logs from transactional system and dumps them into database
     * @param logs
     * @param txid
     * @return list of lsn
     */
    List<Long> writeLog(List<Log> logs, String txid);

//    /**
//     * Query log based on any parameter in record
//     * Supported comparator operator: EQUAL, GT, GTE, LT, LTE
//     * @param record
//     * @param comparator
//     * @return list of logs satisfying conditions
//     */
//    public List<Log> queryLog(Record record, QueryComparator.Comparator comparator);
//
//    /**
//     * Range query for logs
//     * @param lhsRecord
//     * @param rhsRecord
//     * @param lhsInclusive whether left value is inclusive or not
//     * @param rhsInclusive whether right value is inclusive or not
//     * @return list of logs satisfying conditions
//     */
//    public List<Log> queryLog(Record lhsRecord, Record rhsRecord, boolean lhsInclusive, boolean rhsInclusive);

    /**
     * Query logs from both in-memory and persistent database
     * Supported queries - EQUAL,LT,LTE,GT,GTE,Range Queries
     * @param query - Query object
     * @return list of logs
     */
    List<Log> queryLog(QueryGen query);

    /**
     * Flush logs till LSN lsn
     * @param lsn - highest lsn of logs to be flushed
     * @return returns 1 if success and 0 if failure
     */
    int flushLog(Long lsn);

    /**
     * Deletes logs with given lsn
     * @param lsn - LSN of logs to be deleted
     * @return 1 if success and 0 if failure
     */
    int deleteLog(Long lsn);

    /**
     * Delete log based on query
     * Supported comparator operator: EQUAL, GT, GTE, LT, LTE
     * @param query - query object
     * @return 1 if success, 0
     */
    int deleteLog(QueryGen query);

    int deleteLog(QueryGen query, String txid);

}
