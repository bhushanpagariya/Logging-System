package handlers.redishandle.Transaction;


import Util.QuerySpec;
import application.Record;
import com.google.gson.Gson;
import handlers.redishandle.Commons.TransactionDirectory;
import handlers.redishandle.Redis.RedisHandler;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by adityajoshi on 4/26/17.
 */
public class RedisTransactionHandler {
    RedisHandler redisHandler;
    TransactionDirectory transactionDirectory;
    private Gson gson;

    /**
     * Initialize transaction handler
     */
    public RedisTransactionHandler(){
        this.redisHandler = new RedisHandler();
        this.transactionDirectory = new TransactionDirectory();
        this.gson = new Gson();
    }

    /**
     * Write record to log
     * @param LSN
     * @param tuple
     * @return 1 if write is successful, -1 if its unsuccessful
     */
    public int insertRecord(Long LSN, Record tuple){
        // Add LSN entry to Transaction Directory
        this.transactionDirectory.put(tuple.getTxid(), LSN);
        // Insert LSN - Tuple pair into Redis DB
        return redisHandler.addRecord(LSN,tuple.getJson());
    }

    /**
     * read log for given LSN
     * @return record in json format
     */
    public String readRecord(Long LSN){
        return this.redisHandler.readRecord(LSN);
    }

    /**
     * Read all logs corresponding to transaction having id txid
     * @param txid
     * @return list of lsn
     */
    public List<Long> readAllRecordsForTransaction(String txid) {
        // Fetch list of lsn corresponding to transaction
        List<Long> listLsn = this.transactionDirectory.get(txid);
        return listLsn;
    }

    /**
     * Read all logs corresponding to transaction before given lsn
     * @param lsn
     * @return list of lsns
     */
    public Map<String, List<Long>> readAllRecordsBeforeLsn(Long lsn) {
        // Fetch record with given LSN
        Record record = this.gson.fromJson(readRecord(lsn), Record.class);
        // Fetch list of lsn corresponding to transaction before lsn
//        List<String> listLsn = this.transactionDirectory.get(record.getTxid(), lsn);
        Map<String, List<Long>> listLsn = this.transactionDirectory.getPreviousLSN(lsn);
        return listLsn;
    }


    /**
     * delete record corresponding to lsn
     * @param lsn
     * @return 1 if record deleted successfully, -1 otherwise
     */
    public int deleteRecord(Long lsn){
        // Fetch record with given LSN
        Record record = this.gson.fromJson(readRecord(lsn), Record.class);

        int ret = redisHandler.deleteRecord(lsn);
        if(ret == 1)
            transactionDirectory.removeLSN(record.getTxid(), lsn);
        else
            return -1;
        return 1;
    }

    /**
     * delete all records corresponding to a tranasaction
     * @param txid
     * @return 1 if all the records in the list are deleted, -1 otherwise
     */
    public int deleteAllTransactionRecords(String txid){
        // Fetch list of lsn corresponding to transaction
        List<Long> listLsn = this.transactionDirectory.get(txid);

        for(Long lsn_entry : listLsn) {
            int ret = redisHandler.deleteRecord(lsn_entry);
            if(ret == 1)
                transactionDirectory.removeLSN(txid, lsn_entry);
            else
                return -1;
        }
        return 1;
    }

    public List<String> scanComplete() {
        return this.redisHandler.scanComplete();
    }

    public List<String> queryRedis(List<QuerySpec> querySpecList) {
        List<String> results = new ArrayList<>();
        List<String> allRecords = this.redisHandler.scanComplete();
        boolean validRecord;
        for(String record : allRecords) {
            validRecord = true;
            for (QuerySpec querySpec : querySpecList) {
                if(!matchQuery(record, querySpec)) {
                    validRecord = false;
                    break;
                }
            }
            if(validRecord)
                results.add(record);
        }
        return results;
    }

    private boolean matchQuery(String jsonstr, QuerySpec querySpec) {
        Object actualVal = fetchKeyFromJson(querySpec.getKey(), jsonstr);
        if(querySpec.getValue() != null) {
            // normal query
            return compareValues(actualVal, querySpec.getValue(), querySpec.getOperator());
        } else {
            //range query
            if(querySpec.isLhsInclusive()) {
                if (!compareValues(actualVal, querySpec.getLhsValue(), QuerySpec.Operator.GTE))
                    return false;
            } else {
                if (!compareValues(actualVal, querySpec.getLhsValue(), QuerySpec.Operator.GT))
                    return false;
            }

            if(querySpec.isRhsInclusive()) {
                if (!compareValues(actualVal, querySpec.getRhsValue(), QuerySpec.Operator.LTE))
                    return false;
            } else {
                if (!compareValues(actualVal, querySpec.getRhsValue(), QuerySpec.Operator.LT))
                    return false;
            }
            return true;
        }
    }

    private boolean compareValues(Object actualValue, Object expectedValue, QuerySpec.Operator operator) {

        if(!actualValue.getClass().equals(expectedValue.getClass()) && !(actualValue instanceof Long) && !(expectedValue instanceof Long))
            return false;
        switch (operator) {
            case EQUAL:
                if(actualValue instanceof Integer) {
                    if (expectedValue instanceof Long) {
                        Long temp = new Long((int) actualValue);
                        return temp.equals((Long)expectedValue);
                    }
                    return (int) actualValue == (int) expectedValue;
                }
                else if(actualValue instanceof Float)
                    return (float)actualValue == (float)expectedValue;
                else if(actualValue instanceof Long)
                    return (Long) actualValue == (Long)expectedValue;
                else if(actualValue instanceof String)
                    return ((String)actualValue).equals((String)expectedValue);
                else return false;
            case GT:
                if(actualValue instanceof Integer) {
                    if (expectedValue instanceof Long) {
                        Long temp = new Long((int) actualValue);
                        return temp > (Long) expectedValue;
                    }
                    return (int) actualValue > (int) expectedValue;
                }
                else if(actualValue instanceof Float)
                    return (float)actualValue > (float)expectedValue;
                else if(actualValue instanceof Long)
                    return (Long) actualValue > (Long)expectedValue;
                else if(actualValue instanceof String)
                    return ((String)actualValue).compareTo((String)expectedValue)>0;
                else return false;
            case LT:
                if(actualValue instanceof Integer) {
                    if (expectedValue instanceof Long) {
                        Long temp = new Long((int) actualValue);
                        return temp < (Long) expectedValue;
                    }
                    return (int) actualValue < (int) expectedValue;
                }
                else if(actualValue instanceof Float)
                    return (float)actualValue < (float)expectedValue;
                else if(actualValue instanceof Long)
                    return (Long) actualValue < (Long)expectedValue;
                else if(actualValue instanceof String)
                    return ((String)actualValue).compareTo((String)expectedValue)<0;
                else return false;
            case GTE:
                if(actualValue instanceof Integer) {
                    if (expectedValue instanceof Long) {
                        Long temp = new Long((int) actualValue);
                        return temp >= (Long) expectedValue;
                    }
                    return (int) actualValue >= (int) expectedValue;
                }
                else if(actualValue instanceof Float)
                    return (float)actualValue >= (float)expectedValue;
                else if(actualValue instanceof Long)
                    return (Long) actualValue >= (Long)expectedValue;
                else if(actualValue instanceof String)
                    return ((String)actualValue).compareTo((String)expectedValue)>=0;
                else return false;
            case LTE:
                if(actualValue instanceof Integer) {
                    if (expectedValue instanceof Long) {
                        Long temp = new Long((int) actualValue);
                        return temp <= (Long) expectedValue;
                    }
                    return (int) actualValue <= (int) expectedValue;
                }
                else if(actualValue instanceof Float)
                    return (float)actualValue <= (float)expectedValue;
                else if(actualValue instanceof Long)
                    return (Long) actualValue <= (Long)expectedValue;
                else if(actualValue instanceof String)
                    return ((String)actualValue).compareTo((String)expectedValue)<=0;
                else return false;
            default:
                return false;
        }
    }

    private Object fetchKeyFromJson(String nestedKey, String jsonstr) throws JSONException {
        String[] queryparts = nestedKey.split("\\.");
        JSONObject jObj = new JSONObject(jsonstr);
        for(int i=0; i<queryparts.length-1;i++ ) {
            jObj = jObj.getJSONObject(queryparts[i]);
        }
        Object res = jObj.get(queryparts[queryparts.length-1]);
        return res;
    }

}
