package Transaction;

import Commons.TransactionDirectory;
import Redis.RedisHandler;
import Tuple.TestTuple;

import java.util.ArrayList;

/**
 * Created by adityajoshi on 4/26/17.
 */
public class TransactionHandler {
    RedisHandler redisHandler;
    TransactionDirectory transactionDirectory;
    String txID = "";

    /**
     * Initialize transaction handler
     * @param txID
     * @param redisHandler
     * @param transactionDirectory
     */
    public TransactionHandler(String txID,RedisHandler redisHandler, TransactionDirectory transactionDirectory){
        this.txID = txID;
        this.redisHandler = redisHandler;
        this.transactionDirectory = transactionDirectory;
    }

    /**
     * read all non-flushed logs corresponding to a particular transaction
     * @return
     */
    public ArrayList<String> readTransactionRecords(){
        ArrayList<String> LSNList = transactionDirectory.get(txID);
        ArrayList<String> retList = new ArrayList<String>();

        for(String LSN:LSNList){
            retList.add(redisHandler.readRecord(LSN));
        }
        return retList;
    }

    /**
     * Write record to log
     * @param LSN
     * @param tuple
     * @return 1 if write is successful, -1 if its unsuccessful
     */
    public int writeTransactionRecord(String LSN, TestTuple tuple){
        return redisHandler.addRecord(LSN,tuple);
    }

    /**
     * delete all records corresponding to a tranasaction input in the list
     * @param LSNList
     * @return 1 if all the records in the list are deleted, -1 otherwise
     */
    public int deleteTransactionRecords(ArrayList<String> LSNList){
        int count = 0;
        for (String LSN: LSNList){
            int ret = redisHandler.deleteRecord(LSN);
            if(ret == 1) {
                transactionDirectory.removeLSN(txID, LSN);
                count += ret;
            }
            else
                break;
        }

        if(count == LSNList.size())
            return 1;
        return -1;
    }
}
