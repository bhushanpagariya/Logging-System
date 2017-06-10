package handlers.redishandle.Commons;

import java.util.*;

/**
 * Created by adityajoshi on 4/25/17.
 */
public class TransactionDirectory {
    Map<String, List<Long>> transactionToLSN;

    /**
     * Initialize the dictionary
     */
    public TransactionDirectory(){

        transactionToLSN = new HashMap<>();
    }

    /**
     * Put LSN corresponding to a transaction in a hashmap
     * @param transaction
     * @param LSN
     */
    public synchronized void put(String transaction, Long LSN){
        if(!transactionToLSN.containsKey(transaction))
            transactionToLSN.put(transaction,new ArrayList<Long>());
        transactionToLSN.get(transaction).add(LSN);
    }

    /**
     * Get list of LSN's corresponding to a transaction
     * @param transaction
     * @return list of LSN
     */
    public synchronized List<Long> get(String transaction){
        if(!transactionToLSN.containsKey(transaction))
            return null;
        return new ArrayList<Long>(transactionToLSN.get(transaction));
    }

    /**
     * Get list of LSN's corresponding to a transaction before given lsn
     * @param transaction
     * @return list of LSN
     */
    public synchronized List<Long> get(String transaction, Long lsn){
        if(!transactionToLSN.containsKey(transaction))
            return null;
        List<Long> listLsn = new ArrayList<>();
        for(Long lsnEntry : transactionToLSN.get(transaction)) {
            if(lsnEntry==lsn) {
                listLsn.add(lsnEntry);
                return listLsn;
            }
            listLsn.add(lsnEntry);
        }
        return listLsn;
    }


    /**
     * Get list of LSN's corresponding to a transaction before given lsn
     * @param lsn
     * @return list of LSN
     */
    public synchronized  Map<String,List<Long>> getPreviousLSN(Long lsn){
        Map<String,List<Long>> validTxLSN = new HashMap<>();
        Iterator it = this.transactionToLSN.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            List<Long> validLSN = new ArrayList<>();
            for(Long lsnEntry : (ArrayList<Long>) pair.getValue()) {
                if(lsnEntry<=lsn)
                    validLSN.add(lsnEntry);
                else
                    break;
            }
            if(validLSN.size()!=0)
                validTxLSN.put((String)pair.getKey(), validLSN);
            it.remove();
        }
        return validTxLSN;
    }


    /**
     * Remove given LSN from the list of the transaction
     * @param txid
     * @param lsn
     */
    public synchronized void removeLSN(String txid, Long lsn) {
        if(!transactionToLSN.containsKey(txid))
            return;
        transactionToLSN.get(txid).remove(lsn);
    }
}
