package Commons;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by adityajoshi on 4/25/17.
 */
public class TransactionDirectory {
    HashMap<String,ArrayList<String>> transactionToLSN;

    /**
     * Initialize the dictionary
     */
    public TransactionDirectory(){
        transactionToLSN = new HashMap<String, ArrayList<String>>();
    }

    /**
     * Put LSN corresponding to a transaction in a hashmap
     * @param transaction
     * @param LSN
     */
    public void put(String transaction, String LSN){
        if(!transactionToLSN.containsKey(transaction))
            transactionToLSN.put(transaction,new ArrayList<String>());
        transactionToLSN.get(transaction).add(LSN);
    }

    /**
     * Get list of LSN's corresponding to a transaction
     * @param transaction
     * @return
     */
    public ArrayList<String> get(String transaction){
        if(!transactionToLSN.containsKey(transaction))
            return null;
        return new ArrayList<String>(transactionToLSN.get(transaction));
    }

    /**
     * Remove given LSN from the list of the transaction
     * @param txid
     * @param lsn
     */
    public void removeLSN(String txid, String lsn) {
        if(!transactionToLSN.containsKey(txid))
            return;
        transactionToLSN.get(txid).remove(lsn);
    }
}
