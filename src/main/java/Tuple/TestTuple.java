package Tuple;

import com.google.gson.Gson;

/**
 * Created by adityajoshi on 4/25/17.
 */
public class TestTuple {
    public String LSN,TXID,TimeStamp;

    public TestTuple(String LSN,String TXID,String Timestamp){
        this.LSN = LSN;
        this.TXID = TXID;
        this.TimeStamp = Timestamp;
    }

    public void setLSN(String LSN) {
        this.LSN = LSN;
    }

    public void setTimeStamp(String timeStamp) {
        TimeStamp = timeStamp;
    }

    public void setTXID(String TXID) {
        this.TXID = TXID;
    }

    public String getLSN() {
        return LSN;
    }

    public String getTimeStamp() {
        return TimeStamp;
    }

    public String getTXID() {
        return TXID;
    }

    public String getJson(){
        Gson gson = new Gson();
        String json = gson.toJson(this);
        return json;
    }
}
