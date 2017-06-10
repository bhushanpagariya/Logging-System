package application;

import Util.CONSTANTS;
import com.google.gson.Gson;
import handlers.xsdhandle.XMLLoader;

/**
 * Created by bhushan on 5/13/17.
 */
public class Log {
    private String payload;
    private String type;
    private String txid;
    private String timestamp;
    private Long lsn;

    public Log(String payload, String type) {
        this.payload = payload;
        this.type = type;
    }

    public Log(Record record) {
        this.lsn = record.getLsn();
        if(record.getTxid().equals(CONSTANTS.NON_TRANSACTIONAL_ID))
            this.txid = null;
        Gson gson = new Gson();
        switch (record.getType()) {
            case CONSTANTS.LOG_TYPE_XML:
                String jsonstr = gson.toJson(record.getPayload());
                try {
                    this.payload = XMLLoader.marshalXml(gson.fromJson(jsonstr, Class.forName(record.getPayloadClassName())));
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                break;
            case CONSTANTS.LOG_TYPE_JSON:
                this.payload = gson.toJson(record.getPayload());
                break;
            case CONSTANTS.LOG_TYPE_TEXT:
                this.payload = (String) record.getPayload();
                break;
            default:
                this.payload = (String) record.getPayload();
                break;
        }
        this.type = record.getType();
        this.timestamp = record.getTimestamp();
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTxid() {
        return txid;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public Long getLsn() {
        return lsn;
    }
}
