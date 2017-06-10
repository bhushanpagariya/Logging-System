package application;

import Util.CONSTANTS;
import com.google.gson.Gson;
import handlers.xsdhandle.XMLLoader;
import org.bson.Document;

import java.util.Date;

/**
 * Created by bhushan on 5/15/17.
 */
public class Record {
    private String txid;
    private Object payload;
    private String type;
    private String timestamp;
    private Long lsn;
    private String payloadClassName;

    public Record() {
        this.txid = null;
        this.payload = null;
        this.type = null;
        this.timestamp = null;
        this.lsn = null;
        this.payloadClassName = null;
    }

    public Record(Log log, Long lsn) {
        this.txid = CONSTANTS.NON_TRANSACTIONAL_ID;
        if(log.getType().toLowerCase().equals(CONSTANTS.LOG_TYPE_JSON)) {
            this.payload = Document.parse(log.getPayload());
            this.payloadClassName = null;
        } else if(log.getType().toLowerCase().equals(CONSTANTS.LOG_TYPE_XML)) {
            this.payload = XMLLoader.unmarshalXml(log.getPayload());
            this.payloadClassName = this.payload.getClass().getName();
        } else if(log.getType().toLowerCase().equals(CONSTANTS.LOG_TYPE_TEXT)) {
            this.payload = log.getPayload();
            this.payloadClassName = null;
        } else {
            this.payload = log.getPayload();
            this.payloadClassName = null;
        }
        this.type = log.getType();
        this.timestamp = new Long(new Date().getTime()).toString();
        this.lsn = lsn;
    }

    public Record(String txid, Log log, Long lsn) {
        this.txid = txid;
        if(log.getType().toLowerCase().equals(CONSTANTS.LOG_TYPE_JSON)) {
            this.payload = Document.parse(log.getPayload());
            this.payloadClassName = null;
        } else if(log.getType().toLowerCase().equals(CONSTANTS.LOG_TYPE_XML)) {
            this.payload = XMLLoader.unmarshalXml(log.getPayload());
            this.payloadClassName = this.payload.getClass().getName();
        } else if(log.getType().toLowerCase().equals(CONSTANTS.LOG_TYPE_TEXT)) {
            this.payload = log.getPayload();
            this.payloadClassName = null;
        } else {
            this.payload = log.getPayload();
            this.payloadClassName = null;
        }
        this.type = log.getType();
        this.timestamp = new Long(new Date().getTime()).toString();
        this.lsn = lsn;
    }

    public String getJson(){
        Gson gson = new Gson();
//        this.payload = gson.toJson(this.payload);
        String json = gson.toJson(this);
        return json;
    }

    public String getTxid() {
        return txid;
    }

    public void setTxid(String txid) {
        this.txid = txid;
    }

    public Object getPayload() {
        return payload;
    }

    public String getType() {
        return type;
    }

    public void setPayloadAndType(Log log) {
        if(log.getType().toLowerCase().equals(CONSTANTS.LOG_TYPE_JSON))
            this.payload = Document.parse(log.getPayload());
        else if(log.getType().toLowerCase().equals(CONSTANTS.LOG_TYPE_XML))
            this.payload = XMLLoader.unmarshalXml(log.getPayload());
        else if(log.getType().toLowerCase().equals(CONSTANTS.LOG_TYPE_TEXT))
            this.payload = log.getPayload();
        else
            this.payload = log.getPayload();
        this.type = log.getType();

    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Long getLsn() {
        return lsn;
    }

    public void setLsn(Long lsn) {
        this.lsn = lsn;
    }

    public String getPayloadClassName() {
        return payloadClassName;
    }
}
