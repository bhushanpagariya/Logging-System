package handlers.mongodbhandle;

import Util.SequenceGenerator;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bhushan on 5/15/17.
 */
public class MongoDBHandler {
    private final String HOST_ADDR = "localhost";
    private final int PORT_NUM = 27017;
    private final String DATABASE_NAME = "logmanager";
    private final String COLLECTION_NAME = "logmanager";
    private final String TX_DB_NAME = "TXNDB";
    private final String TX_COLLECTION_NAME = "TXNCOL";
    private final String TX_ID_KEY = "_txid";
    private MongoClient client;
    private MongoDatabase mongodb;
    private SequenceGenerator sequenceGenerator;

    /**
     * Constructor - connecting to MongoDB on localhost and default port number
     */
    public MongoDBHandler(){
        this.client = new MongoClient(HOST_ADDR, PORT_NUM);
        this.mongodb = client.getDatabase(DATABASE_NAME);
        this.sequenceGenerator = new SequenceGenerator();
        // Check if there are some transactions which are not yet committed
        // If present delete corresponding entries form database
        MongoDatabase txndb = this.client.getDatabase(TX_DB_NAME);
        MongoCollection<Document> txncol = txndb.getCollection(TX_COLLECTION_NAME);
        FindIterable<Document> iterable = txncol.find();
        MongoCursor<Document> cursor = iterable.iterator();
        while(cursor.hasNext()) {
            String query = "{\""+TX_ID_KEY+"\":\""+cursor.next().get(TX_ID_KEY)+"\"}";
            // Delete all entries related to transaction from database
            deleteRecord( query );
            // Delete entry from transaction database
            txncol.deleteMany(Document.parse( query ));
        }

    }

    /**
     * Constructor - connecting to MongoDB on remote machine
     */
    public MongoDBHandler(String remoteHostAddr, int remotePortNum, String dbName){
        MongoClient client = new MongoClient(remoteHostAddr, remotePortNum);
        this.mongodb = client.getDatabase(dbName);
    }

    /**
     * Insert entry in mongo db
     * @param jsonstr - record to be inserted in json format
     */
    public int insertRecord(String jsonstr) {
        MongoCollection<Document> collection = this.mongodb.getCollection(this.COLLECTION_NAME);
        collection.insertOne(Document.parse( jsonstr ));
        return 1;
    }

    public int insertRecord(String jsonstr, String txid) {
        MongoCollection<Document> collection = this.mongodb.getCollection(this.COLLECTION_NAME);
        collection.insertOne(Document.parse( jsonstr ).append(TX_ID_KEY, txid));
        return 1;
    }

    /**
     * Scan Record based on condition mentioned in query
     * @param query - query
     * @return return list of JSON object matched record
     */
    public List<String> readRecord(String query) {
//        System.out.println("Query String:- "+query);
        List<String> matchedRecord = new ArrayList<String>();
        MongoCollection<Document> collection = this.mongodb.getCollection(this.COLLECTION_NAME);
        FindIterable<Document> iterable = collection.find(Document.parse( query )).projection(Projections.exclude(TX_ID_KEY, "_id"));
        MongoCursor<Document> cursor = iterable.iterator();
        while(cursor.hasNext()) {
            matchedRecord.add(cursor.next().toJson());
        }
        return matchedRecord;
    }

    /**
     * Scan all records
     * @return return list of all records in JSON format
     */
    public List<String> readRecord() {
        List<String> matchedRecord = new ArrayList<String>();
        MongoCollection<Document> collection = this.mongodb.getCollection(this.COLLECTION_NAME);
        FindIterable<Document> iterable = collection.find().projection(Projections.exclude(TX_ID_KEY, "_id"));
        MongoCursor<Document> cursor = iterable.iterator();
        int count = 0;
        while(cursor.hasNext()) {
            count++;
            matchedRecord.add(cursor.next().toJson());
        }
//        System.out.println("Number of records in MongoDB:- "+count);
        return matchedRecord;
    }

    /**
     * Delete all matching records
     * @param query - query in json format
     * @return 1 if success, 0 for failure
     */
    public int deleteRecord(String query) {
        MongoCollection<Document> collection = this.mongodb.getCollection(this.COLLECTION_NAME);
        collection.deleteMany(Document.parse( query ));
        return 1;
    }

    /**
     * Delete all records
     * @return 1 if success, 0 for failure
     */
    public int deleteRecord() {
        MongoCollection<Document> collection = this.mongodb.getCollection(this.COLLECTION_NAME);
        collection.deleteMany(new BasicDBObject());
        return 1;
    }

    /**
     * Support for transactions
     * Begin of the transaction, return TXID assigned to it
     * @return
     */
    public int begin(String txid) {
        // Generate next txid
//        String txid = this.sequenceGenerator.getNextSequence();
        // Insert txid entry in Transaction Database
        MongoDatabase txndb = this.client.getDatabase(TX_DB_NAME);
        MongoCollection<Document> txncol = txndb.getCollection(TX_COLLECTION_NAME);
        // Checking if txid already present in transactions db
        String query = "{\""+TX_ID_KEY+"\":\""+txid+"\"}";
        FindIterable<Document> iterable = txncol.find( Document.parse( query ));
        MongoCursor<Document> cursor = iterable.iterator();
        if(cursor.hasNext()) {
            // txid found in Transaction database
            return 0;
        }

        Document document = new Document();
        document.put(TX_ID_KEY, txid);
        txncol.insertOne(document);
        return 1;
    }

    /**
     * Commit the transaction identified by txid
     * Delete the entry of txid from Transaction database
     * @param txid
     * @return 1 if success and 0 if failure
     */
    public int commit(String txid) {
        MongoDatabase txndb = this.client.getDatabase(TX_DB_NAME);
        MongoCollection<Document> txncol = txndb.getCollection(TX_COLLECTION_NAME);
        String query = "{\""+TX_ID_KEY+"\":\""+txid+"\"}";
        FindIterable<Document> iterable = txncol.find( Document.parse( query ));
        MongoCursor<Document> cursor = iterable.iterator();
        if(cursor.hasNext()) {
            // txid found in Transaction database
            // Delete entry from transaction database
            txncol.deleteMany(Document.parse( query ));
            return 1;
        }
        return 0;
    }

    /**
     * Abort the transaction identified by txid
     * Delete all entries inserted in DB by give transaction
     * Delete txid entry from Transaction database
     * @param txid
     * @return
     */
    public int abort(String txid) {
        MongoDatabase txndb = this.client.getDatabase(TX_DB_NAME);
        MongoCollection<Document> txncol = txndb.getCollection(TX_COLLECTION_NAME);
        String query = "{\""+TX_ID_KEY+"\":\""+txid+"\"}";
        FindIterable<Document> iterable = txncol.find( Document.parse( query ));
        MongoCursor<Document> cursor = iterable.iterator();
        if(cursor.hasNext()) {
            // Delete all entries related to transaction from database
            deleteRecord( query );
            // Delete entry from transaction database
            txncol.deleteMany(Document.parse( query ));
            return 1;
        }
        return 0;
    }

}
