package Test;

import Util.CONSTANTS;
import application.Log;
import application.LoggingSystem;
import Util.QueryGen;
import Util.QuerySpec;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bhushan on 6/9/17.
 */
public class QueryTest1 {
    public static void main(String[] args) {
        TestUtil.clearMongoDB();
        LoggingSystem logsystem = new LoggingSystem();
        List<Log> logList = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            logList.add(TestUtil.createLog("/Users/bhushan/Documents/UCI/Transaction/project/Logging-System/schema/stud2.xml", CONSTANTS.LOG_TYPE_XML));
            logList.add(TestUtil.createLog("/Users/bhushan/Documents/UCI/Transaction/project/Logging-System/schema/json1.txt", CONSTANTS.LOG_TYPE_JSON));
            logList.add(TestUtil.createLog("/Users/bhushan/Documents/UCI/Transaction/project/Logging-System/schema/text1.txt", CONSTANTS.LOG_TYPE_TEXT));
        }

        // Write transactional log
        String txid = logsystem.begin();
        logsystem.writeLog(logList, txid);

        //Print Database State
        System.out.println("$$$$$$$$$$ Print after querying logs");
        TestUtil.printDatabases(logsystem);

        //Abort Transaction
        logsystem.abort(txid);

        //Print Database state
        System.out.println("$$$$$$$$$$ Print after abort");
        TestUtil.printDatabases(logsystem);

        //Write log again
        logsystem.writeLog(logList, txid);

        // Commit transaction
        logsystem.commit(txid);

        //Print database sate
        System.out.println("$$$$$$$$$$ Print after commit");
//        TestUtil.printDatabases(logsystem);
        QueryGen queryGen = new QueryGen();
//        queryGen.append(new QuerySpec("lsn", new Long(2), QuerySpec.Operator.LTE));
        queryGen.append(new QuerySpec("payload", "Lorem Ipsum", QuerySpec.Operator.EQUAL));
        List<Log> queryResult = logsystem.queryLog(queryGen);
        for(Log log : queryResult) {
            System.out.print("LSN:- "+log.getLsn());
            System.out.println(log.getPayload());
        }

    }
}
