package Test;

import Util.CONSTANTS;
import application.Log;
import application.LoggingSystem;
import Util.QueryGen;
import Util.QuerySpec;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bhushan on 6/10/17.
 */
public class DeleteTest {
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

        // Commit transaction
        logsystem.commit(txid);

        //Print Database state
        System.out.println("$$$$$$$$$$ Print after commit");
        TestUtil.printDatabases(logsystem);

        //Write log again
        txid = logsystem.begin();
        logsystem.writeLog(logList, txid);

        txid = logsystem.begin();
        logsystem.writeLog(logList, txid);

        txid = logsystem.begin();
        logsystem.writeLog(logList, txid);

        logsystem.writeLog(logList);

        //Print after delete
        System.out.println("$$$$$$$$$$ Print BEFORE delete");
        TestUtil.printDatabases(logsystem);



        QueryGen queryGen = new QueryGen();
        queryGen.append(new QuerySpec("lsn", 30, QuerySpec.Operator.LT));
        logsystem.deleteLog(queryGen, "2");

        //Print after delete
        System.out.println("$$$$$$$$$$ Print after delete");
        TestUtil.printDatabases(logsystem);

    }

}
