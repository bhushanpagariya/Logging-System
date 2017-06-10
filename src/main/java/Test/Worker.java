package Test;

import application.Log;
import application.LoggingSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bhushan on 6/8/17.
 */
public class Worker implements Runnable {
//    String filename;
    Log log;
    LoggingSystem logsystem;
    int isCommit;

    public Worker(Log log, LoggingSystem logsystem, int isCommit){
        this.log = log;
        this.logsystem = logsystem;
        this.isCommit = isCommit;
    }


    @Override
    public void run() {
        String txid = this.logsystem.begin();
        for (int i = 0;i < 10;i++){
            List<Log> loglist = new ArrayList<>();
            loglist.add(this.log);
            this.logsystem.writeLog(loglist, txid);
//            System.out.println(i);
        }
        if(isCommit == 1)
            this.logsystem.commit(txid);
        else if(isCommit == -1){
            System.out.println("$$$$$$$$$$$Before Abort:-");
            TestUtil.printDatabases(this.logsystem);
            this.logsystem.abort(txid);
        }

    }
}
