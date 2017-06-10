package Test;

import application.Log;
import application.LoggingSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bhushan on 6/8/17.
 */
public class NonTxWorker implements Runnable{
    //    String filename;
    Log log;
    LoggingSystem logsystem;

    public NonTxWorker(Log log, LoggingSystem logsystem){
        this.log = log;
        this.logsystem = logsystem;
    }
    @Override
    public void run() {
        for (int i = 0;i < 10;i++){
            List<Log> loglist = new ArrayList<>();
            loglist.add(this.log);
            this.logsystem.writeLog(loglist);
//            System.out.println(i);
        }


    }

}
