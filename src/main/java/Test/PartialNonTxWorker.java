package Test;

import application.Log;
import application.LoggingSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bhushan on 6/9/17.
 */
public class PartialNonTxWorker implements Runnable{
    //    String filename;
    Log log;
    LoggingSystem logsystem;

    public PartialNonTxWorker(Log log, LoggingSystem logsystem){
        this.log = log;
        this.logsystem = logsystem;
    }
    @Override
    public void run() {
        for (int i = 0;i < 20;i++){
            List<Log> loglist = new ArrayList<>();
            loglist.add(this.log);
            List<Long> lsnlist = this.logsystem.writeLog(loglist);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(i==15)
                this.logsystem.flushLog(lsnlist.get(0));
        }

    }

}
