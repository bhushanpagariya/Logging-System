package Test;

import Util.CONSTANTS;
import application.LoggingSystem;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by bhushan on 6/9/17.
 */
public class ConcurrencyTest2 {
    public static void main(String[] args) {
        TestUtil.clearMongoDB();
        LoggingSystem loggingSystem = new LoggingSystem();
        // Two threads are adding logs simultaneously
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        for(int i = 0;i < 3;i++){
            if(i%3==0) {
                Worker worker1 = new Worker(TestUtil.createLog("/Users/bhushan/Documents/UCI/Transaction/project/Logging-System/schema/stud1.xml", CONSTANTS.LOG_TYPE_XML), loggingSystem, 1);
                executorService.execute(worker1);
            } else if(i%3==1){
                Worker worker2 = new Worker(TestUtil.createLog("/Users/bhushan/Documents/UCI/Transaction/project/Logging-System/schema/json1.txt", CONSTANTS.LOG_TYPE_JSON), loggingSystem, 1);
                executorService.execute(worker2);
            } else {
                Worker worker3 = new Worker(TestUtil.createLog("/Users/bhushan/Documents/UCI/Transaction/project/Logging-System/schema/text1.txt", CONSTANTS.LOG_TYPE_TEXT), loggingSystem, 1);
                executorService.execute(worker3);
            }
        }
        executorService.shutdown();
        while (!executorService.isTerminated());

        //Print database state
        System.out.println("$$$$$$$$$$$After commit and abort:-");
        TestUtil.printDatabases(loggingSystem);
    }
}
