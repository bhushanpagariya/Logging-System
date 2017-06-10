package Test;

import application.Log;
import application.LoggingSystem;
import handlers.mongodbhandle.MongoDBHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * Created by bhushan on 6/9/17.
 */
public class TestUtil {
    public static Log createLog(String filepath, String type) {
        try {
            String content = new Scanner(new File(filepath)).useDelimiter("\\Z").next();
            return new Log(content, type);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void printDatabases(LoggingSystem logsystem) {
        System.out.println("***********Entries in redis:- ");
        for (String entry : logsystem.getAllEntriesOfRedis())
            System.out.println(entry);

        System.out.println("***********Entries in mongo db:- ");
        for (String entry : logsystem.getAllEntriesOfMongoDB())
            System.out.println(entry);

    }

    public static void clearMongoDB() {
        MongoDBHandler dbhand = new MongoDBHandler();
        dbhand.deleteRecord();
    }
}
