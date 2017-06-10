package handlers.redishandle.Redis;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by adityajoshi on 4/25/17.
 */
public class RedisHandler {
    Jedis jedis;
    private String query;
    /**
     * Default constructor for RedisHandler with redis mirror at local host
     * Default connection Port:6379
     */
    public RedisHandler(){
        jedis = new Jedis("localhost");
        jedis.flushAll();
        System.out.println(jedis.ping());
        System.out.println("Connection with redis successful");
        this.query = null;
    }

    /**
     * Constructor for RedisHandler with redis mirror at specified IPAddress and Port
     * @param IPAddress
     */
    public RedisHandler(String IPAddress, int port){
        Jedis jedis = new Jedis(IPAddress,port);
        System.out.println("Connection with redis successful");
    }

    /**
     *  Add tuple to redis.
     * @param tupleJson
     * @return 1 if successful, -1 if unsuccessful
     */
    public synchronized int addRecord(Long LSN, String tupleJson){
        String reply = jedis.set(LSN.toString(),tupleJson);
        if(reply.equals("OK")) {
            return 1;
        }
        return -1;
    }

    /**
     * Get tuple corresponding the LSN provided
     * @param LSN
     * @return JSON stored in redis corresponding to the key: LSN
     */
    public synchronized String readRecord(Long LSN){
        return jedis.get(LSN.toString());
    }

    public synchronized String readRecord(String LSN){
        return jedis.get(LSN);
    }

    public synchronized int deleteRecord(Long LSN){
        long ret = jedis.del(LSN.toString());
        if(ret == 1){
            return (int)ret;
        }
        return -1;
    }

    public synchronized List<String> scanComplete() {
        Set<String> keys = jedis.keys("*");
//        System.out.println("No of entries in redis:- "+keys.size());
        List<String> allEntries = new ArrayList<>();
        for(String key : keys) {
            allEntries.add(readRecord(key));
        }
        return allEntries;
    }
}
