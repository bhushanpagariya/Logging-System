package Redis;

import Tuple.TestTuple;
import redis.clients.jedis.Jedis;

/**
 * Created by adityajoshi on 4/25/17.
 */
public class RedisHandler {
    Jedis jedis;
    /**
     * Default constructor for RedisHandler with redis mirror at local host
     * Default connection Port:6379
     */
    public RedisHandler(){
        jedis = new Jedis("localhost");
        System.out.println(jedis.ping());
        System.out.println("Connection with redis successful");
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
     * @param tuple
     * @return 1 if successful, -1 if unsuccessful
     */
    public int addRecord(String LSN,TestTuple tuple){
        String value = tuple.getJson();
        String reply = jedis.set(LSN,value);
        if(reply.equals("OK"))
            return 1;
        return -1;
    }

    /**
     * Get tuple corresponding the LSN provided
     * @param LSN
     * @return JSON stored in redis corresponding to the key: LSN
     */
    public String readRecord(String LSN){
        return jedis.get(LSN);
    }

    public int deleteRecord(String LSN){
        long ret = jedis.del(LSN);
        if(ret == 1){
            return (int)ret;
        }
        return -1;
    }

    public static void main(String[] args){
        RedisHandler redisHandler = new RedisHandler();
        TestTuple testTuple = new TestTuple("123","234","234234");
        TestTuple testTuple1 = new TestTuple("124","234","234234");
        redisHandler.addRecord("123",testTuple);
        redisHandler.addRecord("123",testTuple1);
        System.out.println(redisHandler.readRecord("234"));
        redisHandler.jedis.close();
    }



}
