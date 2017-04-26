package Redis;

import Commons.TransactionDirectory;
import Tuple.TestTuple;
import redis.clients.jedis.Jedis;

/**
 * Created by adityajoshi on 4/25/17.
 */
public class RedisHandler {
    Jedis jedis;
    TransactionDirectory transactionDirectory;
    /**
     * Default constructor for RedisHandler with redis mirror at local host
     * Get Transaction directory handler
     * Default connection Port:6379
     */
    public RedisHandler(){
        jedis = new Jedis("localhost");
        System.out.println(jedis.ping());
        System.out.println("Connection with redis successful");
        transactionDirectory = new TransactionDirectory();
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
    public int addRecord(TestTuple tuple){
        String value = tuple.getJson();
        String reply = jedis.set(tuple.LSN,value);
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



    public static void main(String[] args){
        RedisHandler redisHandler = new RedisHandler();
        TestTuple testTuple = new TestTuple("123","234","234234");
        redisHandler.addRecord(testTuple);
        System.out.println(redisHandler.readRecord(testTuple.LSN));
        redisHandler.jedis.close();
    }



}
