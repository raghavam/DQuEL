package knoelab.classification;

import java.io.UnsupportedEncodingException;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;

public class RedisTest {

	private static String localKeys = "localkeys";
	
	public static void main(String[] args) throws Exception {	
		readTest();
//		iotest(Integer.parseInt(args[0]));
//		iotest1(Integer.parseInt(args[0]));
	}
	
	public static void readTest() throws Exception {
		Jedis jedis = new Jedis("nimbus2.cs.wright.edu", 6379);
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		String charset = propertyFileHandler.getCharset();
//		byte[] localKeys = propertyFileHandler.getLocalKeys().getBytes(charset);
		GregorianCalendar startTime = new GregorianCalendar();
		Set<byte[]> conceptsToProcess = jedis.keys("*".getBytes(charset));	
		int totalConcepts = conceptsToProcess.size();
		System.out.println("No of concepts: " + totalConcepts);
		int count = 0;
		int conceptCount = 0;
		int progressCount = 1;
		
		for(byte[] concept : conceptsToProcess) {
			try {
				jedis.smembers(concept);
				conceptCount++;
				if(conceptCount == (int)(totalConcepts * progressCount * 0.1)) {
					System.out.println("Completed another 10%: " + conceptCount);
					progressCount++;
				}
			}
			catch(Exception e) { count++; jedis.get(concept); }
		}
		
		GregorianCalendar endTime = new GregorianCalendar();
		double diff = (endTime.getTimeInMillis() - startTime.getTimeInMillis())/1000;
		long completionTimeMin = (long)diff/60;
		double completionTimeSec = diff - (completionTimeMin * 60);
		System.out.println("Exception count: " + count);
		System.out.println("Completed in " + completionTimeMin + " mins and " + completionTimeSec + " secs");
	}
	
	public static void iotest(int maxNumber) throws Exception {		
		Jedis jedis = new Jedis("nimbus2.cs.wright.edu", 6379);		
		GregorianCalendar startTime = new GregorianCalendar();
		createRandomSets(jedis, maxNumber);
		GregorianCalendar endTime1 = new GregorianCalendar();
		System.out.println("Created random sets: " + maxNumber);
		readIterateSets(jedis);
		GregorianCalendar endTime2 = new GregorianCalendar();
		jedis.disconnect();
		double diff = (endTime2.getTimeInMillis() - startTime.getTimeInMillis())/1000;
		long completionTimeMin = (long)diff/60;
		double completionTimeSec = diff - (completionTimeMin * 60);
		System.out.println("Completed in " + completionTimeMin + " mins and " + completionTimeSec + " secs");
				
		double diff1 = (endTime1.getTimeInMillis() - startTime.getTimeInMillis())/1000;
		long completionTimeMin1 = (long)diff1/60;
		double completionTimeSec1 = diff1 - (completionTimeMin1 * 60);
		System.out.println("Writing in " + completionTimeMin1 + " mins and " + completionTimeSec1 + " secs");
		
		double diff2 = (endTime2.getTimeInMillis() - endTime1.getTimeInMillis())/1000;
		long completionTimeMin2 = (long)diff2/60;
		double completionTimeSec2 = diff2 - (completionTimeMin2 * 60);
		System.out.println("Reading in " + completionTimeMin2 + " mins and " + completionTimeSec2 + " secs");
	}

	public static void createRandomSets(Jedis jedis, int max) throws Exception {
		Random r = new Random();
		byte[] localKeyByte = localKeys.getBytes("UTF-8");
//		Pipeline p = jedis.pipelined();
		for(int i = 1; i <= max; i++) {
			byte[] key = new byte[4];
			r.nextBytes(key);
			for(int j = 1; j <= 20; j++) {
				byte[] value = new byte[4];
				r.nextBytes(value);
//				p.sadd(key, value);
				jedis.sadd(key, value);
			}
//			if(i%101 == 0)
//				p.sync();
//			p.sadd(localKeyByte, key);		
			jedis.sadd(localKeyByte, key);
		}
//		p.sync();
	}
	
	public static void readIterateSets(Jedis jedis) throws Exception {
		byte[] localKeyByte = localKeys.getBytes("UTF-8");
		Set<byte[]> keys = jedis.smembers(localKeyByte);
		System.out.println("Key count: " + keys.size());
//		Pipeline p = jedis.pipelined();
		int count = 0;
		for(byte[] k : keys) {
			count++;
//			p.smembers(k);
			jedis.smembers(k);
//			if(count%100 == 0)
//				p.sync();
		}
		// syncing the keys which are not multiples of 100
//		p.sync();
	}
	
	public static void iotest1(int maxNumber) throws Exception {
		Jedis jedis = new Jedis("nimbus2.cs.wright.edu", 6379);
		GregorianCalendar startTime = new GregorianCalendar();
		createRandomSets1(jedis, maxNumber);
		GregorianCalendar endTime1 = new GregorianCalendar();
		System.out.println("Created random sets: " + maxNumber);
		readIterateSets1(jedis);
		GregorianCalendar endTime2 = new GregorianCalendar();
		jedis.disconnect();
		double diff = (endTime2.getTimeInMillis() - startTime.getTimeInMillis())/1000;
		long completionTimeMin = (long)diff/60;
		double completionTimeSec = diff - (completionTimeMin * 60);
		System.out.println("Completed in " + completionTimeMin + " mins and " + completionTimeSec + " secs");
				
		double diff1 = (endTime1.getTimeInMillis() - startTime.getTimeInMillis())/1000;
		long completionTimeMin1 = (long)diff1/60;
		double completionTimeSec1 = diff1 - (completionTimeMin1 * 60);
		System.out.println("Writing in " + completionTimeMin1 + " mins and " + completionTimeSec1 + " secs");
		
		double diff2 = (endTime2.getTimeInMillis() - endTime1.getTimeInMillis())/1000;
		long completionTimeMin2 = (long)diff2/60;
		double completionTimeSec2 = diff2 - (completionTimeMin2 * 60);
		System.out.println("Reading in " + completionTimeMin2 + " mins and " + completionTimeSec2 + " secs");
	}
	
	public static void createRandomSets1(Jedis jedis, int max) throws Exception {
		Random r = new Random();
		byte[] localKeyByte = localKeys.getBytes("UTF-8");
		Pipeline p = jedis.pipelined();
		for(int i = 1; i <= max; i++) {
			byte[] key = new byte[4];
			r.nextBytes(key);
			p.set(key, key);
			if(i%99 == 0)
				p.sync();
			p.sadd(localKeyByte, key);		
//			jedis.sadd(localKeyByte, key);
		}
		p.sync();
	}
	
	public static void readIterateSets1(Jedis jedis) throws Exception {
		byte[] localKeyByte = localKeys.getBytes("UTF-8");
		Set<byte[]> keys = jedis.smembers(localKeyByte);
		System.out.println("Key count: " + keys.size());
		Pipeline p = jedis.pipelined();
		int count = 0;
		for(byte[] k : keys) {
			count++;
			p.get(k);
//			jedis.smembers(k);
			if(count%100 == 0)
				p.sync();
		}
		// syncing the keys which are not multiples of 100
		p.sync();
	}
	
	public void timeTest() {
		// size (max) of each item in the queue
		// number (max) of items in a queue
		// time taken to get a large queue from other node
		// processing time for each item
		// desired throughput -- no. of items processed per second
/*		
		if(args.length != 1 || args[0].isEmpty()) {
			System.out.println("Give the path of owl file");
    		System.exit(-1);
		}
*/		

		Jedis localQueueStore = new Jedis("localhost", 6379);
//		Jedis otherQueueStore = new Jedis("nimbus5.cs.wright.edu", 6379);
		
		GregorianCalendar startTime1 = new GregorianCalendar();
		// get random keys and check for queue size and item size.
		Set<String> localQueueItems = localQueueStore.smembers(localQueueStore.randomKey());
		GregorianCalendar endTime1 = new GregorianCalendar();
		double diff1 = (endTime1.getTimeInMillis() - startTime1.getTimeInMillis());
		// start timer
/*		
		startTime1 = new GregorianCalendar();
		Set<String> otherQueueItems = otherQueueStore.smembers(otherQueueStore.randomKey());
		endTime1 = new GregorianCalendar();
		double diff2 = (endTime1.getTimeInMillis() - startTime1.getTimeInMillis());
*/		
		// Now, to find largest queue and biggest item across the queues
		Set<String> localKeys = localQueueStore.keys("P{*}");
//		Set<String> otherKeys = otherQueueStore.keys("P{*}");
		System.out.println("No of P{} local keys: " + localKeys.size());
//		System.out.println("No of P{} other keys: " + otherKeys.size());
		int qsize = 0;
		String qsizeKey = null;
		int itemSize = 0;
		String item = null;
		double avgQueueSize1 = 0;
		double avgQueueSize2 = 0;
		// check in local queues 
		startTime1 = new GregorianCalendar();
		System.out.println("Checking in local queues");
		for(String key : localKeys) {
			Set<String> members = localQueueStore.smembers(key);
			avgQueueSize1 += members.size();
			if(qsize < members.size()) {
				qsize = members.size();
				qsizeKey = key;
			}
			for(String m : members)
				if(itemSize < m.length()) {
					itemSize = m.length();
					item = m;
				}
		}
		endTime1 = new GregorianCalendar();
		avgQueueSize1 = avgQueueSize1/localKeys.size();
		System.out.println("Queue size: " + qsize + " Key: " + qsizeKey);
		System.out.println("Avg Queue Size: " + avgQueueSize1);
		System.out.println("Item size: " + itemSize + " Item: " + item);
		diff1 = (endTime1.getTimeInMillis() - startTime1.getTimeInMillis());
		System.out.println("Time taken: " + diff1 + " in secs: " + diff1/1000);
		
		// check in local queues 
/*		
		System.out.println("\nChecking in other queues");
		startTime1 = new GregorianCalendar();
		for(String key : otherKeys) {
			Set<String> members = otherQueueStore.smembers(key);
			avgQueueSize2 += members.size();
			if(qsize < members.size()) {
				qsize = members.size();
				qsizeKey = key;
			}
			for(String m : members)
				if(itemSize < m.length()) {
					itemSize = m.length();
					item = m;
				}
		}
		endTime1 = new GregorianCalendar();
		avgQueueSize2 = avgQueueSize2/otherKeys.size();
		System.out.println("Queue size: " + qsize + " Key: " + qsizeKey);
		System.out.println("Avg Queue Size: " + avgQueueSize2);
		System.out.println("Item size: " + itemSize + " Item: " + item);
		diff1 = (endTime1.getTimeInMillis() - startTime1.getTimeInMillis());
		System.out.println("Time taken: " + diff1 + " in secs: " + diff1/1000);
*/		
		byte[] itemBytes = item.getBytes();
		System.out.println("In bytes: " + itemBytes.length);
	}
}
