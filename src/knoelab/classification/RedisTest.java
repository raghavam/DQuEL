package knoelab.classification;

import java.util.GregorianCalendar;
import java.util.Random;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisTest {

	private static String localKeys = "localkeys";
	
	public static void main(String[] args) throws Exception {	
//		readTest();
		
		//Test results: Byte[] vs String. Turns out that String reading
		//is faster and writing is slightly slower than byte[] reading/writing
		iotestByte(Integer.parseInt(args[0]));
		iotestString(Integer.parseInt(args[0]));
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
	
	public static void iotestByte(int maxNumber) throws Exception {		
		Jedis jedis = new Jedis("nimbus2", 6379);		
		System.out.println("Creating and writing byte[] random sets to DB");
		createRandomSets(jedis, maxNumber);
		System.out.println("\nReading byte[] sets from DB");
		readIterateSets(jedis);
		jedis.disconnect();
	}

	private static void createRandomSets(Jedis jedis, int max) throws Exception {
		long startTime1 = System.nanoTime();
		deleteKeys(jedis);
		Random r = new Random();
		byte[] localKeyByte = localKeys.getBytes("UTF-8");
		Pipeline p = jedis.pipelined();
		for(int i = 1; i <= max; i++) {
			byte[] key = new byte[4];
			r.nextBytes(key);
			for(int j = 1; j <= 20; j++) {
				byte[] value = new byte[4];
				r.nextBytes(value);
				p.sadd(key, value);
//				jedis.sadd(key, value);
			}
//			if(i%101 == 0)
//				p.sync();
			p.sadd(localKeyByte, key);		
//			jedis.sadd(localKeyByte, key);
		}
		long startTime2 = System.nanoTime();
		p.sync();
		long endTime = System.nanoTime();
		double diffTime1 = (endTime - startTime1)/(double)1000000000;
		double diffTime2 = (endTime - startTime2)/(double)1000000000;
		System.out.println("Total time taken: " + diffTime1);
		System.out.println("Time taken for sync(): " + diffTime2);
	}
	
	private static void readIterateSets(Jedis jedis) throws Exception {
		long startTime1 = System.nanoTime();
		byte[] localKeyByte = localKeys.getBytes("UTF-8");
		Set<byte[]> keys = jedis.smembers(localKeyByte);
		System.out.println("Key count: " + keys.size());
		Pipeline p = jedis.pipelined();
		for(byte[] k : keys) {
			p.smembers(k);
//			jedis.smembers(k);
//			if(count%100 == 0)
//				p.sync();
		}
		long startTime2 = System.nanoTime();
		p.sync();
		long endTime = System.nanoTime();
		double diffTime1 = (endTime - startTime1)/(double)1000000000;
		double diffTime2 = (endTime - startTime2)/(double)1000000000;
		System.out.println("Total time taken: " + diffTime1);
		System.out.println("Time taken for sync(): " + diffTime2);
	}
	
	private static void deleteKeys(Jedis jedis) {
		boolean anotherAttempt = true; 
		while(anotherAttempt) {
			try {
				jedis.flushAll();
				anotherAttempt = false;
			}
			catch(Exception e) {
				anotherAttempt = true;
				System.out.println(e.getMessage());
				System.out.println("Exception - trying again");
			}
		}
	}
	
	public static void iotestString(int maxNumber) throws Exception {
		Jedis jedis = new Jedis("nimbus2", 6379);		
		System.out.println("\nCreating and writing random String sets to DB");
		createRandomSets1(jedis, maxNumber);
		System.out.println("\nReading String sets from DB");
		readIterateSets1(jedis);
		jedis.disconnect();
	}
	
	private static void createRandomSets1(Jedis jedis, int max) throws Exception {
		long startTime1 = System.nanoTime();
		deleteKeys(jedis);
		Random r = new Random();
		Pipeline p = jedis.pipelined();
		for(int i = 1; i <= max; i++) {
			long key = r.nextLong();
			for(int j = 1; j <= 20; j++) {
				long value = r.nextLong();
				p.sadd(Long.toString(key), Long.toString(value));
//				jedis.sadd(key, value);
			}
//			if(i%101 == 0)
//				p.sync();
			p.sadd(localKeys, Long.toString(key));		
//			jedis.sadd(localKeyByte, key);
		}
		long startTime2 = System.nanoTime();
		p.sync();
		long endTime = System.nanoTime();
		double diffTime1 = (endTime - startTime1)/(double)1000000000;
		double diffTime2 = (endTime - startTime2)/(double)1000000000;
		System.out.println("Total time taken: " + diffTime1);
		System.out.println("Time taken for sync(): " + diffTime2);
	}
	
	private static void readIterateSets1(Jedis jedis) throws Exception {
		long startTime1 = System.nanoTime();
		Set<String> keys = jedis.smembers(localKeys);
		System.out.println("Key count: " + keys.size());
		Pipeline p = jedis.pipelined();
		for(String k : keys) {
			p.smembers(k);
//			jedis.smembers(k);
//			if(count%100 == 0)
//				p.sync();
		}
		long startTime2 = System.nanoTime();
		p.sync();
		long endTime = System.nanoTime();
		double diffTime1 = (endTime - startTime1)/(double)1000000000;
		double diffTime2 = (endTime - startTime2)/(double)1000000000;
		System.out.println("Total time taken: " + diffTime1);
		System.out.println("Time taken for sync(): " + diffTime2);
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
