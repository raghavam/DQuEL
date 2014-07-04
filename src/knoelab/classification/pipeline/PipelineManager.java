package knoelab.classification.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import knoelab.classification.Constants;
import knoelab.classification.HostInfo;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;

public class PipelineManager {

	private HashMap<String, LinkedBlockingQueue<PipelineMessage>> shardQueue;
	private HashMap<String, Jedis> jedisShards;
	private ShardedJedis shardedJedis;
//	private ExecutorService threadExecutor;
	
	public PipelineManager(List<HostInfo> hostInfoList, int queueBlockingSize) {
		shardQueue = new HashMap<String, LinkedBlockingQueue<PipelineMessage>>(hostInfoList.size());
		jedisShards = new HashMap<String, Jedis>(hostInfoList.size());
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		
		for(HostInfo hostInfo : hostInfoList) {
			shards.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
			String mapKey = hostInfo.getHost() + hostInfo.getPort();
			LinkedBlockingQueue<PipelineMessage> pipelineQueue = 
				new LinkedBlockingQueue<PipelineMessage>(queueBlockingSize);
			Jedis jedis = new Jedis(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT);
			shardQueue.put(mapKey, pipelineQueue);
			jedisShards.put(mapKey, jedis);
		}
		shardedJedis = new ShardedJedis(shards);
//		final int MAX_THREADS = Runtime.getRuntime().availableProcessors() * 3;
//		threadExecutor = Executors.newFixedThreadPool(MAX_THREADS);
	}
	
	public void pset(byte[] key, byte[] value) {
		insert(key, key, value, PipelineMessageType.SET);
	}
	
	public void psadd(byte[] queueKey, byte[] value) {
		psadd(queueKey, queueKey, value);
	}
	
	public void psadd(byte[] shardKey, byte[] queueKey, byte[] value) {
		insert(shardKey, queueKey, value, PipelineMessageType.SADD);
	}
	
	private synchronized void insert(byte[] shardKey, byte[] queueKey, 
			byte[] value, PipelineMessageType msgType) {
		PipelineMessage pipelineMessage = new PipelineMessage(queueKey, 
				value, msgType);
		LinkedBlockingQueue<PipelineMessage> queue;
		JedisShardInfo shardInfo = shardedJedis.getShardInfo(shardKey);
		String mapKey = shardInfo.getHost() + shardInfo.getPort();			
		queue = shardQueue.get(mapKey);
		boolean insertSuccessful = queue.offer(pipelineMessage);
		if(!insertSuccessful) {
			// perform the pipeline sync - flush the queue
//			System.out.println("Queue is full: " + mapKey + "  queue size: " + queue.size());
//			threadExecutor.execute(new PipelineMessageProcessor(queue, jedisShards.get(mapKey)));
//			System.out.println("insert() - queue full? " + queue.size() + "   " + new Date().toString());
			synchQueue(queue, jedisShards.get(mapKey));						
			// queue would be empty now. Insert (k,v)
			queue.offer(pipelineMessage);
		}
	}
	
	private void synchQueue(LinkedBlockingQueue<PipelineMessage> queue, Jedis jedis) {
		
		if(queue.isEmpty())
			return; 
		
		List<PipelineMessage> msglist = new ArrayList<PipelineMessage>(queue.size());
		// javadoc says draining is faster than repeated polling
		queue.drainTo(msglist);
		Pipeline p = jedis.pipelined();
		for(PipelineMessage pipelineMsg : msglist) {
			switch(pipelineMsg.messageType) {
				case SET: p.set(pipelineMsg.key, pipelineMsg.value);
				break;
				
				case SADD: p.sadd(pipelineMsg.key, pipelineMsg.value);
				break;
				
				default: try { throw new Exception(); } 
						 catch (Exception e) { e.printStackTrace(); }
			}
		}
		p.sync();
	}
	
	/**
	 * Selectively synch a particular queue. This method is 
	 * useful when reads need to work on the present consistent 
	 * state of a particular shard.
	 * 
	 * @param shardKey 
	 */
	public synchronized void selectiveSynch(byte[] shardKey) {
		JedisShardInfo shardInfo = shardedJedis.getShardInfo(shardKey);
		String mapKey = shardInfo.getHost() + shardInfo.getPort();	
//		System.out.println("Selective synch - queue size: " + shardQueue.get(mapKey).size());
		synchQueue(shardQueue.get(mapKey), jedisShards.get(mapKey));	
	}
	
	/**
	 * Flush all the queues and process the messages (pipeline sync all)
	 */
	public synchronized void synchAll() {
		Set<Entry<String, LinkedBlockingQueue<PipelineMessage>>> 
						shardQueueEntries = shardQueue.entrySet();
		for(Entry<String, LinkedBlockingQueue<PipelineMessage>> 
									entry : shardQueueEntries) {
//			System.out.println("synchAll() - size of queue: " + entry.getValue().size() + "   " + new Date().toString());
			synchQueue(entry.getValue(), jedisShards.get(entry.getKey()));
		}
	}
/*	
	public void forceSynchAll(PipelineManager pipelineManager) {
		CountDownLatch latch = new CountDownLatch(1);
		Thread t = new Thread(new PipelineSyncher(pipelineManager, latch));
		t.start();
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
*/	
	/**
	 * Close all jedis connections
	 */
	public void closeAll() {
//		threadExecutor.shutdown();
		Collection<Jedis> jedisList = jedisShards.values();
		for(Jedis jedis : jedisList)
			jedis.disconnect();
		shardedJedis.disconnect();
	}
	
	public void synchAndCloseAll() {
		synchAll();
		closeAll();
	}
}

class PipelineSyncher implements Runnable {
	private PipelineManager pipelineManager;
	private CountDownLatch latch;
	
	PipelineSyncher(PipelineManager pipelineManager, CountDownLatch latch) {
		this.pipelineManager = pipelineManager;
		this.latch = latch;
	}

	@Override
	public void run() {
		pipelineManager.synchAll();
		latch.countDown();
	}	
}


class PipelineMessageProcessor implements Runnable {
	private LinkedBlockingQueue<PipelineMessage> queue;
	private Jedis jedis;
	
	PipelineMessageProcessor(LinkedBlockingQueue<PipelineMessage> queue,
			Jedis jedis) {
		this.queue = queue;
		this.jedis = jedis;
	}
	
	@Override
	public void run() {
		List<PipelineMessage> msglist = new ArrayList<PipelineMessage>(queue.size());
		// javadoc says draining is faster than repeated polling
		queue.drainTo(msglist);
		Pipeline p = jedis.pipelined();
		for(PipelineMessage pipelineMsg : msglist) {
			switch(pipelineMsg.messageType) {
				case SET: p.set(pipelineMsg.key, pipelineMsg.value);
				break;
				
				case SADD: p.sadd(pipelineMsg.key, pipelineMsg.value);
				break;
				
				default: try { throw new Exception(); } 
						 catch (Exception e) { e.printStackTrace(); }
			}
		}
		p.sync();
	}
	
}

