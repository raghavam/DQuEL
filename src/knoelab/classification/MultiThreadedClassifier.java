package knoelab.classification;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import jsr166y.ForkJoinPool;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import extra166y.Ops.Procedure;
import extra166y.ParallelArray;

/**
 * This is a multi-threaded version of ELClassifier
 * @author Raghava
 *
 */
public class MultiThreadedClassifier implements Classifier {

	private Set<byte[]> localConcepts;
	private ForkJoinPool forkJoinPool;
	private PropertyFileHandler propertyFileHandler;
	private String charset;
	private ShardedJedisPool shardedJedisPool;
	private JedisPool localJedisPool;
	private final int NUM_BYTES = 4;
	
	public MultiThreadedClassifier(int multiplier) throws Exception {
		propertyFileHandler = PropertyFileHandler.getInstance();
		charset = propertyFileHandler.getCharset();
		
		final int MAX_THREADS = Runtime.getRuntime().availableProcessors() * multiplier;
        System.out.println("No threads: " + MAX_THREADS);
        forkJoinPool = new ForkJoinPool(MAX_THREADS);
        
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		List<HostInfo> hostInfoList = propertyFileHandler.getAllShardsInfo();
		for(HostInfo hostInfo : hostInfoList)
			shards.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
//		Config poolConfig = new Config();
		// non-positive number indicates an infinite pool
//		poolConfig.maxActive = -1;
		// non-positive number indicates that idle objects in the pool shouldn't be thrown out
//		poolConfig.timeBetweenEvictionRunsMillis = -1; 
		
		shardedJedisPool = new ShardedJedisPool(poolConfig, shards);
		HostInfo localQueuehostInfo = propertyFileHandler.getLocalHostInfo();
		localJedisPool = new JedisPool(poolConfig, localQueuehostInfo.getHost(), 
				localQueuehostInfo.getPort(), Constants.INFINITE_TIMEOUT);	
		Jedis localJedis = localJedisPool.getResource();
		byte[] localKeys = propertyFileHandler.getLocalKeys().getBytes(charset);			
		localConcepts = localJedis.smembers(localKeys);
		localJedisPool.returnResource(localJedis);
	}
	
	@Override
	public void classify() throws Exception {
		classify(localConcepts);
	}
	
	@Override
	public void classify(Set<byte[]> queuesToProcess) throws Exception {
		Jedis localJedis = localJedisPool.getResource();
		ShardedJedis shardedJedis = shardedJedisPool.getResource();
		try {
			byte[][] localConceptBytes = queuesToProcess.toArray(new byte[0][]);
			ParallelArray<byte[]> conceptsToProcess = ParallelArray.createUsingHandoff(localConceptBytes, forkJoinPool);
			MultiThreadedClassifier.QueueProcessor queueProcessor = this.new QueueProcessor();
						
			// for all queues, process each element of the queue
			long count = 1;
			boolean continueProcessing = true;
			while(continueProcessing) {
				System.out.println("Iteration: " + count++);
				conceptsToProcess.apply(queueProcessor);
				
				System.out.println("Checking whether all queues are empty");
				// check if any queue is non-empty
				continueProcessing = false;
				Set<byte[]> nonEmptyQueues = new HashSet<byte[]>();
				for (byte[] concept : localConcepts) {
					byte[] QofX = KeyGenerator.generateQueueKey(concept);
					if (localJedis.scard(QofX) != 0) 						
						nonEmptyQueues.add(concept);
				}
				if(!nonEmptyQueues.isEmpty()) {
					conceptsToProcess = ParallelArray.createUsingHandoff(
							nonEmptyQueues.toArray(new byte[0][]), forkJoinPool);
					continueProcessing = true;
				}
				System.out.println("Rem queues: " + nonEmptyQueues.size());
			}
			copyRepsToEquiConcepts(shardedJedis);
			System.out.println("FJ pool: " + forkJoinPool);
		}
		finally {
			localJedisPool.returnResource(localJedis);
			shardedJedisPool.returnResource(shardedJedis);
			
			// close pipeline here (after all threads die)
		}
	}
	
	@Override
	public void releaseResources() {
		shardedJedisPool.destroy();
		localJedisPool.destroy();
		forkJoinPool.shutdown();
		
		// close pipeline here (after all threads die)
	}
	
	private void copyRepsToEquiConcepts(ShardedJedis shardedJedis) throws Exception {
		// copy S(A) to other equivalent concepts. 
		// Here A is the representative of an equivalence set
		byte[] equiKeys = propertyFileHandler.getEquivalentClassKeys().getBytes(charset);
		Set<byte[]> repEquiConcepts = shardedJedis.smembers(equiKeys);
		for(byte[] repConcept : repEquiConcepts) {
			byte[] repKey = KeyGenerator.generateEquivalentClassAxiomKey(repConcept);
			byte[] SofRepConcept = KeyGenerator.generateSuperclassKey(repConcept);
			Set<byte[]> equiClasses = shardedJedis.smembers(repKey);
			for(byte[] ecl : equiClasses) {
				final byte[] eclKey = KeyGenerator.generateSuperclassKey(ecl);
				final Set<byte[]> sclassValues = shardedJedis.getShard(repConcept).smembers(SofRepConcept);
				sclassValues.add(ecl);
				for(byte[] value : sclassValues)
					shardedJedis.getShard(ecl).sadd(eclKey, value);				
			}
		}
	}
	
	private class QueueProcessor implements Procedure<byte[]> {

		@Override
		public void op(byte[] concept) {
			ShardedJedis shardedJedis = shardedJedisPool.getResource();
			Jedis localQueueStore = localJedisPool.getResource();
			try {
				byte[] QofX = KeyGenerator.generateQueueKey(concept);
				while (localQueueStore.scard(QofX) != 0) {
					byte[] queueEntry = localQueueStore.spop(QofX);
					process(concept, queueEntry, localQueueStore, shardedJedis);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				shardedJedisPool.returnResource(shardedJedis);
				localJedisPool.returnResource(localQueueStore);
			}
		}
		
		private void process(byte[] key, byte[] entry, 
				Jedis localQueueStore, ShardedJedis shardedJedis) throws Exception {
			
			// check whether the entry is B, B->B' or 3r.B
			byte[] SofA = KeyGenerator.generateSuperclassKey(key);
			if((entry.length > NUM_BYTES) && 
					(entry[0] == SymbolType.COMPLEX_AXIOM_SEPARATOR.getByteValue())) {
				// this is of type B1 ^ B2 ^ B3 ..... ^ Bn -> B'
				// first byte is the entry type, next 4 bytes is the super concept(B')
				Pipeline p = localQueueStore.pipelined();
				byte[] conjunct = new byte[NUM_BYTES];
				Set<byte[]> naryConjuncts = new HashSet<byte[]>();
				byte[] superConcept = new byte[NUM_BYTES];
				int offset = 1;
				for(int i = offset, index=0; i < offset+NUM_BYTES; i++, index++)
					superConcept[index] = entry[i];
				offset += NUM_BYTES;
				for(int i = offset, index=0; i < entry.length; i++, index++) {
					conjunct[index] = entry[i];
					if(i%NUM_BYTES == 0) {
						naryConjuncts.add(conjunct);
						index = -1;
						conjunct = new byte[NUM_BYTES];	
					}
				}
				for(byte[] cj : naryConjuncts) 
					p.sismember(SofA, cj);
				List<Object> results = p.syncAndReturnAll();
				// if all are true then call process(A, B')
				boolean insert = true;
				for(Object obj : results) {
					boolean isMember = (Boolean)obj;
					if(!isMember) {
						insert = false;
						break;
					}
				}
				if(insert)
					process(key, superConcept, localQueueStore, shardedJedis);
			}
			else if((entry.length > NUM_BYTES) && 
					(entry[0] == SymbolType.EXISTENTIAL_AXIOM_SEPARATOR.getByteValue())) {				
				// this is of type 3r.B
				byte[] role = new byte[NUM_BYTES];
				byte[] conceptB = new byte[NUM_BYTES];
				int offset = 1;
				for(int i = offset, index=0; i < offset+NUM_BYTES; i++, index++)
					role[index] = entry[i];
				offset += NUM_BYTES;
				for(int i = offset, index=0; i < offset+NUM_BYTES; i++, index++)
					conceptB[index] = entry[i];
				
				// check if (A,B) \in R(r)
				byte[] resultRole = KeyGenerator.generateResultRoleCompoundKey1(role, key);
				if(!shardedJedis.sismember(resultRole, conceptB)) 
					processNewEdge(key, role, conceptB, shardedJedis);
				}
			else {
				// this is of type B
				if(entry.length != NUM_BYTES)
					throw new Exception("Expecting a queue entry of simple type. Len: " + entry.length);
				
				if(!localQueueStore.sismember(SofA, entry)) {				
					localQueueStore.sadd(SofA, entry);
					final byte[] QofA = KeyGenerator.generateQueueKey(key);
					byte[] PofX = KeyGenerator.generateAxiomKey(entry);					
						   
					final Set<byte[]> queueValues = shardedJedis.getShard(entry).smembers(PofX);
					Pipeline p = localQueueStore.pipelined();
					for(byte[] value : queueValues)
						p.sadd(QofA, value);
					p.sync();
						
					// get the values of R3(A)		
					byte[] R3ofAKey = KeyGenerator.generateResultRoleConceptKey(key);
					Set<byte[]> R3ofAValues = localQueueStore.smembers(R3ofAKey);
					for(byte[] value : R3ofAValues) {
						// get r & B					
						byte[] role = new byte[NUM_BYTES];
						byte[] conceptB = new byte[NUM_BYTES];
						int offset = 0;
						for(int i = offset, index=0; i < offset+NUM_BYTES; i++, index++)
							role[index] = value[i];
						offset += NUM_BYTES;
						for(int i = offset, index=0; i < offset+NUM_BYTES; i++, index++)
							conceptB[index] = value[i];
						
						final byte[] QofB = KeyGenerator.generateQueueKey(conceptB);
						byte[] derivedCEKey = KeyGenerator.generateExistentialAxiomKey(role, entry);
						final Set<byte[]> classExpressionEntries = shardedJedis.smembers(derivedCEKey);
						for(byte[] ce : classExpressionEntries)
							shardedJedis.getShard(conceptB).sadd(QofB, ce);				
					}					
				}
			}
		}
		
		
		private void processNewEdge(byte[] conceptA, byte[] role, 
				byte[] conceptB, ShardedJedis shardedJedis) throws Exception {
			
			byte[] roleAxiomKey = KeyGenerator.generateAxiomKey(role);
			Set<byte[]> superRoles = shardedJedis.smembers(roleAxiomKey);
			for(byte[] s : superRoles) {					
				processNewPair(conceptA, conceptB, s, shardedJedis);
				checkFwdRoleChains(conceptA, conceptB, s, shardedJedis);				
				checkRevRoleChains(conceptA, conceptB, s, shardedJedis);
			}
		}
		
		private void processNewPair(byte[] conceptA, byte[] conceptB, 
				byte[] srole, ShardedJedis sjedis) {
			byte[] resultRoleR1 = KeyGenerator.generateResultRoleCompoundKey1(srole, conceptA);
			byte[] resultRoleR2 = KeyGenerator.generateResultRoleCompoundKey2(srole, conceptB);
			byte[] resultRoleR3 = KeyGenerator.generateResultRoleConceptKey(conceptB);
			// R1(s#A) = B, R2(s#B) = A, R3(B) = s#A 
			sjedis.sadd(resultRoleR1, conceptB);
			sjedis.sadd(resultRoleR2, conceptA);
			sjedis.getShard(conceptB).sadd(resultRoleR3, 
					ByteBuffer.allocate(2*NUM_BYTES).put(srole).put(conceptA).array());
			
			// insert into Q(A)
			final byte[] QofA = KeyGenerator.generateQueueKey(conceptA);
			byte[] superClassB = KeyGenerator.generateSuperclassKey(conceptB);
			Set<byte[]> values = sjedis.getShard(conceptB).smembers(superClassB);
			for(byte[] dashB : values) {				
				byte[] existentialKey = KeyGenerator.generateExistentialAxiomKey(srole, dashB);
				final Set<byte[]> existentialValues = sjedis.smembers(existentialKey);
				
				for(byte[] value : existentialValues)
					sjedis.getShard(conceptA).sadd(QofA, value);			
			}
		}
		
		private void checkFwdRoleChains(byte[] conceptA, byte[] conceptB, 
				byte[] srole, ShardedJedis sjedis) throws Exception {
			// check M2(s) && R2(t#A) -- t o s < u
			byte[] propertyChainKeyReverse = KeyGenerator.generatePropertyChainKey2(srole);
			Set<byte[]> propertyChainReverseValues = sjedis.smembers(propertyChainKeyReverse);
			for(byte[] compoundValue : propertyChainReverseValues) {
				// get t and u
				byte[] role2 = new byte[NUM_BYTES];
				byte[] role3 = new byte[NUM_BYTES];
				int offset = 0;
				for(int i = offset, index=0; i < offset+NUM_BYTES; i++, index++)
					role2[index] = compoundValue[i];
				offset += NUM_BYTES;
				for(int i = offset, index=0; i < offset+NUM_BYTES; i++, index++)
					role3[index] = compoundValue[i];
				
				byte[] compoundKey = KeyGenerator.generateResultRoleCompoundKey2(role2, conceptA);
				Set<byte[]> compoundRoleValues = sjedis.smembers(compoundKey);
				for(byte[] value : compoundRoleValues) {
					// check if (A',B) \in R(u)
					byte[] resultSuperRole = KeyGenerator.generateResultRoleCompoundKey1(role3, value);
					if(!sjedis.sismember(resultSuperRole, conceptB))
						processNewEdge(value, role3, conceptB, sjedis);
				}
			}
		}
		
		private void checkRevRoleChains(byte[] conceptA, byte[] conceptB, 
				byte[] srole, ShardedJedis sjedis) throws Exception {
			// check M1(s) && R1(t#B) -- s o t < u
			byte[] propertyChainKey = KeyGenerator.generatePropertyChainKey1(srole);
			Set<byte[]> propertyChainValues = sjedis.smembers(propertyChainKey);
			for(byte[] compoundValue : propertyChainValues) {
				// get t and u
				byte[] role2 = new byte[NUM_BYTES];
				byte[] role3 = new byte[NUM_BYTES];
				int offset = 0;
				for(int i = offset, index=0; i < offset+NUM_BYTES; i++, index++)
					role2[index] = compoundValue[i];
				offset += NUM_BYTES;
				for(int i = offset, index=0; i < offset+NUM_BYTES; i++, index++)
					role3[index] = compoundValue[i];
				
				byte[] compoundKey = KeyGenerator.generateResultRoleCompoundKey1(role2, conceptB);
				Set<byte[]> compoundRoleValues = sjedis.smembers(compoundKey);
				for(byte[] value : compoundRoleValues) {
					// check if (A,B') \in R(u)
					byte[] resultSuperRole = KeyGenerator.generateResultRoleCompoundKey1(role3, conceptA);
					if(!sjedis.sismember(resultSuperRole, value))
						processNewEdge(conceptA, role3, value, sjedis);
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1 || args[0].isEmpty())
			throw new Exception("Provide the multiplier to be used with num threads");
		
		MultiThreadedClassifier mtClassifier = new MultiThreadedClassifier(Integer.parseInt(args[0]));
		try {
			GregorianCalendar cal1 = new GregorianCalendar();
			mtClassifier.classify();
			GregorianCalendar cal2 = new GregorianCalendar();
			double diff = (cal2.getTimeInMillis() - cal1.getTimeInMillis())/1000;
			long completionTimeMin = (long)diff/60;
			double completionTimeSec = diff - (completionTimeMin * 60);
			System.out.println("Classification completed in " + completionTimeMin + " mins and " + completionTimeSec + " secs");
		}
		finally {
			mtClassifier.releaseResources();
		}
	}
}
