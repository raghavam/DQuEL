package knoelab.classification;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import knoelab.classification.pipeline.PipelineManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Hashing;


/**
 * Classifies the given Ontology using the CEL implementation
 * approach - that of using queues. See the paper titled
 * "Is Tractable Reasoning in Extensions of the Description Logic EL Useful in Practice?"
 * for more details.
 * 
 * @author Raghava
 */

public class ELClassifier implements Classifier {
	
	private ShardedJedis shardedJedis;
	private Jedis localQueueStore;
	private PropertyFileHandler propertyFileHandler;
	private Set<byte[]> conceptsToProcess;
	private PipelineManager pipelineManager;
	private String charset;
	private final int NUM_BYTES = 4;
	private Set<byte[]> r3Values;
	
	public ELClassifier() throws Exception {
		propertyFileHandler = PropertyFileHandler.getInstance();
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		List<HostInfo> hostInfoList = propertyFileHandler.getAllShardsInfo();
		for(HostInfo hostInfo : hostInfoList)
			shards.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
		shardedJedis = new ShardedJedis(shards);
		HostInfo localQueuehostInfo = propertyFileHandler.getLocalHostInfo();
		localQueueStore = new Jedis(localQueuehostInfo.getHost(), 
				localQueuehostInfo.getPort(), Constants.INFINITE_TIMEOUT);	
		charset = propertyFileHandler.getCharset();
		byte[] localKeys = propertyFileHandler.getLocalKeys().getBytes(charset);
		conceptsToProcess = localQueueStore.smembers(localKeys);	
		pipelineManager = new PipelineManager(hostInfoList, 
				propertyFileHandler.getPipelineQueueSize());
	}
	
	@Override
	public void classify() throws Exception {
		// restricting the no of concepts for testing
		double restrictionPercentage = 1;
		int limit = (int)(conceptsToProcess.size() * restrictionPercentage);
		Set<byte[]> restrictedConcepts = new HashSet<byte[]>(limit);
		for(byte[] concept : conceptsToProcess) {
			restrictedConcepts.add(concept);
			limit--;
			if(limit == 0)
				break;
		}
		classify(restrictedConcepts);
	}
	
	@Override
	public void classify(final Set<byte[]> queuesToProcess) throws Exception {
		
		// for all queues, process each element of the queue
		Set<byte[]> conceptQueues = new HashSet<byte[]>(queuesToProcess);
		long count = 1;
		boolean continueProcessing = true;
		while(continueProcessing) {
			double conceptCount = 0;
			System.out.println("Iteration: " + count++);
			System.out.println("No of concepts to process: " + conceptQueues.size());
			
			for (byte[] concept : conceptQueues) {
				System.out.println("\nAbout to process: " + idToConceptStr(concept));
				double progress = (conceptCount++/conceptQueues.size())*100.00;
//				System.out.format("Progress %f: ", progress); 
				System.out.println("  ConceptCount: " + conceptCount + 
						"  Time: " + new Date().toString() + "\n");
				
				// retrieve R3(A) once for each concept A
				byte[] R3ofAKey = KeyGenerator.generateResultRoleConceptKey(concept);
				// selective synch to avoid the problem of missing 
				// both the pre-conditions in CR4 rule.
				pipelineManager.selectiveSynch(R3ofAKey);
				r3Values = localQueueStore.smembers(R3ofAKey);
				
				byte[] QofX = KeyGenerator.generateQueueKey(concept);
				while (localQueueStore.scard(QofX) != 0) {
					byte[] queueEntry = localQueueStore.spop(QofX);
					process(concept, queueEntry);
				}
				r3Values.clear();
				//TODO: why not do a pipeline synch here?
			}
			System.out.println("Checking whether all queues are empty");
			// check if any queue is non-empty
			continueProcessing = false;
			conceptQueues.clear();

			// TODO: Remove this and in process(), processNewEdge(), do a selective synch
			
			conceptQueues = checkIfEmpty();
			if(!conceptQueues.isEmpty())
				continueProcessing = true;
			else {
				pipelineManager.synchAll();
				// check again
				conceptQueues = checkIfEmpty();
				if(!conceptQueues.isEmpty())
					continueProcessing = true;
			}
		}	
		copyRepsToEquiConcepts();
		
		// final synch
		pipelineManager.synchAll();
	}
	
	private Set<byte[]> checkIfEmpty() {
		Set<byte[]> conceptQueues = new HashSet<byte[]>();
		for (byte[] concept : conceptsToProcess) {
			byte[] QofX = KeyGenerator.generateQueueKey(concept);
			if (localQueueStore.scard(QofX) != 0)
				conceptQueues.add(concept);
		}
		
		return conceptQueues;
	}
	
	private void copyRepsToEquiConcepts() throws Exception {
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
					pipelineManager.psadd(ecl, eclKey, value);			
			}
		}
	}
	
	@Override
	public void releaseResources() {
		localQueueStore.disconnect();
		shardedJedis.disconnect();
		pipelineManager.closeAll();
	}
	
	public static void main(String[] args) throws Exception {
		ELClassifier classifier = new ELClassifier();
		try {
			GregorianCalendar cal1 = new GregorianCalendar();
			classifier.classify();
			GregorianCalendar cal2 = new GregorianCalendar();
			double diff = (cal2.getTimeInMillis() - cal1.getTimeInMillis())/1000;
			long completionTimeMin = (long)diff/60;
			double completionTimeSec = diff - (completionTimeMin * 60);
			System.out.println("Classification completed in " + completionTimeMin + " mins and " + completionTimeSec + " secs");
		}
		finally {
			classifier.releaseResources();
		}
	}
	
	
	private void process(byte[] key, byte[] entry) throws Exception {
		
		// check whether the entry is B, B->B' or 3r.B
		byte[] SofA = KeyGenerator.generateSuperclassKey(key);
		if((entry.length > NUM_BYTES) && 
				(entry[0] == SymbolType.COMPLEX_AXIOM_SEPARATOR.getByteValue())) {
			// this is of type B1 ^ B2 ^ B3 ..... ^ Bn -> B'
			// first byte is the entry type, next 4 bytes is the super concept(B')
			byte[] conjunct = new byte[NUM_BYTES];
			byte[] superConcept = new byte[NUM_BYTES];
			int offset = 1;
			for(int i = offset, index=0; i < offset+NUM_BYTES; i++, index++)
				superConcept[index] = entry[i];
			offset += NUM_BYTES;
			boolean isMember = true;
			for(int i = offset, index=0; (i < entry.length) && isMember; i++, index++) {
				conjunct[index] = entry[i];
				//check this: doesn't isMember, conjunct get overwritten?
				if(i%NUM_BYTES == 0) {
					isMember = localQueueStore.sismember(SofA, conjunct);
					index = -1;
					conjunct = new byte[NUM_BYTES];	
				}
			}
			if(isMember)
				process(key, superConcept);
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
			
			// selective synch the shard where the key resultRole is present
//T			pipelineManager.selectiveSynch(resultRole);
			
			if(!shardedJedis.sismember(resultRole, conceptB))
				processNewEdge(key, role, conceptB);
			}
		else {
			// this is of type B
			if(entry.length != NUM_BYTES)
				throw new Exception("Expecting a queue entry of simple type. Len: " + entry.length);
			
			// selective synch to avoid processing the same element (entry) again & again.
//T			pipelineManager.selectiveSynch(key);
			
			if(!localQueueStore.sismember(SofA, entry)) {	
				pipelineManager.psadd(key, SofA, entry);
				final byte[] QofA = KeyGenerator.generateQueueKey(key);
				byte[] PofX = KeyGenerator.generateAxiomKey(entry);					
					   
				final Set<byte[]> queueValues = shardedJedis.getShard(entry).smembers(PofX);
				System.out.println("process - Size of O(X): " + queueValues.size());
				for(byte[] value : queueValues)
					pipelineManager.psadd(key, QofA, value);
					
				// get the values of R3(A)						
//				String r3key = "R3(" + idToConceptStr(key) + ")";
					
				System.out.println("process-Size of R3(A): " + r3Values.size());
				for(byte[] value : r3Values) {
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
					System.out.println("\tprocess - Size of O(3r.X): " + classExpressionEntries.size());
					
					for(byte[] ce : classExpressionEntries)
						pipelineManager.psadd(conceptB, QofB, ce);			
				}					
			}
		}
	}
	
	private void processNewEdge(byte[] conceptA, byte[] role, byte[] conceptB) throws Exception {
		
		byte[] roleAxiomKey = KeyGenerator.generateAxiomKey(role);
		Set<byte[]> superRoles = shardedJedis.smembers(roleAxiomKey);
		
		byte[] resultRoleR3 = KeyGenerator.generateResultRoleConceptKey(conceptB);
		final byte[] QofA = KeyGenerator.generateQueueKey(conceptA);
		byte[] superClassB = KeyGenerator.generateSuperclassKey(conceptB);
		// selective synch to avoid the problem of missing 
		// both the pre-conditions in CR4 rule.
//T		pipelineManager.selectiveSynch(conceptB);
		Set<byte[]> values = shardedJedis.getShard(conceptB).smembers(superClassB);
		System.out.println("\tprocessNewEdge - No of super roles: " + superRoles.size());
		System.out.println("\tprocessNewEdge - S(B): " + values.size());
		
		for(byte[] s : superRoles) {	
			
			byte[] resultRoleR1 = KeyGenerator.generateResultRoleCompoundKey1(s, conceptA);
			byte[] resultRoleR2 = KeyGenerator.generateResultRoleCompoundKey2(s, conceptB);			
			// R1(s#A) = B, R2(s#B) = A, R3(B) = s#A 
			pipelineManager.psadd(resultRoleR1, conceptB);
			pipelineManager.psadd(resultRoleR2, conceptA);
			pipelineManager.psadd(conceptB, resultRoleR3, 
					ByteBuffer.allocate(2*NUM_BYTES).put(s).put(conceptA).array());
			
			// insert into Q(A)			
			for(byte[] dashB : values) {				
				byte[] existentialKey = KeyGenerator.generateExistentialAxiomKey(s, dashB);
				final Set<byte[]> existentialValues = shardedJedis.smembers(existentialKey);
//				System.out.println("processNewEdge - Size of O(3s.B'): " + existentialValues.size());
				for(byte[] value : existentialValues)
					pipelineManager.psadd(conceptA, QofA, value);			
			}

			// check M2(s) && R2(t#A) -- t o s < u
			byte[] propertyChainKeyReverse = KeyGenerator.generatePropertyChainKey2(s);
			Set<byte[]> propertyChainReverseValues = shardedJedis.smembers(propertyChainKeyReverse);
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
//T				pipelineManager.selectiveSynch(compoundKey);
				Set<byte[]> compoundRoleValues = shardedJedis.smembers(compoundKey);
				System.out.println("\tprocessNewEdge - propchain1: " + compoundRoleValues.size());
				for(byte[] value : compoundRoleValues) {
					// check if (A',B) \in R(u)
					byte[] resultSuperRole = KeyGenerator.generateResultRoleCompoundKey1(role3, value);
//T					pipelineManager.selectiveSynch(resultSuperRole);
					if(!shardedJedis.sismember(resultSuperRole, conceptB))
						processNewEdge(value, role3, conceptB);
				}
			}
			
			// check M1(s) && R1(t#B) -- s o t < u
			byte[] propertyChainKey = KeyGenerator.generatePropertyChainKey1(s);
			Set<byte[]> propertyChainValues = shardedJedis.smembers(propertyChainKey);
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
//T				pipelineManager.selectiveSynch(compoundKey);
				Set<byte[]> compoundRoleValues = shardedJedis.smembers(compoundKey);
				System.out.println("\tprocessNewEdge - propchain2: " + compoundRoleValues.size());
				for(byte[] value : compoundRoleValues) {
					// check if (A,B') \in R(u)
					byte[] resultSuperRole = KeyGenerator.generateResultRoleCompoundKey1(role3, conceptA);
//T					pipelineManager.selectiveSynch(resultSuperRole);
					if(!shardedJedis.sismember(resultSuperRole, value))
						processNewEdge(conceptA, role3, value);
				}
			}
		}
	}
	
	private String idToConceptStr(byte[] conceptID) throws Exception {
		byte[] concept = shardedJedis.get(conceptID);
		return new String(concept, "UTF-8");
	}
}