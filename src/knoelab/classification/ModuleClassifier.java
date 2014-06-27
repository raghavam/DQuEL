package knoelab.classification;

import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class ModuleClassifier {

/*	
	
//	private ShardedJedis shardedJedis;
	private Jedis localQueueStore;
	private PropertyFileHandler propertyFileHandler;
	private Set<String> conceptsToProcess;
	
	public ModuleClassifier() {
		propertyFileHandler = PropertyFileHandler.getInstance();
		HostInfo localQueuehostInfo = propertyFileHandler.getLocalHostInfo();
		localQueueStore = new Jedis(localQueuehostInfo.host, localQueuehostInfo.port);	
		conceptsToProcess = new HashSet<String>();
	}
	
	public void classify() throws Exception {
		initializeQueues();
		// for all queues, process each element of the queue
		long count = 1;
		boolean continueProcessing = true;
		while(continueProcessing) {
			System.out.println("Iteration: " + count++);
			for (String concept : conceptsToProcess) {				
				String QofX = KeyGenerator.generateQueueKey(concept);
				while (localQueueStore.scard(QofX) != 0) {
					String queueEntry = localQueueStore.spop(QofX);
					process(concept, queueEntry);
				}
			}
			System.out.println("Checking whether all queues are empty");
			// check if any queue is non-empty
			continueProcessing = false;
			for (String concept : conceptsToProcess) {
				String QofX = KeyGenerator.generateQueueKey(concept);
				if (localQueueStore.scard(QofX) != 0) { 
					continueProcessing = true;
					break;
				}
			}
		}
		localQueueStore.disconnect();
//		shardedJedis.disconnect();
	}
	
	public static void main(String[] args) throws Exception {
		ModuleClassifier classifier = new ModuleClassifier();
		GregorianCalendar cal1 = new GregorianCalendar();
		classifier.classify();
		GregorianCalendar cal2 = new GregorianCalendar();
		double diff = (cal2.getTimeInMillis() - cal1.getTimeInMillis())/1000;
		long completionTimeMin = (long)diff/60;
		double completionTimeSec = diff - (completionTimeMin * 60);
		System.out.println("Classification completed in " + completionTimeMin + " mins and " + completionTimeSec + " secs");
	}
	
	private void initializeQueues() {
		// get all the keys in the local Redis store.
		conceptsToProcess = localQueueStore.smembers(propertyFileHandler.getLocalKeys());
		System.out.println("No of concepts in local queue: " + conceptsToProcess.size());
		
		// initialize Q(X) with P(X)
		for(String key : conceptsToProcess) {
			String PofX = KeyGenerator.generateAxiomKey(key);
			String QofX = KeyGenerator.generateQueueKey(key);
			
			// add P(X) to Q(X)
			localQueueStore.sunionstore(QofX, PofX);
//			if(localQueueStore.scard(PofX) != 0)
//				localQueueStore.sunionstore(QofX, QofX, PofX);
		}
	}
	
	
	private void process(String key, String entry) throws Exception {
		
		// check whether the entry is B, B->B' or 3r.B
		String[] splitStr = entry.split(propertyFileHandler.getComplexAxiomSeparator());
		String SofA = KeyGenerator.generateSuperclassKey(key);
		if(splitStr.length == 2) {
			// this is of type B->B'
			if(localQueueStore.sismember(SofA, splitStr[0])) 
				process(key, splitStr[1]);
		}
		else {
			splitStr = entry.split(propertyFileHandler.getExistentialAxiomSeparator());
			if(splitStr.length == 2) {
				// this is of type 3r.B
				// check if (A,B) \in R(r)
				String resultRole = KeyGenerator.generateResultRoleCompoundKey1(splitStr[0], key);
				if(!localQueueStore.sismember(resultRole, splitStr[1])) 
					processNewEdge(key, splitStr[0], splitStr[1]);
			}
			else {
				// this is of type B
				if(!localQueueStore.sismember(SofA, entry)) {
					localQueueStore.sadd(SofA, entry);
					final String QofA = KeyGenerator.generateQueueKey(key);
					String PofX = KeyGenerator.generateAxiomKey(entry);
					localQueueStore.sunionstore(QofA, QofA, PofX);
					
					// get the values of R3(A)	
					Pipeline p = localQueueStore.pipelined();
					Set<String> R3ofAValues = localQueueStore.smembers(KeyGenerator.generateResultRoleConceptKey(key));
					for(String value : R3ofAValues) {
						// get r & B
						String[] classExpression = value.split(propertyFileHandler.getComplexAxiomSeparator());
						if(classExpression.length != 2)
							throw new Exception("Expecting length 2: " + value + " of R3{" + key + "}");
						final String QofB = KeyGenerator.generateQueueKey(classExpression[1]);
						String derivedCE = new StringBuilder(classExpression[0]).
												append(propertyFileHandler.getExistentialAxiomSeparator()).
												append(entry).toString();
						String derivedCEKey = KeyGenerator.generateAxiomKey(derivedCE);
						// TODO: implement KeysToProcess -- if classExpressionEntries is not empty 
						//		 then add QofB or B to KeysToProcess 
							
						// adding 3r.X to O(3.rX) in the above set
//						p.sadd(QofB, derivedCE);
						p.sunionstore(QofB, QofB, derivedCEKey);
					}
					p.sync();
				}
			}
		}
	}
	
	private void processNewEdge(String conceptA, String role, String conceptB) throws Exception {
		
		String roleAxiomKey = KeyGenerator.generateAxiomKey(role);
		Set<String> superRoles = localQueueStore.smembers(roleAxiomKey);
		for(String s : superRoles) {	
			
			String resultRoleR1 = KeyGenerator.generateResultRoleCompoundKey1(s, conceptA);
			String resultRoleR2 = KeyGenerator.generateResultRoleCompoundKey2(s, conceptB);
			String resultRoleR3 = KeyGenerator.generateResultRoleConceptKey(conceptB);
			// R1(s#A) = B, R2(s#B) = A, R3(B) = s#A 
			localQueueStore.sadd(resultRoleR1, conceptB);
			localQueueStore.sadd(resultRoleR2, conceptA);
			localQueueStore.sadd(resultRoleR3, s + propertyFileHandler.getComplexAxiomSeparator() + conceptA);
			
			// insert into Q(A)
			final String QofA = KeyGenerator.generateQueueKey(conceptA);
			String superClassB = KeyGenerator.generateSuperclassKey(conceptB);
			Set<String> values = localQueueStore.smembers(superClassB);
			Pipeline p = localQueueStore.pipelined();
			for(String dashB : values) {
				String existentialKey = KeyGenerator.generateExistentialAxiomKey(s, dashB);
				// adding 3s.B' to this set. 				
//				p.sadd(QofA, s + propertyFileHandler.getExistentialAxiomSeparator() + dashB);
				p.sunionstore(QofA, QofA, existentialKey);
			}
			p.sync();
			
			// TODO: KeysToProcess -- check if 3s.B' is not empty and add A to KeysToProcess.
			//		 Do it only once here.
			// check M2(s) && R2(t#A) -- t o s < u
			String propertyChainKeyReverse = KeyGenerator.generatePropertyChainKey2(s);
			Set<String> propertyChainReverseValues = localQueueStore.smembers(propertyChainKeyReverse);
			for(String compoundValue : propertyChainReverseValues) {
				// get t and u
				String[] compoundValueSplit = compoundValue.split(propertyFileHandler.getComplexAxiomSeparator());
				if(compoundValueSplit.length != 2)
					throw new Exception("Expecting length 2: " + compoundValue);
				String compoundKey = KeyGenerator.generateResultRoleCompoundKey2(compoundValueSplit[0], conceptA);
				Set<String> compoundRoleValues = localQueueStore.smembers(compoundKey);
				for(String value : compoundRoleValues) {
					// check if (A',B) \in R(u)
					String resultSuperRole = KeyGenerator.generateResultRoleCompoundKey1(compoundValueSplit[1], value);
					if(!localQueueStore.sismember(resultSuperRole, conceptB))
						processNewEdge(value, compoundValueSplit[1], conceptB);
				}
			}
			
			// check M1(s) && R1(t#B) -- s o t < u
			String propertyChainKey = KeyGenerator.generatePropertyChainKey1(s);
			Set<String> propertyChainValues = localQueueStore.smembers(propertyChainKey);
			for(String compoundValue : propertyChainValues) {
				// get t and u
				String[] compoundValueSplit = compoundValue.split(propertyFileHandler.getComplexAxiomSeparator());
				if(compoundValueSplit.length != 2)
					throw new Exception("Expecting length 2: " + compoundValue);
				String compoundKey = KeyGenerator.generateResultRoleCompoundKey1(compoundValueSplit[0], conceptB);
				Set<String> compoundRoleValues = localQueueStore.smembers(compoundKey);
				for(String value : compoundRoleValues) {
					// check if (A,B') \in R(u)
					String resultSuperRole = KeyGenerator.generateResultRoleCompoundKey1(compoundValueSplit[1], conceptA);
					if(!localQueueStore.sismember(resultSuperRole, value))
						processNewEdge(conceptA, compoundValueSplit[1], value);
				}
			}
		}
	}
*/	
}
