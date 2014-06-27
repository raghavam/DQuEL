package knoelab.classification;

import java.util.Set;

/**
 * An interface to all the implementations of Classifiers.
 * Currently there are two types of implementations - Single threaded & Multi threaded
 * @author Raghava
 *
 */
public interface Classifier {
	
	/**
	 * Works on all the classes present in the local node
	 * and classifies them
	 */
	public void classify() throws Exception;
	
	/**
	 * Similar to classify() except that it only works on 
	 * the concepts given as argument. This set is a subset of 
	 * the classes present in the local node
	 * @param conceptsToProcess
	 */
	public void classify(final Set<byte[]> conceptsToProcess) throws Exception;
	
	/**
	 * Releases all the resources (jedis, threads etc). Since the client
	 * to the interface implementations maintains a single connection from
	 * start to end, the resources are released only at the end.
	 */
	public void releaseResources();
	
}
