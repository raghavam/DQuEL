package knoelab.classification.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import knoelab.classification.Constants;
import knoelab.classification.HostInfo;
import knoelab.classification.KeyGenerator;
import knoelab.classification.PropertyFileHandler;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.InferenceType;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

import com.clarkparsia.pellet.owlapiv3.PelletReasoner;
import com.clarkparsia.pellet.owlapiv3.PelletReasonerFactory;

import de.tudresden.inf.lat.jcel.owlapi.main.JcelReasoner;

/**
 * This class compares the classification output of Pellet (for a
 * few Ontologies) with ELClassifier which uses Redis + queue approach.
 *  
 * @author Raghava
 */
public class ELClassifierTest {

	public void compareClassificationResults(String[] args) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		
		System.out.println("Comparing classification output for " + args[0]);
		File ontFile = new File(args[0]);
		IRI documentIRI = IRI.create(ontFile);
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
	    System.out.println("Not Normalizing");
		
	    PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
	    List<HostInfo> hostInfoList = propertyFileHandler.getAllShardsInfo();
		for(HostInfo hostInfo : hostInfoList)
			shards.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));
	    
		ShardedJedis shardedJedis = new ShardedJedis(shards);
		checkClassificationPellet(ontology, shardedJedis);
//		checkClassificationHermit(normalizedOntology, classifier);
//		checkClassificationCEL(ontology, classifier);
//		comparePelletCEL(normalizedOntology);
	
		manager.removeOntology(ontology);
		shardedJedis.disconnect();
	}

	private void checkClassificationPellet(OWLOntology normalizedOntology, 
			ShardedJedis shardedJedis) throws Exception {
		
		System.out.println("\nVerifying using Pellet\n");
		PelletReasoner pelletReasoner = PelletReasonerFactory.getInstance().
				createReasoner( normalizedOntology );
		Set<OWLClass> classes = normalizedOntology.getClassesInSignature();
		int ne = 0;
		for (OWLClass cl : classes) {
			Set<OWLClass> reasonerSuperclasses = pelletReasoner.
					getSuperClasses(cl, false).getFlattened();
			// add cl itself to S(X) computed by reasoner. That is missing
			// in its result.
			reasonerSuperclasses.add(cl);
			// adding equivalent classes -- they are not considered if asked for superclasses
			Iterator<OWLClass> iterator = pelletReasoner.getEquivalentClasses(cl).iterator();
			while(iterator.hasNext())
				reasonerSuperclasses.add(iterator.next());
			byte[] clID = conceptToID(cl.toString(), shardedJedis);
			byte[] clKey = KeyGenerator.generateSuperclassKey(clID);
			Set<byte[]> classifiedSuperClasses = shardedJedis.getShard(clID).smembers(clKey);				
			// above 2 sets should be equal
			if(reasonerSuperclasses.size() != classifiedSuperClasses.size()) { 
				ne++;
				System.out.println("\nConcept: " + cl.toString());
//				assert false :
//				"For concept " + cl.toString() + " calculated S(X) sizes are not equal";
				int diff = reasonerSuperclasses.size() - classifiedSuperClasses.size();
				System.out.println("Pellet S(X): " + reasonerSuperclasses.size() + 
						"\tMy S(X): " + classifiedSuperClasses.size() + 
						"\tdiff: " + diff);
				// extra: <http://www.co-ode.org/ontologies/galen#Heme>
				print1(reasonerSuperclasses);
				print2(classifiedSuperClasses, shardedJedis);
			}
/*				for (OWLClass scl : reasonerSuperclasses) {
				assert classifiedSuperClasses.contains(scl.toString()) : "Mismatch in 2 sets - "
						+ scl.toString() + " not found";
				classifiedSuperClasses.remove(scl.toString());
			}
			assert classifiedSuperClasses.isEmpty() : "This set should be empty, not of size "
					+ classifiedSuperClasses.size();
*/																		
		}
		pelletReasoner.dispose();
		if(ne == 0)
			System.out.println("\nTests pass");
		else
			System.out.println("\nNo of tests failed: " + ne);
	}
/*	
	private void checkClassificationHermit(OWLOntology normalizedOntology, ELClassifier classifier) {
		
		System.out.println("\nUsing Hermit\n");
		Reasoner hermitReasoner = new Reasoner(normalizedOntology);
		Set<OWLClass> classes = normalizedOntology.getClassesInSignature();
		int ne = 0;
		for(OWLClass cl : classes) {
			Set<OWLClass> reasonerSuperclasses = hermitReasoner.getSuperClasses(cl, false).getFlattened();
			// add cl itself to S(X) computed by reasoner. That is missing
			// in its result.
			reasonerSuperclasses.add(cl);
			Set<String> classifiedSuperClasses = classifier.getSuperClassSet(cl.toString());				
			// above 2 sets should be equal
			if(reasonerSuperclasses.size() != classifiedSuperClasses.size()) { 
				ne++;
				System.out.println("\nConcept: " + cl.toString());
//				print1(reasonerSuperclasses);
//				print2(classifiedSuperClasses);
//				assert false :
//				"For concept " + cl.toString() + " calculated S(X) sizes are not equal";
				System.out.println("Hermit S(X): " + reasonerSuperclasses.size() + 
						"\tMy S(X): " + classifiedSuperClasses.size() + 
						"\tdiff: " + (reasonerSuperclasses.size() - classifiedSuperClasses.size()));
			}
				for (OWLClass scl : reasonerSuperclasses) {
				assert classifiedSuperClasses.contains(scl.toString()) : "Mismatch in 2 sets - "
						+ scl.toString() + " not found";
				classifiedSuperClasses.remove(scl.toString());
			}
			assert classifiedSuperClasses.isEmpty() : "This set should be empty, not of size "
					+ classifiedSuperClasses.size();
																		
		}
		hermitReasoner.dispose();
		if(ne == 0)
			System.out.println("\nTests pass");
		else
			System.out.println("\nNo of tests failed: " + ne);
	}
*/
	
	private void checkClassificationCEL(OWLOntology normalizedOntology, 
			ShardedJedis shardedJedis) throws Exception {
		
		System.out.println("\nUsing CEL\n");
		JcelReasoner jcelReasoner = new JcelReasoner(
				normalizedOntology, false);
	    jcelReasoner.precomputeInferences(
	    		InferenceType.CLASS_HIERARCHY);
		Set<OWLClass> classes = normalizedOntology.getClassesInSignature();
		int ne = 0;
		for(OWLClass cl : classes) {
			Set<OWLClass> reasonerSuperclasses = jcelReasoner.getSuperClasses(
					cl, false).getFlattened();
			// add cl itself to S(X) computed by reasoner. That is missing
			// in its result.
			reasonerSuperclasses.add(cl);
			// adding equivalent classes -- they are not considered if asked for superclasses
			Iterator<OWLClass> iterator = jcelReasoner.getEquivalentClasses(
					cl).iterator();
			while(iterator.hasNext())
				reasonerSuperclasses.add(iterator.next());
			byte[] clKey = KeyGenerator.generateSuperclassKey(conceptToID(
					cl.toString(), shardedJedis));
			Set<byte[]> classifiedSuperClasses = shardedJedis.smembers(clKey);				
			// above 2 sets should be equal
			if(reasonerSuperclasses.size() != classifiedSuperClasses.size()) { 
				ne++;
				System.out.println("\nConcept: " + cl.toString());
//				assert false :
//				"For concept " + cl.toString() + " calculated S(X) sizes are not equal";
				int diff = reasonerSuperclasses.size() - classifiedSuperClasses.size();
				System.out.println("CEL S(X): " + reasonerSuperclasses.size() + 
						"\tMy S(X): " + classifiedSuperClasses.size() + 
						"\tdiff: " + diff);
//				print1(reasonerSuperclasses);
//				print2(classifiedSuperClasses);
			}																	
		}
		jcelReasoner.dispose();
		if(ne == 0)
			System.out.println("\nTests pass");
		else
			System.out.println("\nNo of tests failed: " + ne);
	}
	
	private void comparePelletCEL(OWLOntology normalizedOntology) {
		
		System.out.println("\nComparing Pellet with CEL\n");
		PelletReasoner pelletReasoner = PelletReasonerFactory.getInstance().createReasoner( normalizedOntology );
		JcelReasoner celReasoner = new JcelReasoner(normalizedOntology, false);
        celReasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
        celReasoner.flush();
		Set<OWLClass> classes = normalizedOntology.getClassesInSignature();
		int ne = 0;
		for(OWLClass cl : classes) {
			Set<OWLClass> celSuperclasses = celReasoner.getSuperClasses(cl, false).getFlattened();
			Set<OWLClass> pelletSuperclasses = pelletReasoner.getSuperClasses(cl, false).getFlattened();
			// add cl itself to S(X) computed by reasoner. That is missing
			// in its result.
			celSuperclasses.add(cl);
			pelletSuperclasses.add(cl);
			// adding equivalent classes -- they are not considered if asked for superclasses
			Iterator<OWLClass> iterator1 = celReasoner.getEquivalentClasses(cl).iterator();
			while(iterator1.hasNext())
				celSuperclasses.add(iterator1.next());
			Iterator<OWLClass> iterator2 = pelletReasoner.getEquivalentClasses(cl).iterator();
			while(iterator2.hasNext())
				pelletSuperclasses.add(iterator2.next());
			
			System.out.println("\nConcept: " + cl.toString());
			if(celSuperclasses.equals(pelletSuperclasses))
				System.out.println("Both are equal");
			else {
				ne++;
				System.out.println("Pellet: " + pelletSuperclasses.size() + "  CEL: " + celSuperclasses.size() + "  diff:"
						+ (pelletSuperclasses.size() - celSuperclasses.size()));
			}																	
		}
		celReasoner.dispose();
		pelletReasoner.dispose();
		
		if(ne == 0)
			System.out.println("\nTests pass");
		else
			System.out.println("\nNo of tests failed: " + ne);
	}
	
	private byte[] conceptToID(String concept, ShardedJedis shardedJedis) throws Exception {
		byte[] conceptKey = concept.getBytes("UTF-8");
		return shardedJedis.get(conceptKey);
	}
	
	private void print1(Set<OWLClass> reasonerSuperClasses) {
		System.out.println("\nReasoner computed S(X)\n");
		for(OWLClass cl : reasonerSuperClasses)
			System.out.println(cl.toString());
	}
	
	private void print2(Set<byte[]> classifiedSuperClasses, 
			ShardedJedis shardedJedis) throws Exception {
		System.out.println("\nS(X) computed by me\n");
		for(byte[] cl : classifiedSuperClasses) {
			byte[] concept = shardedJedis.get(cl);
			System.out.println(new String(concept, "UTF-8"));
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1 || args[0].isEmpty()) {
			System.out.println("Give the path of owl file");
    		System.exit(-1);
		}
		new ELClassifierTest().compareClassificationResults(args);
	}

}
