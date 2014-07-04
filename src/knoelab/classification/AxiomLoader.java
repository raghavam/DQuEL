package knoelab.classification;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import jpaul.DataStructs.UnionFind;
import knoelab.classification.pipeline.PipelineManager;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLEquivalentClassesAxiom;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyChange;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom;
import org.semanticweb.owlapi.util.OWLEntityRenamer;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

/**
 * This class is used to load normalized axioms into sharded Redis.
 * @author Raghava
 *  
 */ 
public class AxiomLoader {

	private ShardedJedis shardedJedis;
	private PropertyFileHandler propertyFileHandler;
	private Set<OWLEntity> objPropertySignatures;
	private PipelineManager pipelineManager;
	private final int NUM_BYTES = 4;
	private String charset;
	private int conceptRoleCount = 1;
	
	public AxiomLoader() {
		propertyFileHandler = PropertyFileHandler.getInstance();
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		List<HostInfo> hostInfoList = propertyFileHandler.getAllShardsInfo();		
		for(HostInfo hostInfo : hostInfoList)
			shards.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));		
		shardedJedis = new ShardedJedis(shards);		
		objPropertySignatures = new HashSet<OWLEntity>();
		charset = propertyFileHandler.getCharset();
		pipelineManager = new PipelineManager(hostInfoList, propertyFileHandler.getPipelineQueueSize());
	}
	
	public void loadAxioms(String ontoFile) throws Exception {		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontoFile);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
        // check whether normalization is required or not. 
        // Not exactly the right method -- quick & dirty way. Check based on file name
        if(ontoFile.contains("Norm")) {
        	System.out.println("Normalization is not required");
        	mapConceptToID(ontology, new HashSet<OWLClass>());
        	// force sync now -- since there is a read operation in next step
        	pipelineManager.synchAll();
        	loadNormalizedAxioms(ontology);
        }
        else {
        	// replace B with A in all axioms if A==B
        	String equiKeys = propertyFileHandler.getEquivalentClassKeys();
        	Set<OWLClass> repEquiClasses = processEquivalentClassAxioms
        									(manager,ontology, equiKeys.getBytes(charset));
        	
        	Normalizer normalizer = new Normalizer(manager, ontology);
        	OWLOntology normalizedOntology = normalizer.Normalize();
        	// TODO: remove this after testing
        	// saving the normalized ontology to file, for testing
//        	RDFXMLOntologyFormat owlxmlFormat = new RDFXMLOntologyFormat();
//    	    File file = new File("Norm-" + owlFile.getName());
//    	    if(file.exists())
//    	    	file.delete();
//    	    manager.saveOntology(normalizedOntology, owlxmlFormat, IRI.create(file));        	
        	System.out.println("Normalization complete");
        	mapConceptToID(normalizedOntology, repEquiClasses);
        	// force sync now -- since there is a read operation in next step
        	pipelineManager.synchAll();
        	loadNormalizedAxioms(normalizedOntology);
        }
	}
	
	public void loadNormalizedAxioms(OWLOntology normalizedOntology) throws Exception {
        
        // initializing S(X) = {X,T} and r<*s for each Object Property, r (reflexive transitive closure)
		String topConcept = normalizedOntology.getOWLOntologyManager().getOWLDataFactory().getOWLThing().toString();
		byte[] topConceptID = conceptToID(topConcept);
        Set<OWLClass> conceptSet = normalizedOntology.getClassesInSignature();
        byte[] localKeys = propertyFileHandler.getLocalKeys().getBytes(charset);
        System.out.println("No of concepts: " + conceptSet.size());
        for(OWLClass concept : conceptSet) {
        	byte[] conceptID = conceptToID(concept.toString());
        	byte[] axiomkey = KeyGenerator.generateSuperclassKey(conceptID);
        	pipelineManager.psadd(conceptID, axiomkey, conceptID);
        	pipelineManager.psadd(conceptID, axiomkey, topConceptID);
        	// add it to the local queue of the shard. 
        	pipelineManager.psadd(conceptID, localKeys, conceptID);
        }
        
        // sub object property axioms are taken care of here.
        computePropertyTransitiveClosure(normalizedOntology);   
        Set<OWLObjectProperty> objectProperties = normalizedOntology.getObjectPropertiesInSignature();
        objectProperties.removeAll(objPropertySignatures);
        
        // For all other object properties which do not participate in any
        // sub-object properties, initialize P(r)={r}
        for(OWLObjectProperty p : objectProperties) {
        	byte[] propID = conceptToID(p.toString());
        	byte[] propKey = KeyGenerator.generateAxiomKey(propID);
        	pipelineManager.psadd(propKey, propID);
        }
        
        // Load sub-class axioms.
        Set<OWLSubClassOfAxiom> subclassAxioms = normalizedOntology.getAxioms(AxiomType.SUBCLASS_OF);
        for(OWLSubClassOfAxiom axiom : subclassAxioms) 
        	insertSubClassAxioms(axiom);
        // Load property chain axioms
        Set<OWLSubPropertyChainOfAxiom> subPropChainAxioms = normalizedOntology.getAxioms(AxiomType.SUB_PROPERTY_CHAIN_OF);
        for(OWLSubPropertyChainOfAxiom axiom : subPropChainAxioms) 
        	insertPropertyChainAxioms(axiom);   
        
        // close pipeline manager
        pipelineManager.synchAndCloseAll();
	}
	
	private void computePropertyTransitiveClosure(OWLOntology normalizedOntology) throws Exception {
		       
        File dirdb = new File("propdb");
        // delete the db if it already exists -- necessary because it keeps adding to the db.
        if(dirdb.exists())
        	deleteDir(dirdb);
        GraphDatabaseService propertyGraph = new EmbeddedGraphDatabase(dirdb.getName());
        try {
	        Transaction graphTransaction = propertyGraph.beginTx();
			buildPropertyGraph(propertyGraph, normalizedOntology);			
			Iterable<Node> allNodes = propertyGraph.getAllNodes();
			for(Node node : allNodes) {				
				// skip the default start/root node. Its ID is always 0
				if(node.getId() == 0)
					continue;
				Traverser traverser = node.traverse(
					    Order.BREADTH_FIRST,
					    StopEvaluator.END_OF_GRAPH,
					    ReturnableEvaluator.ALL,
					    PropertyRelation.SUBPROPERTY_OF,
					    Direction.OUTGOING );
				byte[] propID = conceptToID((String)node.getProperty("name"));
				byte[] axiomKey = KeyGenerator.generateAxiomKey(propID);
				for(Node n : traverser)
					pipelineManager.psadd(axiomKey, conceptToID((String)n.getProperty("name")));				
			}
			graphTransaction.success();
			graphTransaction.finish();
        }
        catch(Exception e) {
        	e.printStackTrace();
        }
        finally {
        	propertyGraph.shutdown();
        }
	}
	
	private void buildPropertyGraph(GraphDatabaseService propertyGraph, 
			OWLOntology normalizedOntology) throws Exception {
		Hashtable<String, Node> nameNodeMap = new Hashtable<String, Node>();
        Set<OWLSubObjectPropertyOfAxiom> norm = normalizedOntology.getAxioms(AxiomType.SUB_OBJECT_PROPERTY);
        for(OWLSubObjectPropertyOfAxiom e : norm) {
			String subprop = e.getSubProperty().toString();
			String superprop = e.getSuperProperty().toString();
			Node subPropertyNode, superPropertyNode;
			if(nameNodeMap.containsKey(subprop)) 
				subPropertyNode = nameNodeMap.get(subprop);
			else {
				subPropertyNode = propertyGraph.createNode();
				subPropertyNode.setProperty("name", subprop);
				nameNodeMap.put(subprop, subPropertyNode);
			}
			if(nameNodeMap.containsKey(superprop))
				superPropertyNode = nameNodeMap.get(superprop);
			else {
				superPropertyNode = propertyGraph.createNode();
				superPropertyNode.setProperty("name", superprop);
				nameNodeMap.put(superprop, superPropertyNode);
			}
			// edge check is not required because generally there won't be duplication of axioms
			// if(!edgeExists(subPropertyNode, superPropertyNode))
			subPropertyNode.createRelationshipTo(superPropertyNode, PropertyRelation.SUBPROPERTY_OF);
			objPropertySignatures.addAll(e.getSignature());
		}
		System.out.println("Property graph created");
	}
	
	private boolean deleteDir(File dir) {
	    if (dir.isDirectory()) {
	        String[] children = dir.list();
	        for (int i=0; i<children.length; i++) {
	            boolean success = deleteDir(new File(dir, children[i]));
	            if (!success) {
	                return false;
	            }
	        }
	    }
	    // The directory is now empty so delete it
	    return dir.delete();
	}
	
	private void insertSubClassAxioms(OWLSubClassOfAxiom axiom) throws Exception {
		// check whether it is A < B or A1 ^ A2 < B or 3r.A < B or A < 3r.B

		OWLSubClassOfAxiom subClassAxiom = (OWLSubClassOfAxiom) axiom;
		OWLClassExpression subClassExpression = subClassAxiom.getSubClass();

		if (subClassExpression instanceof OWLClass) {
			byte[] conceptID = conceptToID(subClassExpression.toString());
			byte[] axiomKey = KeyGenerator.generateAxiomKey(conceptID);
			byte[] queueKey = KeyGenerator.generateQueueKey(conceptID);
			OWLClassExpression superClassExpression = subClassAxiom.getSuperClass();

			if (superClassExpression instanceof OWLObjectSomeValuesFrom) {
				OWLObjectSomeValuesFrom classExpression = (OWLObjectSomeValuesFrom) superClassExpression;
				byte[] value = KeyGenerator.generateExistentialValue(
						conceptToID(classExpression.getProperty().toString()), 
						conceptToID(classExpression.getFiller().toString())
						);
				pipelineManager.psadd(conceptID, axiomKey, value);
				pipelineManager.psadd(conceptID, queueKey, value);
			} 
			else if (superClassExpression instanceof OWLClass) {
				byte[] superConceptID = conceptToID(superClassExpression.toString());
				pipelineManager.psadd(conceptID, axiomKey, superConceptID);
				pipelineManager.psadd(conceptID, queueKey, superConceptID);
			}
			else
				throw new Exception("Unexpected SuperClass type of axiom. Axiom: "
								+ superClassExpression.toString());
		} 
		else if (subClassExpression instanceof OWLObjectIntersectionOf) {
			// A1 ^ A2 ^ A3 ^ ......... ^ An -> B
			
			OWLClassExpression superClassExpression = subClassAxiom.getSuperClass();
			byte[] superConceptID = conceptToID(superClassExpression.toString());
			OWLObjectIntersectionOf intersectionAxiom = (OWLObjectIntersectionOf) subClassExpression;
			Set<OWLClassExpression> operands = intersectionAxiom.getOperands();
			Set<byte[]> operandIDs = new HashSet<byte[]>(operands.size());
			
			for(OWLClassExpression op : operands) 
				operandIDs.add(conceptToID(op.toString()));
			
			for(byte[] operandID : operandIDs) {
				Set<byte[]> naryConjuncts = new HashSet<byte[]>(operandIDs);
				naryConjuncts.remove(operandID);
				byte[] value = KeyGenerator.generateCompoundAxiomValue(
						naryConjuncts, superConceptID);
				byte[] axiomKey = KeyGenerator.generateAxiomKey(operandID);
				byte[] queueKey = KeyGenerator.generateQueueKey(operandID);
				pipelineManager.psadd(operandID, axiomKey, value);
				pipelineManager.psadd(operandID, queueKey, value);
			}
		} 
		else if (subClassExpression instanceof OWLObjectSomeValuesFrom) {
			OWLObjectSomeValuesFrom classExpression = (OWLObjectSomeValuesFrom) subClassExpression;
			byte[] propID = conceptToID(classExpression.getProperty().toString());
			byte[] conceptID = conceptToID(classExpression.getFiller().toString());
			byte[] axiomKey = KeyGenerator.generateExistentialAxiomKey(propID,conceptID);
			byte[] queueKey = KeyGenerator.generateExistentialQueueKey(propID,conceptID);
			// add 3r.A as the value to itself
//			shardedJedis.sadd(axiomKey, classExpression.getProperty().toString() + 
//										propertyFileHandler.getExistentialAxiomSeparator() + 
//										classExpression.getFiller().toString());
			byte[] valueID = conceptToID(subClassAxiom.getSuperClass().toString());
			pipelineManager.psadd(axiomKey, valueID);
			pipelineManager.psadd(queueKey, valueID);
		} 
		else
			throw new Exception("Unexpected SubClass type of axiom. Axiom: "
					+ subClassExpression.toString());
	}
	
	private void insertPropertyChainAxioms(OWLSubPropertyChainOfAxiom axiom) throws Exception {
		OWLSubPropertyChainOfAxiom propertyChainAxiom = (OWLSubPropertyChainOfAxiom)axiom;
		List<OWLObjectPropertyExpression> operands = propertyChainAxiom.getPropertyChain();
		if(operands.size() == 2) {
			byte[] propID1 = conceptToID(operands.get(0).toString());
			byte[] propID2 = conceptToID(operands.get(1).toString());
			byte[] superPropID = conceptToID(propertyChainAxiom.getSuperProperty().toString());
			byte[] axiomKey1 = KeyGenerator.generatePropertyChainKey1(propID1);
			byte[] value1 = ByteBuffer.allocate(2*NUM_BYTES).put(propID2).put(superPropID).array();

			byte[] axiomKey2 = KeyGenerator.generatePropertyChainKey2(propID2);
			byte[] value2 = ByteBuffer.allocate(2*NUM_BYTES).put(propID1).put(superPropID).array();

			pipelineManager.psadd(axiomKey1, value1);
			pipelineManager.psadd(axiomKey2, value2);
		}
		else
			throw new Exception("Expecting only 2 operands in " + propertyChainAxiom.toString());
	}
	
	private Set<OWLClass> processEquivalentClassAxioms(OWLOntologyManager manager, 
			OWLOntology ontology, byte[] equiKeys) throws Exception {
		
		Set<OWLClass> repEquiClasses = new HashSet<OWLClass>();
		Set<OWLEquivalentClassesAxiom> equivalentClassAxioms = ontology.getAxioms(AxiomType.EQUIVALENT_CLASSES);
		if(equivalentClassAxioms.isEmpty())
			return repEquiClasses;
		
		UnionFind<IRI> uf = new UnionFind<IRI>();		
		Set<OWLOntology> ontologySet = new HashSet<OWLOntology>();
		ontologySet.add(ontology);
		OWLEntityRenamer entityRenamer = new OWLEntityRenamer(manager, ontologySet);
				
		List<OWLOntologyChange> ontologyChanges = new ArrayList<OWLOntologyChange>();
		Set<OWLEquivalentClassesAxiom> simpleEquivalentClasses = new HashSet<OWLEquivalentClassesAxiom>();
		
		for(OWLEquivalentClassesAxiom ax : equivalentClassAxioms) {
			Set<OWLSubClassOfAxiom> pairs = ax.asOWLSubClassOfAxioms();
			OWLSubClassOfAxiom axiom = pairs.iterator().next();	
			if((axiom.getSubClass() instanceof OWLClass) && (axiom.getSuperClass() instanceof OWLClass)) {
				IRI repLHS = uf.find(((OWLClass)axiom.getSubClass()).getIRI());
				IRI repRHS = uf.find(((OWLClass)axiom.getSuperClass()).getIRI());
				uf.union(repLHS, repRHS);
				simpleEquivalentClasses.add(ax);
			}
		}
		
		if(simpleEquivalentClasses.isEmpty())
			return repEquiClasses;
		
		manager.removeAxioms(ontology, simpleEquivalentClasses);
		
		OWLDataFactory dataFactory = manager.getOWLDataFactory();
		Collection<Set<IRI>> equiClasses = uf.allNonTrivialEquivalenceClasses();
		for(Set<IRI> equiClassSet : equiClasses) {
			boolean isRepSet = false;
			IRI repEquiClass = null;
			byte[] repEquiClassID = null;
			for(IRI equiClass : equiClassSet) {
				// pick the first element as the representative of this equivalent class set
				if(!isRepSet) {
					repEquiClass = equiClass;
					isRepSet = true;
					repEquiClasses.add(dataFactory.getOWLClass(repEquiClass));
					// add it to Redis
					byte[] repClass = repEquiClass.toQuotedString().getBytes(charset);
					repEquiClassID = ByteBuffer.allocate(NUM_BYTES).putInt(conceptRoleCount).array();
					pipelineManager.pset(repClass, repEquiClassID);
					pipelineManager.pset(repEquiClassID, repClass);
					pipelineManager.psadd(equiKeys, repEquiClassID);
					conceptRoleCount++;
					continue;
				}
				byte[] equiClassID = ByteBuffer.allocate(NUM_BYTES).putInt(conceptRoleCount).array();
				byte[] equiClassByte = equiClass.toQuotedString().getBytes(charset);
				pipelineManager.pset(equiClassByte, equiClassID);
				pipelineManager.pset(equiClassID, equiClassByte);
				pipelineManager.psadd(KeyGenerator.generateEquivalentClassAxiomKey(repEquiClassID),
						equiClassID);
				conceptRoleCount++;
				ontologyChanges.addAll(entityRenamer.changeIRI(equiClass, repEquiClass));
			}
		}		
		manager.applyChanges(ontologyChanges);
		return repEquiClasses;
	}
	
	private void mapConceptToID(OWLOntology ontology, Set<OWLClass> repEquiClasses) throws Exception {
		Set<OWLClass> ontologyClasses = ontology.getClassesInSignature(); 
		Set<OWLObjectProperty> ontologyProperties = ontology.getObjectPropertiesInSignature();
		byte[] topConcept = ontology.getOWLOntologyManager().
							getOWLDataFactory().getOWLThing().
							toString().getBytes(Charset.forName(charset));
		
		// remove representative equivalence classes as they are already in the DB
		ontologyClasses.removeAll(repEquiClasses);
		for(OWLClass cl : ontologyClasses) {
			byte[] concept = cl.toString().getBytes(Charset.forName(charset));
			// allocating 4 bytes, since it is an int
			byte[] conceptID = ByteBuffer.allocate(NUM_BYTES).putInt(conceptRoleCount).array();
			pipelineManager.pset(concept, conceptID);
			pipelineManager.pset(conceptID, concept);
			conceptRoleCount++;
		}
		// add top concept
		byte[] id = ByteBuffer.allocate(NUM_BYTES).putInt(conceptRoleCount).array();
		pipelineManager.pset(topConcept, id);
		pipelineManager.pset(id, topConcept);
		conceptRoleCount++;
		// add roles
		for(OWLObjectProperty property : ontologyProperties) {
			byte[] role = property.toString().getBytes(Charset.forName(charset));
			byte[] roleID = ByteBuffer.allocate(NUM_BYTES).putInt(conceptRoleCount).array();
			pipelineManager.pset(role, roleID);
			pipelineManager.pset(roleID, role);
			conceptRoleCount++;
		}
	}
	
	private byte[] conceptToID(String concept) throws Exception {
		byte[] conceptKey = concept.getBytes(charset);
		byte[] conceptID = shardedJedis.get(conceptKey);
		if(conceptID == null)
			throw new Exception("Cannot find the concept in DB: " + concept);
		return conceptID;
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1 || args[0].isEmpty()) {
			System.out.println("Give the path of owl file as an argument.");
    		System.exit(-1);
		}
		AxiomLoader axiomLoader = new AxiomLoader();
		GregorianCalendar cal1 = new GregorianCalendar();
		axiomLoader.loadAxioms(args[0]);
		GregorianCalendar cal2 = new GregorianCalendar();
		double diff = cal2.getTimeInMillis() - cal1.getTimeInMillis();
		System.out.println("Loading completed in " + (diff/1000) + " secs");
	}
	
	enum PropertyRelation implements RelationshipType { SUBPROPERTY_OF }

}
