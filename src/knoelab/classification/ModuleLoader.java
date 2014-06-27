package knoelab.classification;

import java.io.File;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import knoelab.classification.AxiomLoader.PropertyRelation;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
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
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.model.OWLSubObjectPropertyOfAxiom;
import org.semanticweb.owlapi.model.OWLSubPropertyChainOfAxiom;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import uk.ac.manchester.syntactic_locality.CreatePhysicalOntologyModule;

public class ModuleLoader {

/*	
	
	private List<JedisShardInfo> jedisShards;
	private PropertyFileHandler propertyFileHandler;
	private Set<OWLEntity> objPropertySignatures;
	
	public ModuleLoader() {
		propertyFileHandler = PropertyFileHandler.getInstance();
		jedisShards = new ArrayList<JedisShardInfo>();
		List<HostInfo> hostInfoList = propertyFileHandler.getAllShardsInfo();		
		for(HostInfo hostInfo : hostInfoList)
			jedisShards.add(new JedisShardInfo(hostInfo.host, hostInfo.port));
		objPropertySignatures = new HashSet<OWLEntity>();
	}
	
	public void loadModules(String ontoFile) throws Exception {
		OWLOntology ontology = normalizeOntology(ontoFile);
        CreatePhysicalOntologyModule physicalOntologyModule = new CreatePhysicalOntologyModule();
        final String moduleURI = "http://knoelab.wright.edu/module.owl";
        Set<OWLClass> ontClasses = ontology.getClassesInSignature();
        Set<String> signatureList = new HashSet<String>();
        Iterator<OWLClass> ontClassesIt = ontClasses.iterator();
        int index = 0;
        while(ontClassesIt.hasNext()) {
        	String cl = ontClassesIt.next().getIRI().getFragment();
        	signatureList.add(cl);
        	OWLOntology module = physicalOntologyModule.getExtractedModule(
        			ontology,signatureList, "UM", moduleURI);
        	
        	// check & reset index
        	if(index == jedisShards.size())
        		index = 0;
        	// load the module
        	loadModuleAxioms(module, jedisShards.get(index));
        	
        	// compute (TotalConcepts - ModuleSignatureConcepts)
        	// reset signature list
        	Set<OWLEntity> moduleSig = module.getSignature();
        	ontClasses.removeAll(moduleSig);
        	ontClassesIt = ontClasses.iterator();
        	System.out.println("No of classes remaining: " + ontClasses.size() + "  Module sig: " + moduleSig.size());
        	signatureList.clear();
        	index++;
        }
	}
	
	private void loadModuleAxioms(OWLOntology module, JedisShardInfo shard) throws Exception {
		Jedis localQueueStore = new Jedis(shard.getHost(), shard.getPort());
		
		// initializing S(X) = {X,T} and r<*s for each Object Property, r (reflexive transitive closure)
		String topConcept = module.getOWLOntologyManager().getOWLDataFactory().getOWLThing().toString();
        Set<OWLClass> conceptSet = module.getClassesInSignature();
        System.out.println("No of concepts: " + conceptSet.size());
        for(OWLClass concept : conceptSet) {
        	String conceptStr = concept.toString();
        	String axiomkey = KeyGenerator.generateSuperclassKey(conceptStr);
        	localQueueStore.sadd(axiomkey, concept.toString());
        	localQueueStore.sadd(axiomkey, topConcept);
        	// add it to the local queue of the shard. 
        	localQueueStore.sadd(propertyFileHandler.getLocalKeys(), conceptStr);
        }
        
        // sub object property axioms are taken care of here.
        computePropertyTransitiveClosure(module, localQueueStore);   
        Set<OWLObjectProperty> objectProperties = module.getObjectPropertiesInSignature();
        objectProperties.removeAll(objPropertySignatures);
        
        // For all other object properties which do not participate in any
        // sub-object properties, initialize P(r)={r}
        for(OWLObjectProperty p : objectProperties) {
        	String propStr = p.toString();
        	String propKey = KeyGenerator.generateAxiomKey(propStr);
        	localQueueStore.sadd(propKey, propStr);
        }
        
        // Load sub-class axioms.
        Set<OWLSubClassOfAxiom> subclassAxioms = module.getAxioms(AxiomType.SUBCLASS_OF);
        for(OWLSubClassOfAxiom axiom : subclassAxioms) 
        	insertSubClassAxioms(axiom, localQueueStore);
        // Load property chain axioms
        Set<OWLSubPropertyChainOfAxiom> subPropChainAxioms = module.getAxioms(AxiomType.SUB_PROPERTY_CHAIN_OF);
        for(OWLSubPropertyChainOfAxiom axiom : subPropChainAxioms) 
        	insertPropertyChainAxioms(axiom, localQueueStore);
        localQueueStore.disconnect();
	}
	
	private void computePropertyTransitiveClosure(OWLOntology normalizedOntology, 
			Jedis localStore) throws Exception {
	       
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
				String axiomKey = KeyGenerator.generateAxiomKey((String)node.getProperty("name"));
				for(Node n : traverser)
					localStore.sadd(axiomKey, (String)n.getProperty("name"));					
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
	
	private OWLOntology normalizeOntology(String ontoFile) throws Exception {
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        File owlFile = new File(ontoFile);
        IRI documentIRI = IRI.create(owlFile);
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
        // check whether normalization is required or not. 
        // Not exactly the right method -- quick & dirty way. Check based on file name
        if(ontoFile.contains("Norm")) 
        	System.out.println("Normalization is not required");
        else {
        	Normalizer normalizer = new Normalizer(manager, ontology);
        	OWLOntology normalizedOntology = normalizer.Normalize();
        	ontology = normalizedOntology;
        	System.out.println("Normalization complete");
        }
        return ontology;
	}
	
	private void insertSubClassAxioms(OWLSubClassOfAxiom axiom, Jedis localStore) throws Exception {
		// check whether it is A < B or A1 ^ A2 < B or 3r.A < B or A < 3r.B

		OWLSubClassOfAxiom subClassAxiom = (OWLSubClassOfAxiom) axiom;
		OWLClassExpression subClassExpression = subClassAxiom.getSubClass();

		if (subClassExpression instanceof OWLClass) {
			String axiomKey = KeyGenerator.generateAxiomKey(subClassExpression.toString());
			OWLClassExpression superClassExpression = subClassAxiom.getSuperClass();

			if (superClassExpression instanceof OWLObjectSomeValuesFrom) {
				OWLObjectSomeValuesFrom classExpression = (OWLObjectSomeValuesFrom) superClassExpression;
				String value = classExpression.getProperty()
						+ propertyFileHandler.getExistentialAxiomSeparator()
						+ classExpression.getFiller();
				localStore.sadd(axiomKey, value);
			} 
			else if (superClassExpression instanceof OWLClass)
				localStore.sadd(axiomKey, superClassExpression.toString());
			else
				throw new Exception("Unexpected SuperClass type of axiom. Axiom: "
								+ superClassExpression.toString());
		} 
		else if (subClassExpression instanceof OWLObjectIntersectionOf) {
			OWLClassExpression superClassExpression = subClassAxiom.getSuperClass();
			OWLObjectIntersectionOf intersectionAxiom = (OWLObjectIntersectionOf) subClassExpression;
			List<OWLClassExpression> operands = intersectionAxiom.getOperandsAsList();
			if (operands.size() == 2) {
				String axiomKey1 = KeyGenerator.generateAxiomKey((operands.get(0)).toString());
				String value1 = (operands.get(1)).toString()
						+ propertyFileHandler.getComplexAxiomSeparator()
						+ superClassExpression.toString();
				localStore.sadd(axiomKey1, value1);
				String axiomKey2 = KeyGenerator.generateAxiomKey((operands
						.get(1)).toString());
				String value2 = (operands.get(0)).toString()
						+ propertyFileHandler.getComplexAxiomSeparator()
						+ superClassExpression.toString();
				localStore.sadd(axiomKey2, value2);
			} 
			else
				throw new Exception("Expecting only 2 operands in "
						+ intersectionAxiom.toString());
		} 
		else if (subClassExpression instanceof OWLObjectSomeValuesFrom) {
			OWLObjectSomeValuesFrom classExpression = (OWLObjectSomeValuesFrom) subClassExpression;
			String axiomKey = KeyGenerator.generateExistentialAxiomKey(
					classExpression.getProperty().toString(), 
					classExpression.getFiller().toString());
			localStore.sadd(axiomKey, subClassAxiom.getSuperClass().toString());
		} 
		else
			throw new Exception("Unexpected SubClass type of axiom. Axiom: "
					+ subClassExpression.toString());
	}
	
	private void insertPropertyChainAxioms(OWLSubPropertyChainOfAxiom axiom, 
			Jedis localStore) throws Exception {
		OWLSubPropertyChainOfAxiom propertyChainAxiom = (OWLSubPropertyChainOfAxiom)axiom;
		List<OWLObjectPropertyExpression> operands = propertyChainAxiom.getPropertyChain();
		if(operands.size() == 2) {
			String axiomKey1 = KeyGenerator.generatePropertyChainKey1(operands.get(0).toString());
			String value1 = operands.get(1).toString() + 
							propertyFileHandler.getComplexAxiomSeparator() + 
							propertyChainAxiom.getSuperProperty().toString();
			String axiomKey2 = KeyGenerator.generatePropertyChainKey2(operands.get(1).toString());
			String value2 = operands.get(0).toString() + 
							propertyFileHandler.getComplexAxiomSeparator() + 
							propertyChainAxiom.getSuperProperty().toString();
			localStore.sadd(axiomKey1, value1);
			localStore.sadd(axiomKey2, value2);
		}
		else
			throw new Exception("Expecting only 2 operands in " + propertyChainAxiom.toString());
	}

	public static void main(String[] args) throws Exception {
		if(args.length !=1 || args[0].isEmpty()) {
			System.out.println("Please specify the Ontology file to load");
			System.exit(-1);
		}
		GregorianCalendar cal1 = new GregorianCalendar();
		ModuleLoader moduleLoader = new ModuleLoader();
		moduleLoader.loadModules(args[0]);
		GregorianCalendar cal2 = new GregorianCalendar();
		double diff = cal2.getTimeInMillis() - cal1.getTimeInMillis();
		System.out.println("Module Loading completed in " + (diff/1000) + " secs");
	}
*/
}
