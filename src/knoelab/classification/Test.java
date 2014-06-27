package knoelab.classification;

import java.io.File;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import jpaul.DataStructs.UnionFind;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLEquivalentClassesAxiom;
import org.semanticweb.owlapi.model.OWLLogicalAxiom;
import org.semanticweb.owlapi.model.OWLObjectIntersectionOf;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyChange;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.util.OWLEntityRenamer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.util.Hashing;




public class Test {
	
	public static void main(String[] args) throws Exception {
		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		File ontFile = new File(args[0]);
		IRI documentIRI = IRI.create(ontFile);
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(documentIRI);
		System.out.println("No of classes: " + ontology.getClassesInSignature().size());
		System.out.println("No of logical axioms: " + ontology.getLogicalAxiomCount());
	}
}


/*
class TimeStampRecorder implements Runnable {
	private CommandMonitor commandMonitor;
	private HostInfo localHostInfo;
	private CountDownLatch countDownLatch;
	
	TimeStampRecorder(CommandMonitor cm, HostInfo hostInfo, CountDownLatch cdl) {
		commandMonitor = cm;
		localHostInfo = hostInfo;
		countDownLatch = cdl;
	}
	
	public void run() {
		Jedis jedisMonitor = new Jedis(localHostInfo.host, localHostInfo.port);
		jedisMonitor.monitor(commandMonitor);
		countDownLatch.countDown();
	}
}

class CommandMonitor extends JedisMonitor {
	
	private SimpleDateFormat dateFormat;
	private PropertyFileHandler propertyFileHandler;
	private String timestampKey;
	
	public CommandMonitor() {
		propertyFileHandler = PropertyFileHandler.getInstance();
		dateFormat = new SimpleDateFormat(propertyFileHandler.getDatePattern());
		timestampKey = propertyFileHandler.getTimestampKey();
	}
	
	public void onCommand(String command) {
		// check for insertion command
		if(command.contains("SADD")) {
			// set the timestamp
			client.set(timestampKey, dateFormat.format(new Date()));
		}
	}
	
	public void stopCommandMonitor() {
		client.disconnect();
	}
}
*/