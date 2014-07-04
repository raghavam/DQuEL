package knoelab.classification;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * This class handles all the read requests for
 * the ShardInfo.properties file. 
 * 
 * @author Raghava
 */
public class PropertyFileHandler {
	private final static PropertyFileHandler propertyFileHandler = new PropertyFileHandler();
	private Properties shardInfoProperties = null;
	private final String PROPERTY_FILE = "resources/ShardInfo.properties";
	
	private PropertyFileHandler() {
		// does not allow instantiation of this class
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(PROPERTY_FILE));
			shardInfoProperties = new Properties();
			shardInfoProperties.load(reader);
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static PropertyFileHandler getInstance() {
		return propertyFileHandler;
	}
	
	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException("Cannot clone an instance of this class");
	}
	
	public HostInfo getLocalHostInfo() {
		String[] hostPort = shardInfoProperties.getProperty("shard.local").split(":");
		HostInfo localhostInfo = new HostInfo(hostPort[0], 
				Integer.parseInt(hostPort[1]));
		return localhostInfo;
	}
	
	public List<HostInfo> getAllShardsInfo() {
		List<HostInfo> allNodes = new ArrayList<HostInfo>();
		String csvHosts = shardInfoProperties.getProperty("NODES_LIST");
		String[] hosts = csvHosts.split(",");
		for(String hostPort : hosts) {
			String[] hpSplit = hostPort.split(":");
			allNodes.add(new HostInfo(hpSplit[0], 
					Integer.parseInt(hpSplit[1])));
		}
		return allNodes;
	}
	
	public String getAxiomSymbol() {
		return shardInfoProperties.getProperty("axiom.symbol");
	}
	
	public String getAxiomPropertyChainForwardSymbol() {
		return shardInfoProperties.getProperty("axiom.propertychain.forward.symbol");
	}
	
	public String getAxiomPropertyChainReverseSymbol() {
		return shardInfoProperties.getProperty("axiom.propertychain.reverse.symbol");
	}
	
	public String getResultRoleConceptSymbol() {
		return shardInfoProperties.getProperty("result.role.concept.symbol");
	}
	
	public String getResultRoleCompoundKey1Symbol() {
		return shardInfoProperties.getProperty("result.role.compound.key1.symbol");
	}
	
	public String getResultRoleCompoundKey2Symbol() {
		return shardInfoProperties.getProperty("result.role.compound.key2.symbol");
	}
	
	public String getQueueSymbol() {
		return shardInfoProperties.getProperty("queue.symbol");
	}
	
	public String getSuperclassSymbol() {
		return shardInfoProperties.getProperty("result.superclass.symbol");
	}
	
	public String getComplexAxiomSeparator() {
		return shardInfoProperties.getProperty("complex.axiom.separator");
	}
	
	public String getExistentialAxiomSeparator() {
		return shardInfoProperties.getProperty("existential.axiom.separator");
	}
	
	public String getLocalKeys() {
		return shardInfoProperties.getProperty("kvstore.localkeys");
	}
	
	public String getDatePattern() {
		return shardInfoProperties.getProperty("datepattern");
	}
	
	public String getJobControllerChannel() {
		return shardInfoProperties.getProperty("jobcontroller.channel");
	}
	
	public String getTerminationControllerChannel() {
		return shardInfoProperties.getProperty("terminationcontroller.channel");
	}
	
	public String getTimestampKey() {
		return shardInfoProperties.getProperty("timestamp");
	}
	
	public HostInfo getTerminationControllerLocation() {
		String[] hostPort = shardInfoProperties.getProperty("tc.location").split(":");
		HostInfo tcHostInfo = new HostInfo(hostPort[0], 
				Integer.parseInt(hostPort[1]));
		return tcHostInfo;
	}
	
	public int getShardCount() {
		String csvHosts = shardInfoProperties.getProperty("NODES_LIST");
		String[] hosts = csvHosts.split(",");
		return hosts.length;
	}
	
	public String getEquivalentClassKeys() {
		return shardInfoProperties.getProperty("equiclass.keys");
	}
	
	public String getAxiomEquivalentClassSymbol() {
		return shardInfoProperties.getProperty("axiom.equivalentclass.symbol");
	}
	
	public String getCharset() {
		return shardInfoProperties.getProperty("charset");
	}
	
	public int getPipelineQueueSize() {
		String maxSize = shardInfoProperties.getProperty("pipeline.queue.size");
		return Integer.parseInt(maxSize);
	}
}


