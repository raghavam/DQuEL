package knoelab.classification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;

/**
 * This class is used to delete all the keys from all the shards or localhost.
 * 
 * @author Raghava
 *
 */
public class DeleteKeys {

	private List<JedisShardInfo> shards;
	
	public DeleteKeys() {
		shards = new ArrayList<JedisShardInfo>();
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		List<HostInfo> hostInfoList = propertyFileHandler.getAllShardsInfo();
		for(HostInfo hostInfo : hostInfoList)
			shards.add(new JedisShardInfo(hostInfo.getHost(), 
					hostInfo.getPort(), Constants.INFINITE_TIMEOUT));	
	}
	
	public void deleteAll() {
		boolean anotherAttempt = true; 
		for(JedisShardInfo shard : shards) {
			Jedis jedis = new Jedis(shard.getHost(), shard.getPort(), 
					Constants.INFINITE_TIMEOUT);
			while(anotherAttempt) {
				try {
					jedis.flushAll();
					anotherAttempt = false;
				}
				catch(Exception e) {
					anotherAttempt = true;
					System.out.println(e.getMessage());
					System.out.println(shard.getHost() + ":" + shard.getPort() + 
							" Exception - trying again");
				}
			}
			jedis.disconnect();
			anotherAttempt = true;
		}
		System.out.println("All Keys in " + shards.size() + " shards are deleted");
	}
	
	// supposed to delete all the entries which were computed after loading axioms.
	// But is not possible currently because S(X) gets initialized while loading axioms.
	public void deleteComputedEntries() {
		throw new UnsupportedOperationException("not implemented");
	}
	
	public static void main(String[] args) throws IOException {
		DeleteKeys deleteKeys = new DeleteKeys();
		deleteKeys.deleteAll();
	}
}
