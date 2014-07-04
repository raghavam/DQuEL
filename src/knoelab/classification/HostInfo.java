package knoelab.classification;

public class HostInfo {
	private String host;
	private int port;
	
	public HostInfo(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	public String getHost() {
		return host;
	}
	
	public int getPort() {
		return port;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	@Override
	public String toString() {
		return host + ":" + port;
	}
}
