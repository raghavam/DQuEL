package knoelab.classification.controller;

import java.net.InetAddress;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import knoelab.classification.Classifier;
import knoelab.classification.Constants;
import knoelab.classification.ELClassifier;
import knoelab.classification.HostInfo;
import knoelab.classification.KeyGenerator;
import knoelab.classification.MessageType;
import knoelab.classification.MultiThreadedClassifier;
import knoelab.classification.PropertyFileHandler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * Runs on each node of the cluster. It has the
 * following purpose:
 * 		1) Start the ELClassifier
 * 		2) Keep track of the classification time
 * 		3) Communicate with TerminationController using pub/sub feature
 * 
 * @author Raghava
 *
 */
public class JobController {
	
	private Classifier stclassifier;
	private Classifier classifier;
	private long totalTime;
	// identifier for this machine
	private String machineIPAddr;
	private Jedis terminationController;
	private PropertyFileHandler propertyFileHandler;
	private String messageSeparator;
	private String tcChannel;
	private String charset;
	private boolean mtmode;
	private final int THRESHOLD = 100;
	
	public JobController(boolean mode, int multiplier) throws Exception {
		stclassifier = new ELClassifier();
		mtmode = mode;
		if(mode) {
			// multi-threaded classifier
			classifier = new MultiThreadedClassifier(multiplier);
		}
		else {
			// single-threaded classifier
			classifier = stclassifier;
		}
		totalTime = 0;
		machineIPAddr = InetAddress.getLocalHost().getHostAddress();
		propertyFileHandler  = PropertyFileHandler.getInstance();
		messageSeparator = propertyFileHandler.getExistentialAxiomSeparator();
		tcChannel = propertyFileHandler.getTerminationControllerChannel();
		HostInfo tcLocation = propertyFileHandler.getTerminationControllerLocation();
		terminationController = new Jedis(tcLocation.getHost(), 
				tcLocation.getPort(), Constants.INFINITE_TIMEOUT);
		charset = propertyFileHandler.getCharset();
	}
	
	public void start() throws Exception {
		long startTime = System.nanoTime();
		classifier.classify();
		long endTime = System.nanoTime();
		totalTime += endTime - startTime;
		
		// send "DONE" message to TC
		// MessageFormat: code + machine IP
		sendMessageToTC(MessageType.DONE);
	}
	
	public void start(Set<byte[]> conceptsToProcess) throws Exception {
		GregorianCalendar startTime = new GregorianCalendar();
		int size = conceptsToProcess.size();
		if(size >= THRESHOLD) {
			System.out.println("Rem size is " + size + " more than threshold " + THRESHOLD);
			classifier.classify(conceptsToProcess);
		}
		else {
			System.out.println("Rem size is " + size + " less than threshold " + THRESHOLD);
			stclassifier.classify(conceptsToProcess);
		}
		GregorianCalendar endTime = new GregorianCalendar();
		totalTime += endTime.getTimeInMillis() - startTime.getTimeInMillis();
		
		// send "DONE" message to TC
		// MessageFormat: code + machine IP
		sendMessageToTC(MessageType.DONE);
	}
	
	private void sendMessageToTC(MessageType messageType) {
		StringBuilder messageToSend;
		
		if(messageType == MessageType.DONE) {
			// append total runtime to the message
			messageToSend = new StringBuilder(messageType.getMessageTypeCode()).
									append(messageSeparator).append(machineIPAddr).
									append(messageSeparator).append(totalTime);
		}
		else
			messageToSend = new StringBuilder(messageType.getMessageTypeCode()).
			append(messageSeparator).append(machineIPAddr);
		
		terminationController.publish(tcChannel, messageToSend.toString());
	}
	
	public void checkAndRestart() throws Exception {
		// check whether all queues are empty
		HostInfo localHostInfo = propertyFileHandler.getLocalHostInfo();
		Jedis localQueues = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		byte[] localKeys = propertyFileHandler.getLocalKeys().getBytes(charset);
		Set<byte[]> conceptsToCheck = localQueues.smembers(localKeys);
		boolean restartRequired = false;
		Set<byte[]> conceptsToProcess = new HashSet<byte[]>();
		for(byte[] concept : conceptsToCheck) {
			byte[] QofX = KeyGenerator.generateQueueKey(concept);
			if (localQueues.scard(QofX) != 0) { 
				restartRequired = true;
				conceptsToProcess.add(concept);
			}
		}
		localQueues.disconnect();
		
		if(restartRequired) {
			// Send "NOT_DONE" message to TC. TC should reset
			sendMessageToTC(MessageType.NOT_DONE);
			// restart the stclassifier
			start(conceptsToProcess);
		}
		else {
			// send "DONE" message to TC
			sendMessageToTC(MessageType.DONE);
		}
	}
	
	public void releaseResources() {
		terminationController.disconnect();
		if(mtmode)
			classifier.releaseResources();
		stclassifier.releaseResources();
	}
	
	/**
	 * returns the total time (in secs) taken for 
	 * classification on this node 
	 * @return
	 */
	public double getTotalTime() {
		return totalTime/1000;
	}
	

	public static void main(String[] args) throws Exception {
		JobController jobController;
		if(args.length == 1) 
			jobController = new JobController(true, Integer.parseInt(args[0]));
		else
			jobController = new JobController(false, -1);
		
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		HostInfo localHostInfo = propertyFileHandler.getLocalHostInfo();
		
		// create a thread for pub/sub with TerminationController
		Thread messageHandlerThread = new Thread(new JCMessageHandler(jobController, 
				localHostInfo, propertyFileHandler.getJobControllerChannel()));
		messageHandlerThread.start();
		
		// start the stclassifier
		jobController.start();
		
		// wait for the thread to die
		messageHandlerThread.join();
		jobController.releaseResources();
		System.out.println("Exiting JobController");
	}

}

/**
 * This class handles the messages sent by TerminationController(TC)
 * @author Raghava
 *
 */
class JCMessageHandler implements Runnable {
	private JobController jobController;
	private HostInfo localHostInfo;
	private String channel;
	
	JCMessageHandler(JobController jc, HostInfo hostInfo, String channel) {
		jobController = jc;
		localHostInfo = hostInfo;
		this.channel = channel;
	}
	
	public void run() {
		Jedis jedisPubSubListener = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		jedisPubSubListener.subscribe(new PubSubTC(jobController), channel);
	}
}

class PubSubTC extends JedisPubSub {

	private JobController jobController;
	
	PubSubTC(JobController jc) {
		jobController = jc;
	}
	
	@Override
	public void onMessage(String channel, String message) {
		try {
			MessageType messageType = MessageType.convertCodeToMessageType(message);
			System.out.println("Msg received: " + messageType);
			if(messageType == MessageType.CHECK_AND_RESTART)
				jobController.checkAndRestart();
			else if(messageType == MessageType.TERMINATE) {
				// unsubscribe from the channel
				unsubscribe();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onPMessage(String pattern, String channel, String message) { }
	@Override
	public void onPSubscribe(String pattern, int subscribedChannels) { }
	@Override
	public void onPUnsubscribe(String pattern, int subscribedChannels) { }
	@Override
	public void onSubscribe(String channel, int subscribedChannels) { }
	@Override
	public void onUnsubscribe(String channel, int subscribedChannels) { }
	
}
