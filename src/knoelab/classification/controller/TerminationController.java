package knoelab.classification.controller;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import knoelab.classification.Constants;
import knoelab.classification.HostInfo;
import knoelab.classification.MessageType;
import knoelab.classification.PropertyFileHandler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * This class(TC) keeps track of the messages sent
 * by JobControllers on each machine and decides
 * the termination of JobControllers which in turn
 * terminate the Classification process.
 * 
 * @author Raghava
 *
 */
public class TerminationController extends JedisPubSub {
	
	private LinkedBlockingQueue<MachineAddrTime> doneMsgQueue;
	private LinkedBlockingQueue<String> notDoneMsgQueue;
	private String msgSeparator;
	private AtomicReference<TCState> atomicState;
	private ExecutorService threadExecutor;
	private String jcChannel;
	
	public TerminationController() {
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		int totalMachines = propertyFileHandler.getShardCount();
		//node sends only one message in an 'iteration'. So this is fine.
		doneMsgQueue = new LinkedBlockingQueue<MachineAddrTime>(totalMachines);
		notDoneMsgQueue = new LinkedBlockingQueue<String>(totalMachines);
		atomicState = new AtomicReference<TCState>(TCState.NO_CHECK);
		threadExecutor = Executors.newSingleThreadExecutor();
		msgSeparator = propertyFileHandler.getExistentialAxiomSeparator();
		jcChannel = propertyFileHandler.getJobControllerChannel();		
	}
	
	@Override
	public void onMessage(String channel, String message) {
		System.out.println("Msg received: " + message);
		threadExecutor.execute(new MessageProcessor(this, message, doneMsgQueue, 
					notDoneMsgQueue, msgSeparator, jcChannel));
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
	public void onUnsubscribe(String channel, int subscribedChannels) { 
		threadExecutor.shutdown();
		System.out.println("Unsubscribed...");
	}
	
	public boolean compareAndSet(TCState expectedState, 
					TCState updatedState) {
		return atomicState.compareAndSet(expectedState, updatedState);
	}
	
	public void setState(TCState newState) {
		atomicState.set(newState);
	}

	
	public static void main(String[] args) {
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		String channel = propertyFileHandler.getTerminationControllerChannel();
		HostInfo localHostInfo = propertyFileHandler.getLocalHostInfo();
		System.out.println("Starting TC...");
		
		Jedis jedisPubSubListener = new Jedis(localHostInfo.getHost(), 
				localHostInfo.getPort(), Constants.INFINITE_TIMEOUT);
		jedisPubSubListener.subscribe(new TerminationController(), channel);
		// TODO: disconnect jedis instance after verifying
		jedisPubSubListener.disconnect();
		System.out.println("Exiting TC");
	}
}

/**
 * This class is responsible to process each 
 * message received by the TC
 * @author Raghava
 *
 */
class MessageProcessor implements Runnable {
	
	private LinkedBlockingQueue<MachineAddrTime> doneMsgQueue;
	private LinkedBlockingQueue<String> notDoneMsgQueue;
	private String message;
	private String msgSeparator;
	private String jcChannel;
	private TerminationController terminationController;
	
	MessageProcessor(TerminationController tc, String msg, 
			LinkedBlockingQueue<MachineAddrTime> doneQueue, 
			LinkedBlockingQueue<String> notDoneQueue, 
			String separator, String jobChannel) {
		terminationController = tc;
		message = msg;
		doneMsgQueue = doneQueue;
		notDoneMsgQueue = notDoneQueue;
		msgSeparator = separator;
		jcChannel = jobChannel;
	}
	
	public void run() {
		// separate code & IP from the message
		String codeIPTime[] = message.split(msgSeparator);
		try {
			MessageType messageType = MessageType.convertCodeToMessageType(codeIPTime[0]);
			if(messageType == MessageType.DONE) {
				// push it into the done queue
				MachineAddrTime machineAddrTime = new MachineAddrTime();
				machineAddrTime.machineIP = codeIPTime[1];
				machineAddrTime.timeTaken = Double.parseDouble(codeIPTime[2]);
				doneMsgQueue.put(machineAddrTime);
				
				// check & remove from not-done queue if present
				if(notDoneMsgQueue.contains(codeIPTime[1]))
					notDoneMsgQueue.remove(codeIPTime[1]);
				
				// check if the done queue is full and change state
				if(doneMsgQueue.remainingCapacity() == 0) {
					// since msgs from all the nodes have been received,
					// change the state
					boolean changeStatus = terminationController.compareAndSet(TCState.NO_CHECK, 
											TCState.SINGLE_CHECK_DONE);
					if(changeStatus) {
						// send CHECK_AND_RESTART msg to all the nodes
						sendMsgToJC(MessageType.CHECK_AND_RESTART);	
					}
					else {
						terminationController.compareAndSet(TCState.SINGLE_CHECK_DONE, 
											TCState.DOUBLE_CHECK_DONE);
						
						// get the runtimes of all the machines
						computeAvgClassificationTime();
						
						// terminate all the job controllers
						sendMsgToJC(MessageType.TERMINATE);
						terminationController.unsubscribe();
					}				
				}
			}				
			else if(messageType == MessageType.NOT_DONE) {
				// push it into the not-done queue
				notDoneMsgQueue.add(codeIPTime[1]);
				// set the state to NO_CHECK, so that the checking is done again
				terminationController.setState(TCState.NO_CHECK);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private void computeAvgClassificationTime() {
		Iterator<MachineAddrTime> msgQueueIt = doneMsgQueue.iterator();
		double totalTime = 0;
		int totalNodes = 0;
		while(msgQueueIt.hasNext()) {
			MachineAddrTime ipTotalTime = msgQueueIt.next();
			totalTime += ipTotalTime.timeTaken;
			System.out.println(ipTotalTime.machineIP + "\t" + ipTotalTime.timeTaken);
			totalNodes++;
		}
		double avgTime = totalTime/(totalNodes * 1000000000);
		System.out.println("\nAvg time (secs) : " + avgTime);
	}
	
	/**
	 * WARNING: using 6379 as port, if another port is used, it needs to
	 * be changed here appropriately.
	 * 
	 * @param msgType
	 */
	private void sendMsgToJC(MessageType msgType) {
		MachineAddrTime machineAddrTime;
		while((machineAddrTime = doneMsgQueue.poll()) != null) {
			Jedis jcJedis = new Jedis(machineAddrTime.machineIP, 
					6379, Constants.INFINITE_TIMEOUT);
			jcJedis.publish(jcChannel, msgType.getMessageTypeCode());
			jcJedis.disconnect();
		}
	}
}


/**
 * Represents the different states of TC. Double check is required
 * considering the following situation: Node n1 finds its queues empty
 * at t1 and node n2 finds out that its queues are empty at t2; t1 != t2;
 * both of them send 'DONE' msg to TC. But they could have placed items in
 * the queues of other nodes. That is why a second check and reset (if queues
 * are not empty) is required.
 * 
 * @author Raghava
 *
 */
enum TCState { NO_CHECK, SINGLE_CHECK_DONE, DOUBLE_CHECK_DONE }

class MachineAddrTime {
	String machineIP;
	double timeTaken;
}
