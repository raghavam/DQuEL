package knoelab.classification;

/**
 * This is an enumeration of the different
 * types of messages that could be exchanged 
 * between JobController and TerminationController
 * 
 * @author Raghava
 *
 */
public enum MessageType {
	DONE("1"),
	CHECK_AND_RESTART("2"),
	NOT_DONE("3"),
	TERMINATE("4");
	
	private String code;
	
	MessageType(String message) {
		code = message;
	}
	
	public String getMessageTypeCode() {
		return code;
	}
	
	public static MessageType convertCodeToMessageType(String code)
		throws Exception {
		
		switch(Integer.valueOf(code)) {
			case 1: return MessageType.DONE;
			case 2: return MessageType.CHECK_AND_RESTART;
			case 3: return MessageType.NOT_DONE;
			case 4: return MessageType.TERMINATE;
			default: throw new Exception("Unrecognised message code");
		}
	}
}
