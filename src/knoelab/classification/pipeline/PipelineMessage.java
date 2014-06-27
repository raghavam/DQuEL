package knoelab.classification.pipeline;

public class PipelineMessage {
	byte[] key;
	byte[] value;
	PipelineMessageType messageType;
	
	public PipelineMessage(byte[] key, byte[] value, 
			PipelineMessageType messageType) {
		this.key = key;
		this.value = value;
		this.messageType = messageType;
	}
}
