package knoelab.classification;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * This class generates all types of keys.
 * @author Raghava
 *
 */
public class KeyGenerator {
	
	private static final int INT_SIZE = 4;
	private static final int NUM_BYTES = 5;
	private static final int COMPOUND_NUM_BYTES = 10;
	private static final int VALUE_NUM_BYTES = 9;
		
	public static byte[] generateQueueKey(byte[] conceptID) {
		byte symbolType = SymbolType.QUEUE.getByteValue();
		return ByteBuffer.allocate(NUM_BYTES).put(symbolType).put(conceptID).array();
	}
	
	public static byte[] generateExistentialQueueKey(byte[] roleID, byte[] conceptID) {
		byte symbolType = SymbolType.QUEUE.getByteValue();
		byte existentialSeparator = SymbolType.EXISTENTIAL_AXIOM_SEPARATOR.getByteValue();
		return ByteBuffer.allocate(COMPOUND_NUM_BYTES).put(symbolType).
									put(existentialSeparator).
									put(roleID).put(conceptID).array();
	}
	
	public static byte[] generateAxiomKey(byte[] conceptID) {
		byte symbolType = SymbolType.GENERAL_AXIOM.getByteValue();
		return ByteBuffer.allocate(NUM_BYTES).put(symbolType).put(conceptID).array();
	}
	
	public static byte[] generateExistentialAxiomKey(byte[] roleID, byte[] conceptID) {
		byte symbolType = SymbolType.GENERAL_AXIOM.getByteValue();
		byte existentialSeparator = SymbolType.EXISTENTIAL_AXIOM_SEPARATOR.getByteValue();
		return ByteBuffer.allocate(COMPOUND_NUM_BYTES).put(symbolType).
									put(existentialSeparator).
									put(roleID).put(conceptID).array();
	}
	
	public static byte[] generateSuperclassKey(byte[] conceptID) {
		byte symbolType = SymbolType.SUPERCLASS.getByteValue();
		return ByteBuffer.allocate(NUM_BYTES).put(symbolType).put(conceptID).array();
	}
	
	public static byte[] generateResultRoleConceptKey(byte[] conceptID) {
		byte symbolType = SymbolType.ROLE_CONCEPT.getByteValue();
		return ByteBuffer.allocate(NUM_BYTES).put(symbolType).put(conceptID).array();
	}
	
	public static byte[] generateResultRoleCompoundKey1(byte[] roleID, byte[] conceptID) {
		byte symbolType = SymbolType.ROLE_COMPOUND1.getByteValue();
		byte axiomSeparator = SymbolType.COMPLEX_AXIOM_SEPARATOR.getByteValue();
		return ByteBuffer.allocate(COMPOUND_NUM_BYTES).put(symbolType).
									put(axiomSeparator).
									put(roleID).put(conceptID).array();
	}
	
	public static byte[] generateResultRoleCompoundKey2(byte[] roleID, byte[] conceptID) {
		byte symbolType = SymbolType.ROLE_COMPOUND2.getByteValue();
		byte axiomSeparator = SymbolType.COMPLEX_AXIOM_SEPARATOR.getByteValue();
		return ByteBuffer.allocate(COMPOUND_NUM_BYTES).put(symbolType).
									put(axiomSeparator).
									put(roleID).put(conceptID).array();
	}
	
	public static byte[] generatePropertyChainKey1(byte[] roleID) {
		byte symbolType = SymbolType.PROP_CHAIN_FWD.getByteValue();
		return ByteBuffer.allocate(NUM_BYTES).put(symbolType).put(roleID).array();
	}
	
	public static byte[] generatePropertyChainKey2(byte[] roleID) {
		byte symbolType = SymbolType.PROP_CHAIN_REV.getByteValue();
		return ByteBuffer.allocate(NUM_BYTES).put(symbolType).put(roleID).array();
	}
	
	public static byte[] generateEquivalentClassAxiomKey(byte[] conceptID) {
		byte symbolType = SymbolType.EQUI_AXIOM.getByteValue();
		return ByteBuffer.allocate(NUM_BYTES).put(symbolType).put(conceptID).array();
	}
	
	public static byte[] generateExistentialValue(byte[] roleID, byte[] conceptID) {
		byte existentialSeparator = SymbolType.EXISTENTIAL_AXIOM_SEPARATOR.getByteValue();
		return ByteBuffer.allocate(VALUE_NUM_BYTES).put(existentialSeparator).
									put(roleID).put(conceptID).array();
	}
	
	public static byte[] generateCompoundAxiomValue(Set<byte[]> operandIDs, byte[] superConceptID) {
		byte axiomSeparator = SymbolType.COMPLEX_AXIOM_SEPARATOR.getByteValue();
		int numOperandBytes = operandIDs.size() * INT_SIZE;
		ByteBuffer compoundAxiomValue = ByteBuffer.allocate(NUM_BYTES + numOperandBytes);
		compoundAxiomValue.put(axiomSeparator);
		compoundAxiomValue.put(superConceptID);
		for(byte[] op : operandIDs)
			compoundAxiomValue.put(op);
		return compoundAxiomValue.array();
	}
}

