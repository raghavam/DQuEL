package knoelab.classification;

/**
 * This enumeration represents the different symbols
 * that are used to represent/separate axioms and roles
 * @author Raghava
 *
 */
public enum SymbolType {
	
	GENERAL_AXIOM((byte)0), 
	QUEUE((byte)1), 
	SUPERCLASS((byte)2), 
	EQUI_AXIOM((byte)3),
	
	PROP_CHAIN_FWD((byte)4), 
	PROP_CHAIN_REV((byte)5), 
	ROLE_COMPOUND1((byte)6), 
	ROLE_COMPOUND2((byte)7), 
	ROLE_CONCEPT((byte)8),
	
	COMPLEX_AXIOM_SEPARATOR((byte)9), 
	EXISTENTIAL_AXIOM_SEPARATOR((byte)10);
	
	private byte value;
	
	SymbolType(byte b) {
		value = b;
	}
	
	public byte getByteValue() {
		return value;
	}
}
