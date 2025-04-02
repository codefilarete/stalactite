package org.codefilarete.stalactite.sql.ddl;

/**
 * Stores the length of a char sequence data.
 *
 * @author Guillaume Mary
 */
public class Length implements Size {
	
	private final int value;
	
	public Length(int value) {
		this.value = value;
	}
	
	public int getValue() {
		return value;
	}
}