package org.codefilarete.stalactite.engine;

/**
 * @author Guillaume Mary
 */
public class StaleStateObjectException extends RuntimeException {
	
	public StaleStateObjectException(long expectedRowCount, long effectiveRowCount) {
		this(expectedRowCount + " rows were expected to be hit but " + effectiveRowCount + " were effectively");
	}
	
	public StaleStateObjectException(String message) {
		super(message);
	}
}
