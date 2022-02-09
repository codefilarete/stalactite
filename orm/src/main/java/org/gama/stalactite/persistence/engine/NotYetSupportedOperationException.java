package org.codefilarete.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public class NotYetSupportedOperationException extends RuntimeException {
	
	public NotYetSupportedOperationException() {
	}
	
	public NotYetSupportedOperationException(String message) {
		super(message);
	}
}
