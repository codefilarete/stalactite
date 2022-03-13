package org.codefilarete.stalactite.engine;

/**
 * @author Guillaume Mary
 */
public class RuntimeMappingException extends RuntimeException {
	
	public RuntimeMappingException() {
		
	}
	
	public RuntimeMappingException(String message) {
		super(message);
	}
	
	public RuntimeMappingException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public RuntimeMappingException(Throwable cause) {
		super(cause);
	}
}
