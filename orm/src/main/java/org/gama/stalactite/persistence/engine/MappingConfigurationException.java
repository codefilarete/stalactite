package org.gama.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public class MappingConfigurationException extends RuntimeException {
	
	public MappingConfigurationException() {
		
	}
	
	public MappingConfigurationException(String message) {
		super(message);
	}
	
	public MappingConfigurationException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public MappingConfigurationException(Throwable cause) {
		super(cause);
	}
}
