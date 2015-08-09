package org.gama.stalactite.persistence.id;

import java.io.Serializable;
import java.util.Map;

/**
 * Marker class for identifier generators that mustn't be called ! because entity has already set its identifier
 * by itself.
 * Shouldn't be combined with {@link BeforeInsertIdentifierGenerator} not {@link AfterInsertIdentifierGenerator}.
 * 
 * @author Guillaume Mary
 */
public class AutoAssignedIdentifierGenerator implements IdentifierGenerator {
	@Override
	public Serializable generate() {
		return null;
	}
	
	@Override
	public void configure(Map<String, Object> configuration) {
		
	}
}
