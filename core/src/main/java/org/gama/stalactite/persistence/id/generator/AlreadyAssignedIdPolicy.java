package org.gama.stalactite.persistence.id.generator;

import java.util.Map;

/**
 * Marker class for identifier generators that mustn't be called ! because entity has already set its identifier
 * by itself.
 * Shouldn't be combined with {@link BeforeInsertIdPolicy} nor {@link JDBCGeneratedKeysIdPolicy}.
 * 
 * @author Guillaume Mary
 * @see AutoAssignedIdentifierManager#INSTANCE
 */
public class AlreadyAssignedIdPolicy implements IdAssignmentPolicy {
	
	private AlreadyAssignedIdPolicy() {
	}
	
	@Override
	public void configure(Map<String, Object> configuration) {
		
	}
}
