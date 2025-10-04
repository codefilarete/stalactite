package org.codefilarete.stalactite.dsl.idpolicy;

import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequenceStorageOptions;

/**
 * Default configuration to store sequence values for before-insert identifier policy
 *
 * @author Guillaume Mary
 */
public class PooledHiLoSequenceIdentifierPolicySupport implements BeforeInsertIdentifierPolicy<Long> {
	
	private final PooledHiLoSequenceStorageOptions storageOptions;
	
	public PooledHiLoSequenceIdentifierPolicySupport() {
		this.storageOptions = PooledHiLoSequenceStorageOptions.DEFAULT;
	}
	
	public PooledHiLoSequenceIdentifierPolicySupport(PooledHiLoSequenceStorageOptions sequenceStorageOptions) {
		this.storageOptions = sequenceStorageOptions;
	}
	
	public PooledHiLoSequenceStorageOptions getStorageOptions() {
		return storageOptions;
	}
}
