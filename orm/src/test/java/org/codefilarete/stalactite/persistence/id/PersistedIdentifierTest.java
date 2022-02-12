package org.codefilarete.stalactite.persistence.id;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class PersistedIdentifierTest {
	
	@Test
	@SuppressWarnings("java:S5845" /* The goal of this test if to ensure that behavior : equals() must accept incompatible types */)
	void equals() {
		PersistedIdentifier<Long> testInstance = new PersistedIdentifier<>(1L);
		assertThat(testInstance)
			.isEqualTo(testInstance)
			.isNotEqualTo(new PersistedIdentifier<>(2L));
		// test against PersistableIdentifier : are equal if persistable identifier is persisted 
		PersistableIdentifier<Long> persistableIdentifier = new PersistableIdentifier<>(1L);
		assertThat(persistableIdentifier).isEqualTo(testInstance);
		persistableIdentifier.setPersisted();
		assertThat(persistableIdentifier).isEqualTo(testInstance);
	}
	
}