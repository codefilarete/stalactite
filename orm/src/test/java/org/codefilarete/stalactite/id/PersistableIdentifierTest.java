package org.codefilarete.stalactite.id;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class PersistableIdentifierTest {
	
	@Test
	@SuppressWarnings("java:S5845" /* The goal of this test if to ensure that behavior : equals() must accept incompatible types */)
	void equals() {
		PersistableIdentifier<Long> testInstance = new PersistableIdentifier<>(1L);
		assertThat(testInstance)
			.isEqualTo(testInstance)
			.isNotEqualTo(new PersistableIdentifier<>(2L));
		// test against PersistedIdentifier : are equal if persistable identifier is persisted 
		PersistedIdentifier<Long> persistedIdentifier = new PersistedIdentifier<>(1L);
		assertThat(persistedIdentifier).isEqualTo(testInstance);
		testInstance.setPersisted();
		assertThat(persistedIdentifier).isEqualTo(testInstance);
	}
	
}