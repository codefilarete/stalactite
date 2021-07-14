package org.gama.stalactite.persistence.id;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class PersistableIdentifierTest {
	
	@Test
	public void testEquals() {
		PersistableIdentifier<Long> testInstance = new PersistableIdentifier<>(1L);
		assertThat(testInstance).isEqualTo(testInstance);
		assertThat(testInstance).isNotEqualTo(new PersistableIdentifier<>(2L));
		// test against PersistedIdentifier : are equal if persistable identifier is persisted 
		PersistedIdentifier<Long> persistedIdentifier = new PersistedIdentifier<>(1L);
		assertThat(persistedIdentifier).isEqualTo(testInstance);
		testInstance.setPersisted();
		assertThat(persistedIdentifier).isEqualTo(testInstance);
	}
	
}