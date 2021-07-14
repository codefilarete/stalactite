package org.gama.stalactite.persistence.id;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class PersistedIdentifierTest {
	
	@Test
	public void testEquals() {
		PersistedIdentifier<Long> testInstance = new PersistedIdentifier<>(1L);
		assertThat(testInstance).isEqualTo(testInstance);
		assertThat(testInstance).isNotEqualTo(new PersistedIdentifier<>(2L));
		// test against PersistableIdentifier : are equal if persistable identifier is persisted 
		PersistableIdentifier<Long> persistableIdentifier = new PersistableIdentifier<>(1L);
		assertThat(persistableIdentifier).isEqualTo(testInstance);
		persistableIdentifier.setPersisted();
		assertThat(persistableIdentifier).isEqualTo(testInstance);
	}
	
}