package org.gama.stalactite.persistence.id;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author Guillaume Mary
 */
public class PersistableIdentifierTest {
	
	@Test
	public void testEquals() {
		PersistableIdentifier<Long> testInstance = new PersistableIdentifier<>(1L);
		assertEquals(testInstance, testInstance);
		assertNotEquals(new PersistableIdentifier<>(2L), testInstance);
		// test against PersistedIdentifier : are equal if persistable identifier is persisted 
		PersistedIdentifier<Long> persistedIdentifier = new PersistedIdentifier<>(1L);
		assertNotEquals(testInstance, persistedIdentifier);
		testInstance.setPersisted();
		assertEquals(testInstance, persistedIdentifier);
	}
	
}