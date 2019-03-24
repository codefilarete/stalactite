package org.gama.stalactite.persistence.id;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Guillaume Mary
 */
public class PersistedIdentifierTest {
	
	@Test
	public void testEquals() {
		PersistedIdentifier<Long> testInstance = new PersistedIdentifier<>(1L);
		assertEquals(testInstance, testInstance);
		assertNotEquals(new PersistedIdentifier<>(2L), testInstance);
		// test against PersistableIdentifier : are equal if persistable identifier is persisted 
		PersistableIdentifier<Long> persistableIdentifier = new PersistableIdentifier<>(1L);
		assertNotEquals(testInstance, persistableIdentifier);
		persistableIdentifier.setPersisted();
		assertEquals(testInstance, persistableIdentifier);
	}
	
}