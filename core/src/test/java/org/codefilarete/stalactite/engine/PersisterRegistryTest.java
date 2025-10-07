package org.codefilarete.stalactite.engine;

import java.sql.Timestamp;
import java.util.Date;

import org.codefilarete.stalactite.engine.PersisterRegistry.DefaultPersisterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PersisterRegistryTest {
	
	@Test
	void getPersister() {
		DefaultPersisterRegistry testInstance = new DefaultPersisterRegistry();
		EntityPersister persisterMock = mock(EntityPersister.class);
		when(persisterMock.getClassToPersist()).thenReturn(Date.class);
		testInstance.addPersister(persisterMock);
		
		// check with persister type
		assertThat(testInstance.getPersister(Date.class)).isEqualTo(persisterMock);
		
		assertThat(testInstance.getPersisters()).extracting(EntityPersister::getClassToPersist).contains(Date.class);
	}
}