package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PersistExecutorTest {
	
	@Test
	void forPersister_notAlreadyAssignedIdentifier_persistIsBasedOnIsNewAndDatabaseSelect() {
		ConfiguredPersister<Toto, Integer> persister = mock(ConfiguredPersister.class, RETURNS_MOCKS);
		
		when(persister.getId(any())).thenAnswer((Answer<Integer>) invocationOnMock -> invocationOnMock.getArgument(0, Toto.class).id);
		when(persister.isNew(any())).thenAnswer((Answer<Boolean>) invocationOnMock -> invocationOnMock.getArgument(0, Toto.class).id == 0);
		
		// we fix the identifier insertion manager because forPersister base its condition on it
		when(persister.getMapping().getIdMapping().getIdentifierInsertionManager()).thenReturn(mock(IdentifierInsertionManager.class));
		
		PersistExecutor<Toto> result = PersistExecutor.forPersister(persister);
		
		// mixing new entities and existing ones
		Toto newEntity = new Toto(0);
		Toto existingEntity = new Toto(42);
		
		when(persister.select(anyCollection())).thenAnswer((Answer<Set<Toto>>) invocationOnMock -> Arrays.asSet(existingEntity));
		
		result.persist(newEntity, existingEntity);
		
		// every entity is tested against novelty
		verify(persister).isNew(newEntity);
		verify(persister).isNew(existingEntity);
		// only new ones are inserted
		verify(persister).insert(Arrays.asSet(newEntity));
		// only existing one is selected from the database
		verify(persister).select(Arrays.asHashSet(existingEntity.id));
		// only existing ones are updated
		verify(persister).update(Arrays.asSet(new Duo<>(existingEntity, existingEntity)), true);
	}
	
	@Test
	<T extends Table<T>> void forPersister_alreadyAssignedIdentifier_withoutManagingLambdas_persistIsBasedOnDatabaseSelect() {
		ConfiguredPersister<Toto, Integer> persister = mock(ConfiguredPersister.class, RETURNS_MOCKS);
		
		when(persister.getId(any())).thenAnswer((Answer<Integer>) invocationOnMock -> invocationOnMock.getArgument(0, Toto.class).id);
		
		// we fix the identifier insertion manager because forPersister base its condition on it
		AlreadyAssignedIdentifierManager<Toto, Integer> identifierManager = new AlreadyAssignedIdentifierManager<>(Integer.class, null, null);
		EntityMapping<Toto, Integer, T> entityMappingMock = mock(EntityMapping.class);
		when(persister.<T>getMapping()).thenReturn(entityMappingMock);
		IdMapping<Toto, Integer> idMappingMock = mock(IdMapping.class);
		when(entityMappingMock.getIdMapping()).thenReturn(idMappingMock);
		when(idMappingMock.getIdentifierInsertionManager()).thenReturn(identifierManager);
		
		PersistExecutor<Toto> result = PersistExecutor.forPersister(persister);
		
		// mixing new entities and existing ones
		Toto newEntity = new Toto(0);
		Toto existingEntity = new Toto(42);
		
		when(persister.select(anyCollection())).thenAnswer((Answer<Set<Toto>>) invocationOnMock -> Arrays.asSet(existingEntity));
		
		result.persist(newEntity, existingEntity);
		
		// no entity is tested against novelty
		verify(persister, never()).isNew(any());
		// all entities are selected from the database
		verify(persister).select(Arrays.asHashSet(existingEntity.id, newEntity.id));
		// only new ones are inserted
		verify(persister).insert(Arrays.asSet(newEntity));
		// only existing ones are updated
		verify(persister).update(Arrays.asSet(new Duo<>(existingEntity, existingEntity)), true);
	}
	
	@Test
	<T extends Table<T>> void forPersister_alreadyAssignedIdentifier_withManagingLambdas_persistIsBasedOnDatabaseSelect() {
		ConfiguredPersister<Toto, Integer> persister = mock(ConfiguredPersister.class, RETURNS_MOCKS);
		
		Set<Toto> existingEntities = new HashSet<>();
		
		when(persister.getId(any())).thenAnswer((Answer<Integer>) invocationOnMock -> invocationOnMock.getArgument(0, Toto.class).id);
		when(persister.isNew(any())).thenAnswer((Answer<Boolean>) invocationOnMock -> !existingEntities.contains(invocationOnMock.getArgument(0, Toto.class)));
		
		// we fix the identifier insertion manager because forPersister base its condition on it
		AlreadyAssignedIdentifierManager<Toto, Integer> identifierManager = new AlreadyAssignedIdentifierManager<>(Integer.class, existingEntities::add, existingEntities::contains);
		EntityMapping<Toto, Integer, T> entityMappingMock = mock(EntityMapping.class);
		when(persister.<T>getMapping()).thenReturn(entityMappingMock);
		IdMapping<Toto, Integer> idMappingMock = mock(IdMapping.class);
		when(entityMappingMock.getIdMapping()).thenReturn(idMappingMock);
		when(idMappingMock.getIdentifierInsertionManager()).thenReturn(identifierManager);
		
		PersistExecutor<Toto> result = PersistExecutor.forPersister(persister);
		
		// mixing new entities and existing ones
		Toto newEntity = new Toto(0);
		Toto existingEntity = new Toto(42);
		
		existingEntities.add(existingEntity);
		when(persister.select(anyCollection())).thenAnswer((Answer<Set<Toto>>) invocationOnMock -> Arrays.asSet(existingEntity));
		
		result.persist(newEntity, existingEntity);
		
		// every entity is tested against novelty
		verify(persister).isNew(newEntity);
		verify(persister).isNew(existingEntity);
		// no entity is selected from the database
		verify(persister).select(Arrays.asHashSet(existingEntity.id));
		// only new ones are inserted
		verify(persister).insert(Arrays.asSet(newEntity));
		// only existing ones are updated
		verify(persister).update(Arrays.asSet(new Duo<>(existingEntity, existingEntity)), true);
	}
	
	private static class Toto {
		
		private int id;
		private String prop;
		
		public Toto(int id) {
			this.id = id;
		}
		
		public Toto(int id, String prop) {
			this.id = id;
			this.prop = prop;
		}
		
		public void setId(int id) {
			this.id = id;
		}
		
		public void setProp(String prop) {
			this.prop = prop;
		}
	}
}
