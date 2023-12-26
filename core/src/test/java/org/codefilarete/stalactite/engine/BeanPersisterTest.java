package org.codefilarete.stalactite.engine;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.PersisterITTest.Toto;
import org.codefilarete.stalactite.engine.PersisterITTest.TotoTable;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.BeanPersister;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Hanger.Holder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class BeanPersisterTest {
	
	@Test
	void constructor_mustIncludeIdMappingListener() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		ReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		Map<? extends ReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps.asMap(identifier, primaryKey);
		IdentifierInsertionManager<Toto, Long> identifierInsertionManagerMock = mock(IdentifierInsertionManager.class);
		when(identifierInsertionManagerMock.getIdentifierType()).thenReturn(Long.class);
		InsertListener identifierManagerInsertListenerMock = mock(InsertListener.class);
		when(identifierInsertionManagerMock.getInsertListener()).thenReturn(identifierManagerInsertListenerMock);
		ClassMapping<Toto, Long, TotoTable> classMappingStrategy = new ClassMapping<>(Toto.class, totoTable,
																					  mapping, identifier,
																					  identifierInsertionManagerMock);
		BeanPersister<Toto, Long, TotoTable> testInstance = new BeanPersister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected void doInsert(Iterable entities) {
			}
			
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected void doUpdateById(Iterable<? extends Toto> entities) {
			}
		};
		
		Toto toto = new Toto();
		testInstance.insert(toto);
		verify(identifierManagerInsertListenerMock).afterInsert(ArgumentMatchers.eq(Arrays.asList(toto)));
	}
	
	@Test
	void persist() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Integer> primaryKey = totoTable.addColumn("a", Integer.class).primaryKey();
		ReversibleAccessor<Toto, Integer> identifier = Accessors.accessorByField(Toto.class, "a");
		Map<? extends ReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps.asMap(identifier, primaryKey);
		IdentifierInsertionManager<Toto, Integer> identifierInsertionManagerMock = mock(IdentifierInsertionManager.class);
		when(identifierInsertionManagerMock.getIdentifierType()).thenReturn(Integer.class);
		InsertListener identifierManagerInsertListenerMock = mock(InsertListener.class);
		when(identifierInsertionManagerMock.getInsertListener()).thenReturn(identifierManagerInsertListenerMock);
		SelectListener identifierManagerSelectListenerMock = mock(SelectListener.class);
		when(identifierInsertionManagerMock.getSelectListener()).thenReturn(identifierManagerSelectListenerMock);
		ClassMapping<Toto, Integer, TotoTable> classMappingStrategy = new ClassMapping<>(Toto.class, totoTable,
																						 mapping, identifier,
																						 identifierInsertionManagerMock);
		Holder<Toto> mockedSelectAnswer = new Holder<>();
		BeanPersister<Toto, Integer, TotoTable> testInstance = new BeanPersister<Toto, Integer, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected void doInsert(Iterable entities) {
			}
			
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected void doUpdate(Iterable<? extends Duo<Toto, Toto>> entities, boolean allColumnsStatement) {
			}
			
			@Override
			protected Set<Toto> doSelect(Iterable<Integer> ids) {
				return Iterables.collect(ids, id -> mockedSelectAnswer.get(), HashSet::new);
			}
		};
		
		
		PersistListener persistListener = mock(PersistListener.class);
		testInstance.getPersisterListener().addPersistListener(persistListener);
		InsertListener insertListener = mock(InsertListener.class);
		testInstance.getPersisterListener().addInsertListener(insertListener);
		UpdateListener updateListener = mock(UpdateListener.class);
		testInstance.getPersisterListener().addUpdateListener(updateListener);
		
		testInstance.persist(Arrays.asList());
		verifyNoMoreInteractions(persistListener);
		verifyNoMoreInteractions(insertListener);
		verifyNoMoreInteractions(updateListener);
		verifyNoMoreInteractions(identifierManagerInsertListenerMock);
		verifyNoMoreInteractions(identifierManagerSelectListenerMock);
		
		// On persist of a never persisted instance (no id), insertion chain must be invoked 
		Toto unPersisted = new Toto();
		Toto persisted = new Toto(1, 2, 3);
		mockedSelectAnswer.set(persisted);	// filling mock
		
		testInstance.persist(unPersisted);
		verify(persistListener).beforePersist(eq(Arrays.asSet(unPersisted)));
		verify(persistListener).afterPersist(eq(Arrays.asSet(unPersisted)));
		verify(insertListener).beforeInsert(eq(Arrays.asList(unPersisted)));
		verify(insertListener).afterInsert(eq(Arrays.asList(unPersisted)));
		verify(identifierManagerInsertListenerMock).beforeInsert(eq(Arrays.asList(unPersisted)));
		verify(identifierManagerInsertListenerMock).afterInsert(eq(Arrays.asList(unPersisted)));
		// no invocation of select listener because target of persist(..) method wasn't persisted
		verify(identifierManagerSelectListenerMock, never()).beforeSelect(anyIterable());
		verify(identifierManagerSelectListenerMock, never()).afterSelect(anySet());
		
		// On persist of a already persisted instance (with id), "rough update" chain must be invoked
		testInstance.persist(persisted);
		ArgumentCaptor<Iterable<Duo>> updateArgCaptor = ArgumentCaptor.forClass(Iterable.class);
		verify(updateListener).beforeUpdate(updateArgCaptor.capture(), eq(true));
		assertThat(updateArgCaptor.getValue()).containsExactly(new Duo<>(persisted, persisted));
		verify(updateListener).afterUpdate(updateArgCaptor.capture(), eq(true));
		assertThat(updateArgCaptor.getValue()).containsExactly(new Duo<>(persisted, persisted));
		
		clearInvocations(insertListener, identifierManagerInsertListenerMock, updateListener, identifierManagerSelectListenerMock);
		// mix
		Toto totoInDatabase = new Toto(1, 2, 3);
		mockedSelectAnswer.set(totoInDatabase);	// filling mock
		Toto totoModifiedFromDatabase = new Toto(1, 2, 4);
		
		testInstance.persist(Arrays.asList(unPersisted, totoModifiedFromDatabase));
		verify(insertListener).beforeInsert(eq(Arrays.asList(unPersisted)));
		verify(insertListener).afterInsert(eq(Arrays.asList(unPersisted)));
		verify(identifierManagerInsertListenerMock).beforeInsert(eq(Arrays.asList(unPersisted)));
		verify(identifierManagerInsertListenerMock).afterInsert(eq(Arrays.asList(unPersisted)));
		verify(identifierManagerSelectListenerMock).beforeSelect(eq(Arrays.asHashSet(1)));
		verify(identifierManagerSelectListenerMock).afterSelect(eq(Arrays.asHashSet(totoInDatabase)));
		verify(updateListener).beforeUpdate(updateArgCaptor.capture(), eq(true));
		assertThat(updateArgCaptor.getValue()).containsExactly(new Duo<>(totoModifiedFromDatabase, totoInDatabase));
		verify(updateListener).afterUpdate(updateArgCaptor.capture(), eq(true));
		assertThat(updateArgCaptor.getValue()).containsExactly(new Duo<>(totoModifiedFromDatabase, totoInDatabase));
	}
	
	@Test
	void insert() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		ReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		Map<? extends ReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps.asMap(identifier, primaryKey);
		
		IdentifierInsertionManager<Toto, Long> identifierInsertionManagerMock = mock(IdentifierInsertionManager.class);
		when(identifierInsertionManagerMock.getIdentifierType()).thenReturn(Long.class);
		InsertListener identifierManagerInsertListenerMock = mock(InsertListener.class);
		when(identifierInsertionManagerMock.getInsertListener()).thenReturn(identifierManagerInsertListenerMock);
		ClassMapping<Toto, Long, TotoTable> classMappingStrategy = new ClassMapping<>(Toto.class, totoTable,
																					  mapping, identifier,
																					  identifierInsertionManagerMock);
		BeanPersister<Toto, Long, TotoTable> testInstance = new BeanPersister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected void doInsert(Iterable entities) {
			}
		};
		
		
		InsertListener insertListener = mock(InsertListener.class);
		testInstance.getPersisterListener().addInsertListener(insertListener);
		
		testInstance.insert(Arrays.asList());
		verifyNoMoreInteractions(insertListener);
		verifyNoMoreInteractions(identifierManagerInsertListenerMock);
		
		// On persist of a never persisted instance (no id), insertion chain must be invoked 
		Toto toBeInserted = new Toto(1, 2, 3);
		testInstance.insert(toBeInserted);
		verify(insertListener).beforeInsert(eq(Arrays.asList(toBeInserted)));
		verify(insertListener).afterInsert(eq(Arrays.asList(toBeInserted)));
		verify(identifierManagerInsertListenerMock).beforeInsert(eq(Arrays.asList(toBeInserted)));
		verify(identifierManagerInsertListenerMock).afterInsert(eq(Arrays.asList(toBeInserted)));
	}
	
	@Test
	void update() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		Column<TotoTable, Long> columnB = totoTable.addColumn("b", Long.class);
		ReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		ReversibleAccessor<Toto, Long> propB = Accessors.accessorByField(Toto.class, "b");
		// we must add a property to let us set some differences between 2 instances and have them detected by the system
		Map<? extends ReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps
				.asMap(identifier, primaryKey)
				.add(propB, columnB);
		ClassMapping<Toto, Long, TotoTable> classMappingStrategy = new ClassMapping<>(Toto.class, totoTable,
																					  mapping, identifier,
																					  new AlreadyAssignedIdentifierManager<>(Long.class, c -> {}, c -> false));
		BeanPersister<Toto, Long, TotoTable> testInstance = new BeanPersister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected void doUpdate(Iterable<? extends Duo<Toto, Toto>> entities, boolean allColumnsStatement) {
			}
		};
		
		UpdateListener updateListener = mock(UpdateListener.class);
		testInstance.getPersisterListener().addUpdateListener(updateListener);
		
		// when nothing to be updated, listener is not invoked
		testInstance.update(Arrays.asList(), false);
		verifyNoMoreInteractions(updateListener);
		testInstance.update(Arrays.asList(), true);
		verifyNoMoreInteractions(updateListener);
		
		Toto original = new Toto(1, 2, 3);
		Toto modified = new Toto(1, -2, -3);
		
		// On update of an already persisted instance (with id), "rough update" chain must be invoked
		testInstance.update(modified, original, false);
		Duo<Toto, Toto> expectedPayload = new Duo<>(modified, original);
		ArgumentCaptor<Iterable<Duo<Toto, TotoTable>>> listenerArgumentCaptor = ArgumentCaptor.forClass(Iterable.class);
		verify(updateListener).beforeUpdate(listenerArgumentCaptor.capture(), eq(false));
		assertThat(Iterables.first(listenerArgumentCaptor.getValue())).isEqualTo(expectedPayload);
		verify(updateListener).afterUpdate(listenerArgumentCaptor.capture(), eq(false));
		assertThat(Iterables.first(listenerArgumentCaptor.getValue())).isEqualTo(expectedPayload);
	}
	
	@Test
	void updateById() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		Column<TotoTable, Long> columnB = totoTable.addColumn("b", Long.class);
		ReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		ReversibleAccessor<Toto, Long> propB = Accessors.accessorByField(Toto.class, "b");
		// we must add a property to let us set some differences between 2 instances and have them detected by the system
		Map<? extends ReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps
				.asMap(identifier, primaryKey)
				.add(propB, columnB);
		ClassMapping<Toto, Long, TotoTable> classMappingStrategy = new ClassMapping<>(Toto.class, totoTable,
																					  mapping, identifier,
																					  new AlreadyAssignedIdentifierManager<>(Long.class, c -> {}, c -> false));
		BeanPersister<Toto, Long, TotoTable> testInstance = new BeanPersister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected void doUpdateById(Iterable<? extends Toto> entities) {
			}
		};
		
		UpdateByIdListener updateListener = mock(UpdateByIdListener.class);
		testInstance.getPersisterListener().addUpdateByIdListener(updateListener);
		
		// when nothing to be deleted, listener is not invoked
		testInstance.updateById(Arrays.asList());
		verifyNoMoreInteractions(updateListener);
		testInstance.updateById(Arrays.asList());
		verifyNoMoreInteractions(updateListener);
		
		Toto toBeUpdated = new Toto(1, 2, 3);
		
		// On persist of a already persisted instance (with id), "rough update" chain must be invoked
		testInstance.updateById(toBeUpdated);
		verify(updateListener).beforeUpdateById(Arrays.asList(toBeUpdated));
		verify(updateListener).afterUpdateById(Arrays.asList(toBeUpdated));
	}
	
	@Test
	void delete() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		Column<TotoTable, Long> columnB = totoTable.addColumn("b", Long.class);
		ReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		ReversibleAccessor<Toto, Long> propB = Accessors.accessorByField(Toto.class, "b");
		// we must add a property to let us set some differences between 2 instances and have them detected by the system
		Map<? extends ReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps
				.asMap(identifier, primaryKey)
				.add(propB, columnB);
		ClassMapping<Toto, Long, TotoTable> classMappingStrategy = new ClassMapping<>(Toto.class, totoTable,
																					  mapping, identifier,
																					  new AlreadyAssignedIdentifierManager<>(Long.class, c -> {}, c -> false));
		BeanPersister<Toto, Long, TotoTable> testInstance = new BeanPersister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected void doDelete(Iterable<? extends Toto> entities) {
			}
		};
		
		
		DeleteListener deleteListener = mock(DeleteListener.class);
		testInstance.getPersisterListener().addDeleteListener(deleteListener);
		
		// when nothing to be deleted, listener is not invoked
		testInstance.delete(Arrays.asList());
		verifyNoMoreInteractions(deleteListener);
		
		Toto toBeDeleted = new Toto(1, 2, 3);
		testInstance.delete(toBeDeleted);
		verify(deleteListener).beforeDelete(eq(Arrays.asList(toBeDeleted)));
		verify(deleteListener).afterDelete(eq(Arrays.asList(toBeDeleted)));
	}
	
	@Test
	void deleteById() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		Column<TotoTable, Long> columnB = totoTable.addColumn("b", Long.class);
		ReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		ReversibleAccessor<Toto, Long> propB = Accessors.accessorByField(Toto.class, "b");
		// we must add a property to let us set some differences between 2 instances and have them detected by the system
		Map<? extends ReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps
				.asMap(identifier, primaryKey)
				.add(propB, columnB);
		ClassMapping<Toto, Long, TotoTable> classMappingStrategy = new ClassMapping<>(Toto.class, totoTable,
																					  mapping, identifier,
																					  new AlreadyAssignedIdentifierManager<>(Long.class, c -> {}, c -> false));
		BeanPersister<Toto, Long, TotoTable> testInstance = new BeanPersister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected void doDeleteById(Iterable<? extends Toto> entities) {
			}
		};
		
		DeleteByIdListener deleteListener = mock(DeleteByIdListener.class);
		testInstance.getPersisterListener().addDeleteByIdListener(deleteListener);
		
		// when nothing to be deleted, listener is not invoked
		testInstance.deleteById(Arrays.asList());
		verifyNoMoreInteractions(deleteListener);
		Toto toBeDeleted = new Toto(1, 2, 3);
		
		testInstance.deleteById(toBeDeleted);
		verify(deleteListener).beforeDeleteById(eq(Arrays.asList(toBeDeleted)));
		verify(deleteListener).afterDeleteById(eq(Arrays.asList(toBeDeleted)));
	}
	
	@Test
	void select() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Integer> primaryKey = totoTable.addColumn("a", Integer.class).primaryKey();
		ReversibleAccessor<Toto, Integer> identifier = Accessors.accessorByField(Toto.class, "a");
		Map<? extends ReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps.asMap(identifier, primaryKey);
		IdentifierInsertionManager<Toto, Integer> identifierInsertionManagerMock = mock(IdentifierInsertionManager.class);
		when(identifierInsertionManagerMock.getIdentifierType()).thenReturn(Integer.class);
		InsertListener identifierManagerInsertListenerMock = mock(InsertListener.class);
		when(identifierInsertionManagerMock.getInsertListener()).thenReturn(identifierManagerInsertListenerMock);
		SelectListener identifierManagerSelectListenerMock = mock(SelectListener.class);
		when(identifierInsertionManagerMock.getSelectListener()).thenReturn(identifierManagerSelectListenerMock);
		ClassMapping<Toto, Integer, TotoTable> classMappingStrategy = new ClassMapping<>(Toto.class, totoTable,
																						 mapping, identifier,
																						 identifierInsertionManagerMock);
		BeanPersister<Toto, Integer, TotoTable> testInstance = new BeanPersister<Toto, Integer, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected void doInsert(Iterable entities) {
			}
			
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected void doUpdate(Iterable<? extends Duo<Toto, Toto>> entities, boolean allColumnsStatement) {
			}
			
			/** Overridden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected Set<Toto> doSelect(Iterable<Integer> ids) {
				return Iterables.collect(ids, id -> new Toto(id, null, null), HashSet::new);
			}
		};
		
		Toto selectedToto = testInstance.select(1);
		verify(identifierManagerSelectListenerMock).beforeSelect(eq(Arrays.asSet(1)));
		verify(identifierManagerSelectListenerMock).afterSelect(eq(Arrays.asHashSet(selectedToto)));
	}
}