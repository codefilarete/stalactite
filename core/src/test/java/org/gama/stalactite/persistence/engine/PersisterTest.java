package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.gama.lang.Duo;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.function.Hanger.Holder;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.PersisterITTest.Toto;
import org.gama.stalactite.persistence.engine.PersisterITTest.TotoTable;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateByIdListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.engine.runtime.Persister;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.sql.ConnectionProvider;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class PersisterTest {
	
	@Test
	void constructor_mustIncludeIdMappingListener() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		IReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		Map<? extends IReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps.asMap(identifier, primaryKey);
		IdentifierInsertionManager<Toto, Long> identifierInsertionManagerMock = mock(IdentifierInsertionManager.class);
		when(identifierInsertionManagerMock.getIdentifierType()).thenReturn(Long.class);
		InsertListener identifierManagerInsertListenerMock = mock(InsertListener.class);
		when(identifierInsertionManagerMock.getInsertListener()).thenReturn(identifierManagerInsertListenerMock);
		ClassMappingStrategy<Toto, Long, TotoTable> classMappingStrategy = new ClassMappingStrategy<>(Toto.class, totoTable,
				mapping, identifier,
				identifierInsertionManagerMock);
		Persister<Toto, Long, TotoTable> testInstance = new Persister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected int doInsert(Iterable entities) {
				return ((Collection) entities).size();
			}
			
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected int doUpdateById(Iterable<Toto> entities) {
				return ((Collection) entities).size();
			}
		};
		
		Toto toto = new Toto();
		testInstance.insert(toto);
		verify(identifierManagerInsertListenerMock).afterInsert(ArgumentMatchers.eq(Arrays.asList(toto)));
	}
	
	@Test
	void testPersist() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Integer> primaryKey = totoTable.addColumn("a", Integer.class).primaryKey();
		IReversibleAccessor<Toto, Integer> identifier = Accessors.accessorByField(Toto.class, "a");
		Map<? extends IReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps.asMap(identifier, primaryKey);
		IdentifierInsertionManager<Toto, Integer> identifierInsertionManagerMock = mock(IdentifierInsertionManager.class);
		when(identifierInsertionManagerMock.getIdentifierType()).thenReturn(Integer.class);
		InsertListener identifierManagerInsertListenerMock = mock(InsertListener.class);
		when(identifierInsertionManagerMock.getInsertListener()).thenReturn(identifierManagerInsertListenerMock);
		SelectListener identifierManagerSelectListenerMock = mock(SelectListener.class);
		when(identifierInsertionManagerMock.getSelectListener()).thenReturn(identifierManagerSelectListenerMock);
		ClassMappingStrategy<Toto, Integer, TotoTable> classMappingStrategy = new ClassMappingStrategy<>(Toto.class, totoTable,
				mapping, identifier,
				identifierInsertionManagerMock);
		Holder<Toto> mockedSelectAnswer = new Holder<>();
		Persister<Toto, Integer, TotoTable> testInstance = new Persister<Toto, Integer, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected int doInsert(Iterable entities) {
				return ((Collection) entities).size();
			}
			
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected int doUpdate(Iterable<? extends Duo<Toto, Toto>> entities, boolean allColumnsStatement) {
				return ((Collection) entities).size();
			}
			
			@Override
			protected List<Toto> doSelect(Iterable<Integer> ids) {
				return Iterables.collectToList(ids, id -> mockedSelectAnswer.get());
			}
		};
		
		
		InsertListener insertListener = mock(InsertListener.class);
		testInstance.getPersisterListener().addInsertListener(insertListener);
		UpdateListener updateListener = mock(UpdateListener.class);
		testInstance.getPersisterListener().addUpdateListener(updateListener);
		
		int rowCount = testInstance.persist(Arrays.asList());
		assertThat(rowCount).isEqualTo(0);
		verifyNoMoreInteractions(insertListener);
		verifyNoMoreInteractions(updateListener);
		verifyNoMoreInteractions(identifierManagerInsertListenerMock);
		verifyNoMoreInteractions(identifierManagerSelectListenerMock);
		
		// On persist of a never persisted instance (no id), insertion chain must be invoked 
		Toto unPersisted = new Toto();
		Toto persisted = new Toto(1, 2, 3);
		mockedSelectAnswer.set(persisted);	// filling mock
		
		rowCount = testInstance.persist(unPersisted);
		assertThat(rowCount).isEqualTo(1);
		verify(insertListener).beforeInsert(eq(Arrays.asList(unPersisted)));
		verify(insertListener).afterInsert(eq(Arrays.asList(unPersisted)));
		verify(identifierManagerInsertListenerMock).beforeInsert(eq(Arrays.asList(unPersisted)));
		verify(identifierManagerInsertListenerMock).afterInsert(eq(Arrays.asList(unPersisted)));
		// no invokation of select listener because target of persist(..) method wasn't persisted
		verify(identifierManagerSelectListenerMock, never()).beforeSelect(anyIterable());
		verify(identifierManagerSelectListenerMock, never()).afterSelect(anyIterable());
		
		// On persist of a already persisted instance (with id), "rough update" chain must be invoked
		rowCount = testInstance.persist(persisted);
		assertThat(rowCount).isEqualTo(1);
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
		
		rowCount = testInstance.persist(Arrays.asList(unPersisted, totoModifiedFromDatabase));
		assertThat(rowCount).isEqualTo(2);
		verify(insertListener).beforeInsert(eq(Arrays.asList(unPersisted)));
		verify(insertListener).afterInsert(eq(Arrays.asList(unPersisted)));
		verify(identifierManagerInsertListenerMock).beforeInsert(eq(Arrays.asList(unPersisted)));
		verify(identifierManagerInsertListenerMock).afterInsert(eq(Arrays.asList(unPersisted)));
		verify(identifierManagerSelectListenerMock).beforeSelect(eq(Arrays.asList(1)));
		verify(identifierManagerSelectListenerMock).afterSelect(eq(Arrays.asList(totoInDatabase)));
		verify(updateListener).beforeUpdate(updateArgCaptor.capture(), eq(true));
		assertThat(updateArgCaptor.getValue()).containsExactly(new Duo<>(totoModifiedFromDatabase, totoInDatabase));
		verify(updateListener).afterUpdate(updateArgCaptor.capture(), eq(true));
		assertThat(updateArgCaptor.getValue()).containsExactly(new Duo<>(totoModifiedFromDatabase, totoInDatabase));
	}
	
	@Test
	void testInsert() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		IReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		Map<? extends IReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps.asMap(identifier, primaryKey);
		
		IdentifierInsertionManager<Toto, Long> identifierInsertionManagerMock = mock(IdentifierInsertionManager.class);
		when(identifierInsertionManagerMock.getIdentifierType()).thenReturn(Long.class);
		InsertListener identifierManagerInsertListenerMock = mock(InsertListener.class);
		when(identifierInsertionManagerMock.getInsertListener()).thenReturn(identifierManagerInsertListenerMock);
		ClassMappingStrategy<Toto, Long, TotoTable> classMappingStrategy = new ClassMappingStrategy<>(Toto.class, totoTable,
				mapping, identifier,
				identifierInsertionManagerMock);
		Persister<Toto, Long, TotoTable> testInstance = new Persister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected int doInsert(Iterable entities) {
				return ((Collection) entities).size();
			}
		};
		
		
		InsertListener insertListener = mock(InsertListener.class);
		testInstance.getPersisterListener().addInsertListener(insertListener);
		
		int rowCount = testInstance.insert(Arrays.asList());
		assertThat(rowCount).isEqualTo(0);
		verifyNoMoreInteractions(insertListener);
		verifyNoMoreInteractions(identifierManagerInsertListenerMock);
		
		// On persist of a never persisted instance (no id), insertion chain must be invoked 
		Toto toBeInserted = new Toto(1, 2, 3);
		rowCount = testInstance.insert(toBeInserted);
		assertThat(rowCount).isEqualTo(1);
		verify(insertListener).beforeInsert(eq(Arrays.asList(toBeInserted)));
		verify(insertListener).afterInsert(eq(Arrays.asList(toBeInserted)));
		verify(identifierManagerInsertListenerMock).beforeInsert(eq(Arrays.asList(toBeInserted)));
		verify(identifierManagerInsertListenerMock).afterInsert(eq(Arrays.asList(toBeInserted)));
	}
	
	@Test
	void testUpdate() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		Column<TotoTable, Long> columnB = totoTable.addColumn("b", Long.class);
		IReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		IReversibleAccessor<Toto, Long> propB = Accessors.accessorByField(Toto.class, "b");
		// we must add a property to let us set some differences between 2 instances and have them detected by the system
		Map<? extends IReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps
				.asMap(identifier, primaryKey)
				.add(propB, columnB);
		ClassMappingStrategy<Toto, Long, TotoTable> classMappingStrategy = new ClassMappingStrategy<>(Toto.class, totoTable,
				mapping, identifier,
				new AlreadyAssignedIdentifierManager<>(Long.class, c -> {}, c -> false));
		Persister<Toto, Long, TotoTable> testInstance = new Persister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected int doUpdate(Iterable<? extends Duo<Toto, Toto>> entities, boolean allColumnsStatement) {
				return ((Collection) entities).size();
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
	void testUpdateById() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		Column<TotoTable, Long> columnB = totoTable.addColumn("b", Long.class);
		IReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		IReversibleAccessor<Toto, Long> propB = Accessors.accessorByField(Toto.class, "b");
		// we must add a property to let us set some differences between 2 instances and have them detected by the system
		Map<? extends IReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps
				.asMap(identifier, primaryKey)
				.add(propB, columnB);
		ClassMappingStrategy<Toto, Long, TotoTable> classMappingStrategy = new ClassMappingStrategy<>(Toto.class, totoTable,
				mapping, identifier,
				new AlreadyAssignedIdentifierManager<>(Long.class, c -> {}, c -> false));
		Persister<Toto, Long, TotoTable> testInstance = new Persister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected int doUpdateById(Iterable entities) {
				return ((Collection) entities).size();
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
	void testDelete() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		Column<TotoTable, Long> columnB = totoTable.addColumn("b", Long.class);
		IReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		IReversibleAccessor<Toto, Long> propB = Accessors.accessorByField(Toto.class, "b");
		// we must add a property to let us set some differences between 2 instances and have them detected by the system
		Map<? extends IReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps
				.asMap(identifier, primaryKey)
				.add(propB, columnB);
		ClassMappingStrategy<Toto, Long, TotoTable> classMappingStrategy = new ClassMappingStrategy<>(Toto.class, totoTable,
				mapping, identifier,
				new AlreadyAssignedIdentifierManager<>(Long.class, c -> {}, c -> false));
		Persister<Toto, Long, TotoTable> testInstance = new Persister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected int doDelete(Iterable entities) {
				return ((Collection) entities).size();
			}
		};
		
		
		DeleteListener deleteListener = mock(DeleteListener.class);
		testInstance.getPersisterListener().addDeleteListener(deleteListener);
		
		// when nothing to be deleted, listener is not invoked
		int rowCount = testInstance.delete(Arrays.asList());
		assertThat(rowCount).isEqualTo(0);
		verifyNoMoreInteractions(deleteListener);
		
		Toto toBeDeleted = new Toto(1, 2, 3);
		rowCount = testInstance.delete(toBeDeleted);
		assertThat(rowCount).isEqualTo(1);
		verify(deleteListener).beforeDelete(eq(Arrays.asList(toBeDeleted)));
		verify(deleteListener).afterDelete(eq(Arrays.asList(toBeDeleted)));
	}
	
	@Test
	void testDeleteById() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Long> primaryKey = totoTable.addColumn("a", Long.class).primaryKey();
		Column<TotoTable, Long> columnB = totoTable.addColumn("b", Long.class);
		IReversibleAccessor<Toto, Long> identifier = Accessors.accessorByField(Toto.class, "a");
		IReversibleAccessor<Toto, Long> propB = Accessors.accessorByField(Toto.class, "b");
		// we must add a property to let us set some differences between 2 instances and have them detected by the system
		Map<? extends IReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps
				.asMap(identifier, primaryKey)
				.add(propB, columnB);
		ClassMappingStrategy<Toto, Long, TotoTable> classMappingStrategy = new ClassMappingStrategy<>(Toto.class, totoTable,
				mapping, identifier,
				new AlreadyAssignedIdentifierManager<>(Long.class, c -> {}, c -> false));
		Persister<Toto, Long, TotoTable> testInstance = new Persister<Toto, Long, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected int doDeleteById(Iterable entities) {
				return ((Collection) entities).size();
			}
		};
		
		DeleteByIdListener deleteListener = mock(DeleteByIdListener.class);
		testInstance.getPersisterListener().addDeleteByIdListener(deleteListener);
		
		// when nothing to be deleted, listener is not invoked
		int rowCount = testInstance.deleteById(Arrays.asList());
		assertThat(rowCount).isEqualTo(0);
		verifyNoMoreInteractions(deleteListener);
		Toto toBeDeleted = new Toto(1, 2, 3);
		
		rowCount = testInstance.deleteById(toBeDeleted);
		assertThat(rowCount).isEqualTo(1);
		verify(deleteListener).beforeDeleteById(eq(Arrays.asList(toBeDeleted)));
		verify(deleteListener).afterDeleteById(eq(Arrays.asList(toBeDeleted)));
	}
	
	@Test
	void testSelect() {
		TotoTable totoTable = new TotoTable("TotoTable");
		Column<TotoTable, Integer> primaryKey = totoTable.addColumn("a", Integer.class).primaryKey();
		IReversibleAccessor<Toto, Integer> identifier = Accessors.accessorByField(Toto.class, "a");
		Map<? extends IReversibleAccessor<Toto, Object>, Column<TotoTable, Object>> mapping = (Map) Maps.asMap(identifier, primaryKey);
		IdentifierInsertionManager<Toto, Integer> identifierInsertionManagerMock = mock(IdentifierInsertionManager.class);
		when(identifierInsertionManagerMock.getIdentifierType()).thenReturn(Integer.class);
		InsertListener identifierManagerInsertListenerMock = mock(InsertListener.class);
		when(identifierInsertionManagerMock.getInsertListener()).thenReturn(identifierManagerInsertListenerMock);
		SelectListener identifierManagerSelectListenerMock = mock(SelectListener.class);
		when(identifierInsertionManagerMock.getSelectListener()).thenReturn(identifierManagerSelectListenerMock);
		ClassMappingStrategy<Toto, Integer, TotoTable> classMappingStrategy = new ClassMappingStrategy<>(Toto.class, totoTable,
				mapping, identifier,
				identifierInsertionManagerMock);
		Persister<Toto, Integer, TotoTable> testInstance = new Persister<Toto, Integer, TotoTable>(classMappingStrategy, new Dialect(),
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 0)) {
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected int doInsert(Iterable entities) {
				return ((Collection) entities).size();
			}
			
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected int doUpdate(Iterable<? extends Duo<Toto, Toto>> entities, boolean allColumnsStatement) {
				return ((Collection) entities).size();
			}
			
			/** Overriden to prevent from building real world SQL statement because ConnectionProvider is mocked */
			@Override
			protected List<Toto> doSelect(Iterable<Integer> ids) {
				return Iterables.collectToList(ids, id -> new Toto(id, null, null));
			}
		};
		
		Toto selectedToto = testInstance.select(1);
		verify(identifierManagerSelectListenerMock).beforeSelect(eq(Arrays.asSet(1)));
		verify(identifierManagerSelectListenerMock).afterSelect(eq(Arrays.asList(selectedToto)));
	}
}