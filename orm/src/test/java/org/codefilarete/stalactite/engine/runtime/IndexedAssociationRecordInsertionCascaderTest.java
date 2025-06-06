package org.codefilarete.stalactite.engine.runtime;

import java.util.Collection;
import java.util.List;

import org.codefilarete.stalactite.engine.runtime.AssociationRecordInsertionCascaderTest.Key;
import org.codefilarete.stalactite.engine.runtime.AssociationRecordInsertionCascaderTest.Keyboard;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class IndexedAssociationRecordInsertionCascaderTest {
	
	@Test
	void testIndexedAssociationRecordInsertionCascader_getTargets() {
		ClassMapping classMappingStrategyMock = mock(ClassMapping.class);
		when(classMappingStrategyMock.getId(any(Keyboard.class))).thenAnswer((Answer<Identifier<Long>>) invocation ->
				((Keyboard) invocation.getArgument(0)).getId());
		
		// mocking Persister action of adding identifier manager InsertListeners
		IdMapping idMappingStrategymock = mock(IdMapping.class);
		when(idMappingStrategymock.getIdentifierInsertionManager()).thenReturn(mock(IdentifierInsertionManager.class));
		when(classMappingStrategyMock.getIdMapping()).thenReturn(idMappingStrategymock);
		
		ClassMapping keyClassMappingStrategyMock = mock(ClassMapping.class);
		when(keyClassMappingStrategyMock.getId(any(Key.class))).thenAnswer((Answer<Identifier<Long>>) invocation ->
				((Key) invocation.getArgument(0)).getId());
		AssociationRecordPersister persisterStub =
				new AssociationRecordPersister(classMappingStrategyMock, new DefaultDialect(), new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 1));
		IndexedAssociationRecordInsertionCascader<Keyboard, Key, Identifier, Identifier, List<Key>> testInstance
				= new IndexedAssociationRecordInsertionCascader<>(persisterStub, Keyboard::getKeys, classMappingStrategyMock, keyClassMappingStrategyMock);
		
		Keyboard inputData = new Keyboard(1L);
		Key key1 = new Key(1L);
		Key key2 = new Key(2L);
		Key key3 = new Key(3L);
		inputData.getKeys().addAll(Arrays.asList(
				key1,
				key2,
				key2,
				key3
		));
		
		Collection<IndexedAssociationRecord> targets = testInstance.getTargets(inputData);
		List<IndexedAssociationRecord> expectedResult = Arrays.asList(
				new IndexedAssociationRecord(inputData.getId(), key1.getId(), 1),
				new IndexedAssociationRecord(inputData.getId(), key2.getId(), 2),
				new IndexedAssociationRecord(inputData.getId(), key2.getId(), 3),
				new IndexedAssociationRecord(inputData.getId(), key3.getId(), 4)
		);
		assertThat(printIndexedAssociationRecord(targets)).isEqualTo(printIndexedAssociationRecord(expectedResult));
	}
	
	private static String printIndexedAssociationRecord(Iterable<IndexedAssociationRecord> records) {
		StringAppender result = new StringAppender() {
			@Override
			public StringAppender cat(Object o) {
				if (o instanceof IndexedAssociationRecord) {
					return super.cat(print((IndexedAssociationRecord) o));
				} else {
					return super.cat(o);
				}
			}
		};
		result.ccat(records, ", ");
		return result.toString();
	}
	
	private static String print(IndexedAssociationRecord record) {
		return "{" + record.getLeft() + ", " + record.getIndex() + ", " + record.getRight() + "}";
	}
	
}