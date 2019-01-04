package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.List;

import org.gama.lang.Retryer;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.ParameterBinderIndex;
import org.gama.stalactite.persistence.engine.AssociationRecordPersister;
import org.gama.stalactite.persistence.engine.IndexedAssociationRecord;
import org.gama.stalactite.persistence.engine.runtime.AssociationRecordInsertionCascaderTest.Key;
import org.gama.stalactite.persistence.engine.runtime.AssociationRecordInsertionCascaderTest.Keyboard;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class IndexedAssociationRecordInsertionCascaderTest {
	
	@Test
	void testIndexedAssociationRecordInsertionCascader_getTargets() {
		ClassMappingStrategy classMappingStrategyMock = mock(ClassMappingStrategy.class);
		when(classMappingStrategyMock.getIdMappingStrategy()).thenReturn(mock(IdMappingStrategy.class));
		AssociationRecordPersister persisterStub =
				new AssociationRecordPersister(classMappingStrategyMock, mock(ConnectionProvider.class),
				new DMLGenerator(mock(ParameterBinderIndex.class)), Retryer.NO_RETRY, 1, 1);
		IndexedAssociationRecordInsertionCascader<Keyboard, Key, List<Key>> testInstance
				= new IndexedAssociationRecordInsertionCascader<>(persisterStub, Keyboard::getKeys);
		
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
				new IndexedAssociationRecord(inputData.getId(), key1.getId(), 0),
				new IndexedAssociationRecord(inputData.getId(), key2.getId(), 1),
				new IndexedAssociationRecord(inputData.getId(), key2.getId(), 2),
				new IndexedAssociationRecord(inputData.getId(), key3.getId(), 3)
		);
		assertEquals(printIndexedAssociationRecord(expectedResult), printIndexedAssociationRecord(targets));
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