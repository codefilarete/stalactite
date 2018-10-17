package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.gama.lang.Retryer;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.ParameterBinderIndex;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
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
class AssociationRecordInsertionCascaderTest {
	
	@Test
	void testAssociationRecordInsertionCascader_getTargets() {
		ClassMappingStrategy classMappingStrategyMock = mock(ClassMappingStrategy.class);
		when(classMappingStrategyMock.getIdMappingStrategy()).thenReturn(mock(IdMappingStrategy.class));
		AssociationRecordPersister persisterStub =
				new AssociationRecordPersister(classMappingStrategyMock, mock(ConnectionProvider.class),
				new DMLGenerator(mock(ParameterBinderIndex.class)), Retryer.NO_RETRY, 1, 1);
		AssociationRecordInsertionCascader<Keyboard, Key, List<Key>> testInstance
				= new AssociationRecordInsertionCascader<>(persisterStub, Keyboard::getKeys);
		
		Keyboard inputData = new Keyboard(1L);
		Key key1 = new Key(1L);
		Key key2 = new Key(2L);
		Key key3 = new Key(3L);
		inputData.getKeys().addAll(Arrays.asList(
				key1,
				key2,
				key3
		));
		
		Collection<AssociationRecord> targets = testInstance.getTargets(inputData);
		List<AssociationRecord> expectedResult = Arrays.asList(
				new AssociationRecord(inputData.getId(), key1.getId()),
				new AssociationRecord(inputData.getId(), key2.getId()),
				new AssociationRecord(inputData.getId(), key3.getId())
		);
		assertEquals(printAssociationRecord(expectedResult), printAssociationRecord(targets));
	}
	
	private static String printAssociationRecord(Iterable<AssociationRecord> records) {
		StringAppender result = new StringAppender() {
			@Override
			public StringAppender cat(Object o) {
				if (o instanceof AssociationRecord) {
					return super.cat(print((AssociationRecord) o));
				} else {
					return super.cat(o);
				}
			}
		};
		result.ccat(records, ", ");
		return result.toString();
	}
	
	private static String print(AssociationRecord record) {
		return "{" + record.getLeft() + ", " + record.getRight() + "}";
	}
	
	static class Keyboard implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private List<Key> keys = new ArrayList<>();
		
		Keyboard(Long id) {
			this.id = new PersistableIdentifier<>(id);
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public List<Key> getKeys() {
			return keys;
		}
		
		public void setKeys(List<Key> keys) {
			this.keys = keys;
		}
	}
	
	static class Key implements Identified<Long> {
		
		private Identifier<Long> id;
		
		Key(Long id) {
			this.id = new PersistableIdentifier<>(id);
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
	}
}