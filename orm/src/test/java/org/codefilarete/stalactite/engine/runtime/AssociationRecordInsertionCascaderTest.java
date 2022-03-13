package org.codefilarete.stalactite.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.ClassMappingStrategy;
import org.codefilarete.stalactite.mapping.IdMappingStrategy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class AssociationRecordInsertionCascaderTest {
	
	@Test
	void testAssociationRecordInsertionCascader_getTargets() {
		ClassMappingStrategy classMappingStrategyMock = mock(ClassMappingStrategy.class);
		when(classMappingStrategyMock.getId(any(Keyboard.class))).thenAnswer((Answer<Identifier<Long>>) invocation ->
				((Keyboard) invocation.getArgument(0)).getId());
		
		// mocking Persister action of adding identifier manager InsertListeners
		IdMappingStrategy idMappingStrategymock = mock(IdMappingStrategy.class);
		when(idMappingStrategymock.getIdentifierInsertionManager()).thenReturn(mock(IdentifierInsertionManager.class));
		when(classMappingStrategyMock.getIdMappingStrategy()).thenReturn(idMappingStrategymock);
		
		ClassMappingStrategy keyClassMappingStrategyMock = mock(ClassMappingStrategy.class);
		when(keyClassMappingStrategyMock.getId(any(Key.class))).thenAnswer((Answer<Identifier<Long>>) invocation ->
				((Key) invocation.getArgument(0)).getId());
		AssociationRecordPersister persisterStub =
				new AssociationRecordPersister(classMappingStrategyMock, new Dialect(), new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 1));
		AssociationRecordInsertionCascader<Keyboard, Key, Identifier, Identifier, List<Key>> testInstance
				= new AssociationRecordInsertionCascader<>(persisterStub, Keyboard::getKeys, classMappingStrategyMock, keyClassMappingStrategyMock);
		
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
		assertThat(printAssociationRecord(targets)).isEqualTo(printAssociationRecord(expectedResult));
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