package org.gama.stalactite.persistence.engine;

import java.util.Collections;

import org.gama.lang.collection.Arrays;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport.EntityLinkageByColumnName;
import org.gama.stalactite.persistence.engine.WriteExecutor.JDBCBatchingIterator;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.model.Gender;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.engine.model.PersonWithGender;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.internal.stubbing.defaultanswers.ReturnsMocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntityMappingBuilderTest {
	
	@Test
	void build_invokeIdentifierManagerAfterInsertListener() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		
		// mocking Persister action of adding identifier manager InsertListeners
		IdentifierInsertionManager identifierInsertionManagerMock = mock(IdentifierInsertionManager.class);
		when(identifierInsertionManagerMock.getIdentifierType()).thenReturn(Identifier.class);
		JDBCBatchingIterator iteratorMock = mock(JDBCBatchingIterator.class);
		when(identifierInsertionManagerMock.buildJDBCBatchingIterator(any(), any(), anyInt())).thenReturn(iteratorMock);
		InsertListener insertListenerMock = mock(InsertListener.class);
		when(identifierInsertionManagerMock.getInsertListener()).thenReturn(insertListenerMock);
		
		IReversibleAccessor<Person, Identifier> identifierAccessor = Accessors.propertyAccessor(Person.class, "id");
		
		EmbeddableMappingConfiguration<Person> personPropertiesMapping = mock(EmbeddableMappingConfiguration.class);
		// declaring mapping
		when(personPropertiesMapping.getPropertiesMapping()).thenReturn(Arrays.asList(new EntityLinkageByColumnName<>(identifierAccessor, Identifier.class, "id")));
		// preventing NullPointerException
		when(personPropertiesMapping.getInsets()).thenReturn(Collections.emptyList());
		when(personPropertiesMapping.getColumnNamingStrategy()).thenReturn(ColumnNamingStrategy.DEFAULT);
		
		EntityMappingConfiguration<Person, Identifier<Long>> configuration = mock(EntityMappingConfiguration.class);
		// declaring mapping
		when(configuration.getPropertiesMapping()).thenReturn(personPropertiesMapping);
		when(configuration.getIdentifierInsertionManager()).thenReturn(identifierInsertionManagerMock);
		// preventing NullPointerException
		when(configuration.getPersistedClass()).thenReturn(Person.class);
		when(configuration.getTableNamingStrategy()).thenReturn(TableNamingStrategy.DEFAULT);
		when(configuration.getIdentifierAccessor()).thenReturn(identifierAccessor);
		when(configuration.getOneToOnes()).thenReturn(Collections.emptyList());
		when(configuration.getOneToManys()).thenReturn(Collections.emptyList());
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, new ReturnsMocks());
		Persister<Person, Identifier<Long>, Table> testInstance = new EntityMappingBuilder<>(configuration, new MethodReferenceCapturer())
				.build(new PersistenceContext(connectionProviderMock, dialect));
		Person person = new Person(new PersistableIdentifier<>(1L));
		testInstance.insert(person);
		verify(insertListenerMock).afterInsert(ArgumentMatchers.eq(Arrays.asList(person)));
	}
	
	@Test
	void addByColumn_columnIsNotInTargetClass_throwsException() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class,
				Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		
		Table<?> targetTable = new Table<>("person");
		Column<Table, String> unkownColumnInTargetTable = new Column<>(new Table<>("xx"), "aa", String.class);
		EntityMappingConfiguration<PersonWithGender, Identifier> configuration = FluentEntityMappingConfigurationSupport.from(PersonWithGender.class, Identifier.class)
						.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(Person::getName, unkownColumnInTargetTable)
						.getConfiguration();
		
		EntityMappingBuilder<PersonWithGender, Identifier> testInstance = new EntityMappingBuilder<>(configuration, new MethodReferenceCapturer());
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				testInstance.build(new PersistenceContext(mock(ConnectionProvider.class), dialect), targetTable)
		);
		assertEquals("Column specified for mapping Person::getName is not in target table : column xx.aa is not in table person",
				thrownException.getMessage());
	}
	
	@Test
	void enumMappedByColumn_columnIsNotInTargetClass_throwsException() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class,
				Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		
		Table<?> targetTable = new Table<>("person");
		Column<Table, Gender> unkownColumnInTargetTable = new Column<>(new Table<>("xx"), "aa", Gender.class);
		EntityMappingConfiguration<PersonWithGender, Identifier> configuration = FluentEntityMappingConfigurationSupport.from(PersonWithGender.class, Identifier.class)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName)
				.addEnum(PersonWithGender::getGender, unkownColumnInTargetTable).byOrdinal()
				.getConfiguration();
		
		EntityMappingBuilder<PersonWithGender, Identifier> testInstance = new EntityMappingBuilder<>(configuration, new MethodReferenceCapturer());
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				testInstance.build(new PersistenceContext(mock(ConnectionProvider.class), dialect), targetTable)
		);
		assertEquals("Column specified for mapping PersonWithGender::getGender is not in target table : column xx.aa is not in table person",
				thrownException.getMessage());
	}
}