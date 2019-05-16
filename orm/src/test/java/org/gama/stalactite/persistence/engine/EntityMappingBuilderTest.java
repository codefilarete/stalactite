package org.gama.stalactite.persistence.engine;

import org.gama.reflection.MethodReferenceCapturer;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.model.Gender;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.engine.model.PersonWithGender;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
class EntityMappingBuilderTest {
	
	@Test
	void enumMappedByColumn_columnIsNotInTargetClass_throwsException() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class,
				Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		
		Table<?> targetTable = new Table<>("person");
		Column<Table, Gender> unkownColumnInTargetTable = new Column<>(new Table<>("xx"), "aa", Gender.class);
		EntityMappingConfiguration<PersonWithGender, Identifier> configuration =
				FluentEntityMappingConfigurationSupport.from(PersonWithGender.class, Identifier.class)
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