package org.codefilarete.stalactite.engine.configurer.builder.embeddable;

import java.util.Date;
import java.util.Set;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.MappingEase;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.UniqueConstraintNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.embeddable.FluentEmbeddableMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.entity.FluentEntityMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.DateBinder;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.dsl.MappingEase.embeddableBuilder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EmbeddableMappingBuilderTest {

	@Test
	void giveTargetTable() {
		Table expectedResult = new Table("MyOverridingTable");
		Column colorTable = expectedResult.addColumn("myOverridingColumn", Integer.class);
		FluentEntityMappingConfigurationSupport<Vehicle, Object> vehicleObjectFluentEntityMappingConfigurationSupport =
				new FluentEntityMappingConfigurationSupport<>(Vehicle.class);
		vehicleObjectFluentEntityMappingConfigurationSupport.embed(Vehicle::getColor, embeddableBuilder(Color.class)
				.map(Color::getRgb))
				.override(Color::getRgb, colorTable);
		Table result = EmbeddableMappingBuilder.giveTargetTable(vehicleObjectFluentEntityMappingConfigurationSupport.getPropertiesMapping());
		assertThat(result).isSameAs(expectedResult);
	}

	@Test
	void giveTargetTable_multipleTableFound_throwsException() {
		Table firstTable = new Table("MyOverridingTable");
		Column<?, String> nameColumn = firstTable.addColumn("myOverridingColumn", String.class);
		Table secondTable = new Table("MyOverridingTable2");
		Column<?, Integer> versionColumn = secondTable.addColumn("myOverridingColumn", Integer.class);
		FluentEntityMappingConfigurationSupport<Vehicle, Object> vehicleObjectFluentEntityMappingConfigurationSupport =
				new FluentEntityMappingConfigurationSupport<>(Vehicle.class);
		vehicleObjectFluentEntityMappingConfigurationSupport.embed(Vehicle::getOwner, embeddableBuilder(Person.class)
						.map(Person::getName)
						.map(Person::getVersion))
				.override(Person::getName, nameColumn)
				.override(Person::getVersion, versionColumn);
		assertThatThrownBy(() -> EmbeddableMappingBuilder.giveTargetTable(vehicleObjectFluentEntityMappingConfigurationSupport.getPropertiesMapping()))
				.extracting(t -> Exceptions.findExceptionInCauses(t, MappingConfigurationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Property o.c.s.e.m.Person::getName overrides column with MyOverridingTable.myOverridingColumn but it is not part of main table MyOverridingTable2");
	}

	@Test
	void giveTargetTable_withImportedConfiguration() {
		Table expectedResult = new Table("MyOverridingTable");
		Column nameColumn = expectedResult.addColumn("myOverridingColumn", String.class);
		FluentEntityMappingConfigurationSupport<Vehicle, Object> vehicleObjectFluentEntityMappingConfigurationSupport =
				new FluentEntityMappingConfigurationSupport<>(Vehicle.class);
		vehicleObjectFluentEntityMappingConfigurationSupport.embed(Vehicle::getOwner, MappingEase.embeddableBuilder(Person.class)
				.map(Person::getName))
				.override(Person::getName, nameColumn);
		Table result = EmbeddableMappingBuilder.giveTargetTable(vehicleObjectFluentEntityMappingConfigurationSupport.getPropertiesMapping());
		assertThat(result).isSameAs(expectedResult);
	}

	@Test
	void ensureColumnBindingInRegistry() {
		Table countryTable = new Table("Country");
		Column<?, Set> dummyColumn = countryTable.addColumn("dummyColumn", Set.class);
		EmbeddableMappingBuilder<Country, ?> testInstance = new EmbeddableMappingBuilder(
				new FluentEmbeddableMappingConfigurationSupport(Country.class),
				countryTable,
				new ColumnBinderRegistry(),
				ColumnNamingStrategy.DEFAULT,
				UniqueConstraintNamingStrategy.DEFAULT);
		EmbeddableMappingBuilder<Country, ?>.InternalProcessor internalProcessor = testInstance.new InternalProcessor(false);
		EmbeddableLinkage<Country, Set> linkageMock = mock(EmbeddableLinkage.class);
		when(linkageMock.getAccessor()).thenReturn(Accessors.accessor(Country::getCities));
		when(linkageMock.getColumnType()).thenReturn(Set.class);

		assertThatThrownBy(() -> internalProcessor.ensureColumnBindingInRegistry(linkageMock, dummyColumn))
				.isInstanceOf(Exception.class)
				.hasMessage("No binder found for property Country::getCities"
						+ " : neither its column nor its type are registered (Country.dummyColumn, type j.u.Set)");
	}

	@Nested
	class InternalProcessor {

		@Test
		void addColumnToTable_existingColumnDoesntMatchLinkageType_linkageAsParameterBinder_doesntThrowException() {
			// Given a table that has a column ...
			Table countryTable = new Table("Country");
			Column dummyColumn = countryTable.addColumn("dummyColumnName", String.class);
			EmbeddableMappingBuilder<Country, ?> testInstanceBuilder = new EmbeddableMappingBuilder(
					new FluentEmbeddableMappingConfigurationSupport(Country.class),
					countryTable,
					new ColumnBinderRegistry(),
					ColumnNamingStrategy.DEFAULT,
					UniqueConstraintNamingStrategy.DEFAULT);
			EmbeddableMappingBuilder.InternalProcessor testInstance = testInstanceBuilder.new InternalProcessor(false);

			// ... and a linkage that uses a different type
			EmbeddableLinkage linkageMock = mock(EmbeddableLinkage.class);
			when(linkageMock.getColumnType()).thenReturn(Date.class);
			// ... but with a SQL parameter binder that overrides its own "columnType" to match column one
			when(linkageMock.getParameterBinder()).thenReturn(new DateBinder() {
				@Override
				public <O> Class<O> getColumnType() {
					return (Class<O>) String.class;
				}
			});

			// When I add the linkage to the configurator for this column, then it doesn't fail with any exception
			assertThatCode(() -> testInstance.addColumnToTable(linkageMock, dummyColumn.getName(), dummyColumn.getSize())).doesNotThrowAnyException();
		}
		
		@Test
		void addColumnToTable_handlePrimitiveTypeAndNullability() {
			// Given a table that has a column ...
			Table countryTable = new Table("Country");
			EmbeddableMappingBuilder<Country, ?> testInstanceBuilder = new EmbeddableMappingBuilder(
					new FluentEmbeddableMappingConfigurationSupport(Country.class),
					countryTable,
					new ColumnBinderRegistry(),
					ColumnNamingStrategy.DEFAULT,
					UniqueConstraintNamingStrategy.DEFAULT);
			EmbeddableMappingBuilder.InternalProcessor testInstance = testInstanceBuilder.new InternalProcessor(false);
			
			// ... and a linkage that uses a different type
			EmbeddableLinkage linkageMock = mock(EmbeddableLinkage.class);
			
			// type is primitive and user didn't mention anything on nullability => we make it not nullable
			when(linkageMock.getColumnType()).thenReturn(int.class);
			when(linkageMock.isNullable()).thenReturn(null);
			Column intColumn = testInstance.addColumnToTable(linkageMock, "dummyName_int", null);
			assertThat(intColumn.isNullable()).isFalse();
			
			// type is Object and user didn't mention anything on nullability => we make it nullable
			when(linkageMock.getColumnType()).thenReturn(Integer.class);
			Column integerColumn = testInstance.addColumnToTable(linkageMock, "dummyName_integer", null);
			assertThat(integerColumn.isNullable()).isNull();
			
			// type is primitive but is overridden by user => we trust the user and make it as he asked
			when(linkageMock.getColumnType()).thenReturn(int.class);
			when(linkageMock.isNullable()).thenReturn(true);
			Column intNullableColumn = testInstance.addColumnToTable(linkageMock, "dummyName_int_nullable", null);
			assertThat(intNullableColumn.isNullable()).isTrue();
			
			// type is Object but is overridden by user => we trust the user and make it as he asked
			when(linkageMock.getColumnType()).thenReturn(Integer.class);
			when(linkageMock.isNullable()).thenReturn(false);
			Column integerNotNullableColumn = testInstance.addColumnToTable(linkageMock, "dummyName_integer_notNullable", null);
			assertThat(integerNotNullableColumn.isNullable()).isFalse();
		}
	}
}
