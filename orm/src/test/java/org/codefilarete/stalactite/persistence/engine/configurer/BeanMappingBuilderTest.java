package org.codefilarete.stalactite.persistence.engine.configurer;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.stalactite.persistence.engine.MappingConfigurationException;
import org.codefilarete.stalactite.persistence.engine.MappingEase;
import org.codefilarete.stalactite.persistence.engine.model.Color;
import org.codefilarete.stalactite.persistence.engine.model.Person;
import org.codefilarete.stalactite.persistence.engine.model.Vehicle;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.persistence.engine.MappingEase.embeddableBuilder;

/**
 * @author Guillaume Mary
 */
class BeanMappingBuilderTest {
	
	@Test
	void giveTargetTable() {
		Table expectedResult = new Table("MyOverridingTable");
		Column colorTable = expectedResult.addColumn("myOverridingColumn", Integer.class);
		FluentEntityMappingConfigurationSupport<Vehicle, Object> vehicleObjectFluentEntityMappingConfigurationSupport =
				new FluentEntityMappingConfigurationSupport<>(Vehicle.class);
		vehicleObjectFluentEntityMappingConfigurationSupport.embed(Vehicle::getColor, embeddableBuilder(Color.class)
				.map(Color::getRgb))
				.override(Color::getRgb, colorTable);
		Table result = BeanMappingBuilder.giveTargetTable(vehicleObjectFluentEntityMappingConfigurationSupport.getPropertiesMapping());
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
		assertThatThrownBy(() -> BeanMappingBuilder.giveTargetTable(vehicleObjectFluentEntityMappingConfigurationSupport.getPropertiesMapping()))
				.extracting(t -> Exceptions.findExceptionInCauses(t, MappingConfigurationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Property override doesn't target main table : o.c.s.p.e.m.Person::getName");
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
		Table result = BeanMappingBuilder.giveTargetTable(vehicleObjectFluentEntityMappingConfigurationSupport.getPropertiesMapping());
		assertThat(result).isSameAs(expectedResult);
	}
	
}