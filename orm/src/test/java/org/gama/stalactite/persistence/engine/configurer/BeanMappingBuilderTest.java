package org.gama.stalactite.persistence.engine.configurer;

import org.gama.lang.test.Assertions;
import org.gama.stalactite.persistence.engine.MappingConfigurationException;
import org.gama.stalactite.persistence.engine.MappingEase;
import org.gama.stalactite.persistence.engine.model.Color;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.engine.model.Vehicle;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.gama.stalactite.persistence.engine.MappingEase.embeddableBuilder;
import static org.junit.jupiter.api.Assertions.assertSame;

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
				.add(Color::getRgb))
				.override(Color::getRgb, colorTable);
		Table result = BeanMappingBuilder.giveTargetTable(vehicleObjectFluentEntityMappingConfigurationSupport.getPropertiesMapping());
		assertSame(expectedResult, result);
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
						.add(Person::getName)
						.add(Person::getVersion))
				.override(Person::getName, nameColumn)
				.override(Person::getVersion, versionColumn);
		Assertions.assertThrows(() -> BeanMappingBuilder.giveTargetTable(vehicleObjectFluentEntityMappingConfigurationSupport.getPropertiesMapping()),
				Assertions.hasExceptionInCauses(MappingConfigurationException.class)
						.andProjection(Assertions.hasMessage("Property override doesn't target main table : o.g.s.p.e.m.Person::getName")));
	}
	
	@Test
	void giveTargetTable_withImportedConfiguration() {
		Table expectedResult = new Table("MyOverridingTable");
		Column nameColumn = expectedResult.addColumn("myOverridingColumn", String.class);
		FluentEntityMappingConfigurationSupport<Vehicle, Object> vehicleObjectFluentEntityMappingConfigurationSupport =
				new FluentEntityMappingConfigurationSupport<>(Vehicle.class);
		vehicleObjectFluentEntityMappingConfigurationSupport.embed(Vehicle::getOwner, MappingEase.embeddableBuilder(Person.class)
				.add(Person::getName))
				.override(Person::getName, nameColumn);
		Table result = BeanMappingBuilder.giveTargetTable(vehicleObjectFluentEntityMappingConfigurationSupport.getPropertiesMapping());
		assertSame(expectedResult, result);
	}
	
}