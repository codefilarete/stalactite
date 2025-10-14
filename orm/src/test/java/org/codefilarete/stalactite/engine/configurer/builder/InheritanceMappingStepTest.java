package org.codefilarete.stalactite.engine.configurer.builder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.MappingPerTable;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.reflection.Accessors.accessorByMethodReference;
import static org.codefilarete.reflection.Accessors.mutatorByField;
import static org.codefilarete.reflection.Accessors.mutatorByMethodReference;
import static org.codefilarete.stalactite.dsl.MappingEase.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.MappingEase.entityBuilder;

class InheritanceMappingStepTest {
	
	@Test
	void collectEmbeddedMappingFromInheritance_fromMappedSuperClasses() {
		InheritanceMappingStep<Car, Identifier<Long>> testInstance = new InheritanceMappingStep<>();
		Table dummyTable = new Table("Car");
		FluentEntityMappingBuilder<Car, Identifier<Long>> entityMappingBuilder = entityBuilder(Car.class, Identifier.LONG_TYPE)
				.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Car::getModel)
				.mapSuperClass(embeddableBuilder(AbstractVehicle.class)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate)
						)
				);
		
		EntityMappingConfiguration<Car, Identifier<Long>> configuration = entityMappingBuilder.getConfiguration();
		MappingPerTable<Car> mappingPerTable = testInstance.collectPropertiesMappingFromInheritance(
				configuration,
				new TableMappingStep<Car, Identifier<Long>>().mapEntityConfigurationToTable(configuration, dummyTable, TableNamingStrategy.DEFAULT),
				new ColumnBinderRegistry(),
				ColumnNamingStrategy.DEFAULT
		);

		// NB: containsOnly() doesn't work : returns false whereas result is good
		// (probably due to ValueAccessPoint Comparator not used by containsOnly() method)
		ArrayList<Entry<ReversibleAccessor, Column>> expected = new ArrayList<>(Maps
				.forHashMap(ReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getModel), mutatorByField(Car.class, "model")),
						dummyTable.getColumn("model"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								new PropertyAccessor<>(accessorByMethodReference(Timestamp::getCreationDate), mutatorByMethodReference(Timestamp::setCreationDate))),
						dummyTable.getColumn("creationDate"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								new PropertyAccessor<>(accessorByMethodReference(Timestamp::getModificationDate), mutatorByMethodReference(Timestamp::setCreationDate))),
						dummyTable.getColumn("modificationDate"))
				.entrySet());
		expected.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		ArrayList<Entry<ReversibleAccessor, Column>> actual = new ArrayList<>(mappingPerTable.giveMapping(dummyTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actual)
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expected);
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_fromInheritedClasses() {
		InheritanceMappingStep<Car, Identifier<Long>> testInstance = new InheritanceMappingStep<>();
		FluentEntityMappingBuilder<Car, Identifier<Long>> entityMappingBuilder = entityBuilder(Car.class, Identifier.LONG_TYPE)
						.map(Car::getModel)
						.mapSuperClass(entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
								.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
								.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
										.map(Timestamp::getCreationDate)
										.map(Timestamp::getModificationDate))
						);

		Table dummyTable = new Table("Car");
		EntityMappingConfiguration<Car, Identifier<Long>> configuration = entityMappingBuilder.getConfiguration();
		MappingPerTable<Car> mappingPerTable = testInstance.collectPropertiesMappingFromInheritance(
				configuration,
				new TableMappingStep<Car, Identifier<Long>>().mapEntityConfigurationToTable(configuration, dummyTable, TableNamingStrategy.DEFAULT),
				new ColumnBinderRegistry(),
				ColumnNamingStrategy.DEFAULT
		);

		// NB: AssertJ containsOnly() doesn't work : returns false whereas result is good
		// (probably due to ValueAccessPoint Comparator not used by containsOnly() method)
		ArrayList<Entry<ReversibleAccessor, Column>> expected = new ArrayList<>(Maps
				.forHashMap(ReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getModel), mutatorByField(Car.class, "model")),
						dummyTable.getColumn("model"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								new PropertyAccessor<>(accessorByMethodReference(Timestamp::getCreationDate), mutatorByMethodReference(Timestamp::setCreationDate))),
						dummyTable.getColumn("creationDate"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								new PropertyAccessor<>(accessorByMethodReference(Timestamp::getModificationDate), mutatorByMethodReference(Timestamp::setCreationDate))),
						dummyTable.getColumn("modificationDate"))
				.entrySet());
		expected.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(mappingPerTable.giveTables())
				.extracting(Table::getAbsoluteName)
				.containsExactly("Car");
		ArrayList<Entry<ReversibleAccessor, Column>> actual = new ArrayList<>(mappingPerTable.giveMapping(dummyTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actual)
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expected);
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_fromInheritedClasses_withJoinedTables() {
		Table carTable = new Table("Car");
		Table vehicleTable = new Table("Vehicle");
		Table abstractVehicleTable = new Table("AbstractVehicle");
		InheritanceMappingStep<Car, Identifier<Long>> testInstance = new InheritanceMappingStep<>();
		FluentEntityMappingBuilder<Car, Identifier<Long>> entityMappingBuilder =
				entityBuilder(Car.class, Identifier.LONG_TYPE)
						.map(Car::getModel)
						.mapSuperClass(entityBuilder(Vehicle.class, Identifier.LONG_TYPE)
								.embed(Vehicle::getColor, embeddableBuilder(Color.class)
										.map(Color::getRgb))
								.mapSuperClass(entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
										.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
										.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
												.map(Timestamp::getCreationDate)
												.map(Timestamp::getModificationDate)))
								// AbstractVehicle class doesn't get a Table, for testing purpose
								.withJoinedTable())
						// Vehicle class does get a Table, for testing purpose
						.withJoinedTable(vehicleTable);

		EntityMappingConfiguration<Car, Identifier<Long>> configuration = entityMappingBuilder.getConfiguration();
		MappingPerTable<Car> mappingPerTable = testInstance.collectPropertiesMappingFromInheritance(
				configuration,
				new TableMappingStep<Car, Identifier<Long>>().mapEntityConfigurationToTable(configuration, carTable, TableNamingStrategy.DEFAULT),
				new ColumnBinderRegistry(),
				ColumnNamingStrategy.DEFAULT
		);

		assertThat(mappingPerTable.giveTables())
				.usingElementComparator(Comparator.comparing(Table::getAbsoluteName))
				.containsExactly(carTable, vehicleTable, abstractVehicleTable);
		// NB: AssertJ containsOnly() doesn't work : returns false whereas result is good
		// (probably due to ValueAccessPoint Comparator not used by containsOnly() method)
		// Checking Car mapping
		List<Entry<ReversibleAccessor, Column>> expectedCarMapping = new ArrayList<>(Maps
				.forHashMap(ReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getModel), mutatorByField(Car.class, "model")),
						carTable.getColumn("model"))
				.entrySet());
		expectedCarMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		List<Entry<ReversibleAccessor, Column>> actualCarMapping = new ArrayList<>(mappingPerTable.giveMapping(carTable).entrySet());
		actualCarMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actualCarMapping)
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expectedCarMapping);

		// Checking Vehicle mapping
		List<Entry<ReversibleAccessor, Column>> expectedVehicleMapping = new ArrayList<>(Maps
				.forHashMap(ReversibleAccessor.class, Column.class)
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Vehicle::getColor), mutatorByMethodReference(Vehicle::setColor)),
								new PropertyAccessor<>(accessorByMethodReference(Color::getRgb), mutatorByField(Color.class, "rgb"))),
						vehicleTable.getColumn("rgb"))
				.entrySet());
		expectedVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		List<Entry<ReversibleAccessor, Column>> actualVehicleMapping = new ArrayList<>(mappingPerTable.giveMapping(vehicleTable).entrySet());
		actualVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actualVehicleMapping)
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expectedVehicleMapping);

		// Checking AbstractVehicle mapping
		// we get the table instance created by builder because our (the one of this test) is only one with same name but without columns because
		// it wasn't given at mapping definition time
		abstractVehicleTable = Iterables.find(mappingPerTable.giveTables(), table -> table.getName().equalsIgnoreCase("AbstractVehicle"));
		List<Entry<ReversibleAccessor, Column>> expectedAbstractVehicleMapping = new ArrayList<>(Maps
				.forHashMap(ReversibleAccessor.class, Column.class)
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(AbstractVehicle::getTimestamp), mutatorByMethodReference(AbstractVehicle::setTimestamp)),
						new PropertyAccessor<>(accessorByMethodReference(Timestamp::getCreationDate), mutatorByMethodReference(Timestamp::setCreationDate))),
						abstractVehicleTable.getColumn("creationDate"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(AbstractVehicle::getTimestamp), mutatorByMethodReference(AbstractVehicle::setTimestamp)),
						new PropertyAccessor<>(accessorByMethodReference(Timestamp::getModificationDate), mutatorByMethodReference(Timestamp::setModificationDate))),
						abstractVehicleTable.getColumn("modificationDate"))
				.entrySet());
		expectedAbstractVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		List<Entry<ReversibleAccessor, Column>> actualAbstractVehicleMapping = new ArrayList<>(mappingPerTable.giveMapping(abstractVehicleTable).entrySet());
		actualAbstractVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actualAbstractVehicleMapping)
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expectedAbstractVehicleMapping);
	}

	@Test
	void collectEmbeddedMappingFromInheritance_withoutHierarchy() {
		InheritanceMappingStep<Car, Identifier<Long>> testInstance = new InheritanceMappingStep<>();
		FluentEntityMappingBuilder<Car, Identifier<Long>> entityMappingBuilder = entityBuilder(Car.class, Identifier.LONG_TYPE)
						.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Car::getModel)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate));

		Table dummyTable = new Table("Car");
		EntityMappingConfiguration<Car, Identifier<Long>> configuration = entityMappingBuilder.getConfiguration();
		MappingPerTable<Car> mappingPerTable = testInstance.collectPropertiesMappingFromInheritance(
				configuration,
				new TableMappingStep<Car, Identifier<Long>>().mapEntityConfigurationToTable(configuration, dummyTable, TableNamingStrategy.DEFAULT),
				new ColumnBinderRegistry(),
				ColumnNamingStrategy.DEFAULT
		);

		// NB: containsOnly() doesn't work : returns false whereas result is good
		// (probably due to ValueAccessPoint Comparator not used by containsOnly() method)
		ArrayList<Entry<ReversibleAccessor, Column>> expected = new ArrayList<>(Maps
				.forHashMap(ReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getModel), mutatorByField(Car.class, "model")),
						dummyTable.getColumn("model"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								new PropertyAccessor<>(accessorByMethodReference(Timestamp::getCreationDate), mutatorByMethodReference(Timestamp::setCreationDate))),
						dummyTable.getColumn("creationDate"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								new PropertyAccessor<>(accessorByMethodReference(Timestamp::getModificationDate), mutatorByMethodReference(Timestamp::setCreationDate))),
						dummyTable.getColumn("modificationDate"))
				.entrySet());
		expected.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		ArrayList<Entry<ReversibleAccessor, Column>> actual = new ArrayList<>(mappingPerTable.giveMapping(dummyTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actual)
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expected);
	}
}