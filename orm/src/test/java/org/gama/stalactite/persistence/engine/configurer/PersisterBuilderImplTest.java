package org.gama.stalactite.persistence.engine.configurer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.KeepOrderSet;
import org.gama.lang.collection.Maps;
import org.gama.lang.test.Assertions;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable;
import org.gama.stalactite.persistence.engine.model.AbstractVehicle;
import org.gama.stalactite.persistence.engine.model.Car;
import org.gama.stalactite.persistence.engine.model.Color;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.engine.model.Truk;
import org.gama.stalactite.persistence.engine.model.Vehicle;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.LambdaParameterBinder;
import org.gama.stalactite.sql.binder.NullAwareParameterBinder;
import org.gama.stalactite.sql.result.InMemoryResultSet;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;

import static org.gama.lang.function.Functions.chain;
import static org.gama.lang.function.Functions.link;
import static org.gama.lang.test.Assertions.assertAllEquals;
import static org.gama.lang.test.Assertions.assertEquals;
import static org.gama.lang.test.Assertions.hasMessage;
import static org.gama.reflection.Accessors.accessorByMethod;
import static org.gama.reflection.Accessors.accessorByMethodReference;
import static org.gama.reflection.Accessors.mutatorByField;
import static org.gama.reflection.Accessors.mutatorByMethodReference;
import static org.gama.stalactite.persistence.engine.MappingEase.embeddableBuilder;
import static org.gama.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.gama.stalactite.persistence.engine.MappingEase.subentityBuilder;
import static org.gama.stalactite.persistence.id.Identifier.identifierBinder;
import static org.gama.stalactite.sql.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.gama.stalactite.sql.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * @author Guillaume Mary
 */
public class PersisterBuilderImplTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Color.class, "int");
		
		DIALECT.getDmlGenerator().sortColumnsAlphabetically();	// for steady checks on SQL orders
	}
	
	@BeforeEach
	void initEntityCandidates() {
		PersisterBuilderImpl.ENTITY_CANDIDATES.set(new HashSet<>());
	}
	
	@AfterEach
	void removeEntityCandidates() {
		PersisterBuilderImpl.ENTITY_CANDIDATES.remove();
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_fromMappedSuperClasses() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(Car::getModel)
						.mapSuperClass(embeddableBuilder(AbstractVehicle.class)
								.embed(AbstractVehicle::getTimestamp)
								)
						);
		
		Table dummyTable = new Table("Car");
		testInstance.setColumnBinderRegistry(DIALECT.getColumnBinderRegistry())
				.setColumnNamingStrategy(ColumnNamingStrategy.DEFAULT)
				.setTable(dummyTable)
				.mapEntityConfigurationPerTable();
		
		MappingPerTable map = testInstance.collectEmbeddedMappingFromInheritance();
		
		// NB: containsOnly() doesn't work : returns false whereas result is good
		// (probably due to ValueAccessPoint Comparator not used by containsOnly() method)
		ArrayList<Entry<IReversibleAccessor, Column>> expected = new ArrayList<>(Maps
				.forHashMap(IReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getId), mutatorByField(Car.class, "id")),
						dummyTable.getColumn("id"))
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getModel), mutatorByField(Car.class, "model")),
						dummyTable.getColumn("model"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								accessorByMethod(Timestamp.class, "creationDate")),
						dummyTable.getColumn("creationDate"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								accessorByMethod(Timestamp.class, "modificationDate")),
						dummyTable.getColumn("modificationDate"))
				.entrySet());
		expected.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		ArrayList<Entry<IReversibleAccessor, Column>> actual = new ArrayList<>(map.giveMapping(dummyTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		// Objects are similar but not equals so we compare them throught their footprint (truely comparing them is quite hard)
		assertAllEquals(expected, actual, Object::toString);
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_fromInheritedClasses() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getModel)
						.mapInheritance(entityBuilder(AbstractVehicle.class, Identifier.class)
								.add(AbstractVehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
								.embed(AbstractVehicle::getTimestamp)
						)
		);
		
		Table dummyTable = new Table("Car");
		testInstance.setColumnBinderRegistry(DIALECT.getColumnBinderRegistry())
				.setColumnNamingStrategy(ColumnNamingStrategy.DEFAULT)
				.setTable(dummyTable)
				.mapEntityConfigurationPerTable();
		
		MappingPerTable map = testInstance.collectEmbeddedMappingFromInheritance();
		
		// NB: AssertJ containsOnly() doesn't work : returns false whereas result is good
		// (probably due to ValueAccessPoint Comparator not used by containsOnly() method)
		ArrayList<Entry<IReversibleAccessor, Column>> expected = new ArrayList<>(Maps
				.forHashMap(IReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getId), mutatorByField(Car.class, "id")),
						dummyTable.getColumn("id"))
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getModel), mutatorByField(Car.class, "model")),
						dummyTable.getColumn("model"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								accessorByMethod(Timestamp.class, "creationDate")),
						dummyTable.getColumn("creationDate"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								accessorByMethod(Timestamp.class, "modificationDate")),
						dummyTable.getColumn("modificationDate"))
				.entrySet());
		expected.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertAllEquals(Arrays.asSet(new Table("Car")), map.giveTables(), Table::getAbsoluteName);
		ArrayList<Entry<IReversibleAccessor, Column>> actual = new ArrayList<>(map.giveMapping(dummyTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		// Objects are similar but not equals so we compare them throught their footprint (truely comparing them is quite hard)
		assertAllEquals(expected, actual, Object::toString);
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_fromInheritedClasses_withJoinedTables() {
		Table carTable = new Table("Car");
		Table vehicleTable = new Table("Vehicle");
		Table abstractVehicleTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getModel)
						.mapInheritance(entityBuilder(Vehicle.class, Identifier.class)
								.embed(Vehicle::getColor)
								.mapInheritance(entityBuilder(AbstractVehicle.class, Identifier.class)
										.add(AbstractVehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
										.embed(AbstractVehicle::getTimestamp))
								// AbstractVehicle class doesn't get a Table, for testing purpose
								.withJoinedTable())
						// Vehicle class does get a Table, for testing purpose
						.withJoinedTable(vehicleTable));
		
		
		testInstance.setColumnBinderRegistry(DIALECT.getColumnBinderRegistry())
				.setColumnNamingStrategy(ColumnNamingStrategy.DEFAULT)
				.setTable(carTable)
				.setTableNamingStrategy(TableNamingStrategy.DEFAULT)
				.mapEntityConfigurationPerTable();
		
		MappingPerTable map = testInstance.collectEmbeddedMappingFromInheritance();
		
		assertAllEquals(Arrays.asHashSet(carTable, vehicleTable, abstractVehicleTable), map.giveTables(), Table::getAbsoluteName);
		// NB: AssertJ containsOnly() doesn't work : returns false whereas result is good
		// (probably due to ValueAccessPoint Comparator not used by containsOnly() method)
		// Checking Car mapping
		List<Entry<IReversibleAccessor, Column>> expectedCarMapping = new ArrayList<>(Maps
				.forHashMap(IReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getModel), mutatorByField(Car.class, "model")),
						carTable.getColumn("model"))
				.entrySet());
		expectedCarMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		List<Entry<IReversibleAccessor, Column>> actualCarMapping = new ArrayList<>(map.giveMapping(carTable).entrySet());
		actualCarMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		// Objects are similar but not equals so we compare them throught their footprint (truely comparing them is quite hard)
		assertAllEquals(expectedCarMapping, actualCarMapping, Object::toString);
		
		// Checking Vehicle mapping
		List<Entry<IReversibleAccessor, Column>> expectedVehicleMapping = new ArrayList<>(Maps
				.forHashMap(IReversibleAccessor.class, Column.class)
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Vehicle::getColor), mutatorByMethodReference(Vehicle::setColor)),
								accessorByMethod(Color.class, "rgb")),
						vehicleTable.getColumn("rgb"))
				.entrySet());
		expectedVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		List<Entry<IReversibleAccessor, Column>> actualVehicleMapping = new ArrayList<>(map.giveMapping(vehicleTable).entrySet());
		actualVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		// Objects are similar but not equals so we compare them throught their footprint (truely comparing them is quite hard)
		assertAllEquals(expectedVehicleMapping, actualVehicleMapping, Object::toString);
		
		// Checking AbstractVehicle mapping
		// we get the table instance created by builder because our (the one of this test) is only one with same name but without columns because
		// it wasn't given at mapping definition time
		abstractVehicleTable = Iterables.find(map.giveTables(), abstractVehicleTable::equals);
		List<Entry<IReversibleAccessor, Column>> expectedAbstractVehicleMapping = new ArrayList<>(Maps
				.forHashMap(IReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(AbstractVehicle::getId), mutatorByField(Car.class, "id")),
						abstractVehicleTable.getColumn("id"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(AbstractVehicle::getTimestamp), mutatorByMethodReference(AbstractVehicle::setTimestamp)),
								accessorByMethod(Timestamp.class, "creationDate")),
						abstractVehicleTable.getColumn("creationDate"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(AbstractVehicle::getTimestamp), mutatorByMethodReference(AbstractVehicle::setTimestamp)),
								accessorByMethod(Timestamp.class, "modificationDate")),
						abstractVehicleTable.getColumn("modificationDate"))
				.entrySet());
		expectedAbstractVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		List<Entry<IReversibleAccessor, Column>> actualAbstractVehicleMapping = new ArrayList<>(map.giveMapping(abstractVehicleTable).entrySet());
		actualAbstractVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		// Objects are similar but not equals so we compare them throught their footprint (truely comparing them is quite hard)
		assertAllEquals(expectedAbstractVehicleMapping, actualAbstractVehicleMapping, Object::toString);
		
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_withoutHierarchy() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(Car::getModel)
						.embed(AbstractVehicle::getTimestamp)
						);
		
		Table dummyTable = new Table("Car");
		testInstance.setColumnBinderRegistry(DIALECT.getColumnBinderRegistry())
				.setColumnNamingStrategy(ColumnNamingStrategy.DEFAULT)
				.setTable(dummyTable)
				.mapEntityConfigurationPerTable();
		
		MappingPerTable map = testInstance.collectEmbeddedMappingFromInheritance();
		
		// NB: containsOnly() doesn't work : returns false whereas result is good
		// (probably due to ValueAccessPoint Comparator not used by containsOnly() method)
		ArrayList<Entry<IReversibleAccessor, Column>> expected = new ArrayList<>(Maps
				.forHashMap(IReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getId), mutatorByField(Car.class, "id")),
						dummyTable.getColumn("id"))
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getModel), mutatorByField(Car.class, "model")),
						dummyTable.getColumn("model"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								accessorByMethod(Timestamp.class, "creationDate")),
						dummyTable.getColumn("creationDate"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(Car::getTimestamp), mutatorByMethodReference(Car::setTimestamp)),
								accessorByMethod(Timestamp.class, "modificationDate")),
						dummyTable.getColumn("modificationDate"))
				.entrySet());
		expected.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		ArrayList<Entry<IReversibleAccessor, Column>> actual = new ArrayList<>(map.giveMapping(dummyTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		// Objects are similar but not equals so we compare them throught their footprint (truely comparing them is quite hard)
		assertAllEquals(expected, actual, Object::toString);
	}
	
	@Test
	void collectEmbeddedMappingFromSubEntities() {
		Table dummyCarTable = new Table("dummyCarTable");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(AbstractVehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.embed(AbstractVehicle::getTimestamp)
						.mapPolymorphism(PolymorphismPolicy.joinedTables()
								.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel), dummyCarTable)
								.addSubClass(subentityBuilder(Truk.class)
									.add(Truk::getEngine))
						));
		
		Table dummyTable = new Table("dummyTable");
		testInstance.setColumnBinderRegistry(DIALECT.getColumnBinderRegistry())
				.setColumnNamingStrategy(ColumnNamingStrategy.DEFAULT)
				.setTableNamingStrategy(TableNamingStrategy.DEFAULT)
				.setTable(dummyTable);
		
		MappingPerTable mappingFromSubEntities = testInstance.collectEmbeddedMappingFromSubEntities();
		Table trukTable = Iterables.find(mappingFromSubEntities.giveTables(), t -> "Truk".equals(t.getName()));
		
		// NB: containsOnly() doesn't work : returns false whereas result is good
		// (probably due to ValueAccessPoint Comparator not used by containsOnly() method)
		ArrayList<Entry<IReversibleAccessor, Column>> expected = new ArrayList<>(Maps
				.forHashMap(IReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getModel), mutatorByField(Car.class, "model")),
						dummyCarTable.getColumn("model"))
				.entrySet());
		expected.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		ArrayList<Entry<IReversibleAccessor, Column>> actual = new ArrayList<>(mappingFromSubEntities.giveMapping(dummyCarTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		// Objects are similar but not equals so we compare them throught their footprint (truely comparing them is quite hard)
		assertAllEquals(expected, actual, Object::toString);
		
		ArrayList<Entry<IReversibleAccessor, Column>> expected2 = new ArrayList<>(Maps
				.forHashMap(IReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Truk::getEngine), mutatorByField(Truk.class, "engine")),
						trukTable.getColumn("engine"))
				.entrySet());
		expected2.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		ArrayList<Entry<IReversibleAccessor, Column>> actual2 = new ArrayList<>(mappingFromSubEntities.giveMapping(trukTable).entrySet());
		actual2.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		// Objects are similar but not equals so we compare them throught their footprint (truely comparing them is quite hard)
		assertAllEquals(expected2, actual2, Object::toString);
	}
	
	@Test
	void addPrimaryKeys() {
		EntityMappingConfiguration<AbstractVehicle, Identifier> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.class)
				.add(AbstractVehicle::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.getConfiguration();
		
		Table mainTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration)
				.setTable(mainTable)
				.setColumnNamingStrategy(ColumnNamingStrategy.DEFAULT)
				.setForeignKeyNamingStrategy(ForeignKeyNamingStrategy.DEFAULT);
		testInstance.mapEntityConfigurationPerTable();
		
		Table tableB = new Table("Vehicle");
		Table tableC = new Table("Car");
		testInstance.addPrimarykeys(new Identification(identifyingConfiguration), Arrays.asSet(mainTable, tableB, tableC));
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, (Function<Class, String>) Reflections::toString));  
		Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
				ForeignKey::getName,
				link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
				link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
		assertAllEquals(Arrays.asList(mainTable.getColumn("id")), mainTable.getPrimaryKey().getColumns(), columnPrinter);
		assertEquals(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isAutoGenerated(), true);
		assertAllEquals(Arrays.asList(tableB.getColumn("id")), tableB.getPrimaryKey().getColumns(), columnPrinter);
		assertEquals(Iterables.first((Set<Column>) tableB.getPrimaryKey().getColumns()).isAutoGenerated(), false);
		assertAllEquals(Arrays.asList(tableC.getColumn("id")), tableC.getPrimaryKey().getColumns(), columnPrinter);
		assertEquals(Iterables.first((Set<Column>) tableC.getPrimaryKey().getColumns()).isAutoGenerated(), false);
	}
	
	@Test
	void addForeignKeys() {
		EntityMappingConfiguration<AbstractVehicle, Identifier> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.class)
				.add(AbstractVehicle::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.getConfiguration();
		
		Table mainTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration)
				.setTable(mainTable)
				.setColumnNamingStrategy(ColumnNamingStrategy.DEFAULT)
				.setForeignKeyNamingStrategy(ForeignKeyNamingStrategy.DEFAULT);
		testInstance.mapEntityConfigurationPerTable();
		
		Table tableB = new Table("Vehicle");
		Table tableC = new Table("Car");
		testInstance.addPrimarykeys(new Identification(identifyingConfiguration), Arrays.asSet(mainTable, tableB, tableC));
		testInstance.addForeignKeys(new Identification(identifyingConfiguration), Arrays.asSet(mainTable, tableB, tableC));
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, (Function<Class, String>) Reflections::toString));  
		Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
				ForeignKey::getName,
				link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
				link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
//		assertAllEquals(Arrays.asList(mainTable.getColumn("id")), mainTable.getPrimaryKey().getColumns(), columnPrinter);
//		assertEquals(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isAutoGenerated(), true);
//		assertAllEquals(Arrays.asList(tableB.getColumn("id")), tableB.getPrimaryKey().getColumns(), columnPrinter);
//		assertEquals(Iterables.first((Set<Column>) tableB.getPrimaryKey().getColumns()).isAutoGenerated(), false);
//		assertAllEquals(Arrays.asList(tableC.getColumn("id")), tableC.getPrimaryKey().getColumns(), columnPrinter);
//		assertEquals(Iterables.first((Set<Column>) tableC.getPrimaryKey().getColumns()).isAutoGenerated(), false);
		assertAllEquals(Arrays.asList(new ForeignKey("FK_Vehicle_id_AbstractVehicle_id", tableB.getColumn("id"), mainTable.getColumn("id"))), tableB.getForeignKeys(),
				fkPrinter);
		assertAllEquals(Arrays.asList(new ForeignKey("FK_Car_id_Vehicle_id", tableC.getColumn("id"), tableB.getColumn("id"))), tableC.getForeignKeys(),
				fkPrinter);
	}
	
	@Test
	void addPrimaryKeys_pkTableNotFound_throwsException() {
		EntityMappingConfiguration<AbstractVehicle, Identifier> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.class)
				.add(AbstractVehicle::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.getConfiguration();
		
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration);
		// fixing TableNamingStrategy because error message needs it
		testInstance.setTableNamingStrategy(TableNamingStrategy.DEFAULT);
		
		Table tableA = new Table("A");
		Table tableB = new Table("B");
		Table tableC = new Table("C");
		Assertions.assertThrows(() -> testInstance.addPrimarykeys(new Identification(identifyingConfiguration), Arrays.asSet(tableA, tableB, tableC)), 
				Assertions.hasExceptionInCauses(IllegalArgumentException.class)
						.andProjection(hasMessage("Table for primary key wasn't found in given tables : looking for AbstractVehicle in [A, B, C]")));
	}
	
	@Test
	void build_singleTable_singleClass() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getModel)
								.add(Vehicle::getColor)
										.add(AbstractVehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
										.embed(AbstractVehicle::getTimestamp)
						);
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		IEntityPersister<Car, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
		Car entity = new Car(1L);
		entity.setModel("Renault");
		entity.setColor(new Color(123));
		entity.setTimestamp(new Timestamp());
		result.insert(entity);
	}
	
	@Test
	void build_singleTable_multipleClasses() throws SQLException {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getModel)
						.mapInheritance(entityBuilder(Vehicle.class, Identifier.class)
								.add(Vehicle::getColor)
								.mapInheritance(entityBuilder(AbstractVehicle.class, Identifier.class)
										.add(AbstractVehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
										.embed(AbstractVehicle::getTimestamp)
								)
								)
						);
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		Connection connectionMock = mock(Connection.class);
		when(connectionProviderMock.getCurrentConnection()).thenReturn(connectionMock);
		ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatementMock);
		when(preparedStatementMock.executeBatch()).thenReturn(new int[] { 1 });
		IEntityPersister<Car, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
		Car entity = new Car(1L);
		entity.setModel("Renault");
		entity.setColor(new Color(123));
		entity.setTimestamp(new Timestamp());
		result.insert(entity);
		assertEquals(Arrays.asList("insert into Car(color, creationDate, id, model, modificationDate) values (?, ?, ?, ?, ?)"), sqlCaptor.getAllValues());
	}
	
	@Test
	void build_joinedTables() throws SQLException {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getModel)
						.mapInheritance(entityBuilder(Vehicle.class, Identifier.class)
								.add(Vehicle::getColor)
								.mapInheritance(entityBuilder(AbstractVehicle.class, Identifier.class)
										.add(AbstractVehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
										.embed(AbstractVehicle::getTimestamp)
								).withJoinedTable()
								).withJoinedTable()
						);
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		Connection connectionMock = mock(Connection.class);
		when(connectionProviderMock.getCurrentConnection()).thenReturn(connectionMock);
		ArgumentCaptor<String> insertCaptor = ArgumentCaptor.forClass(String.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(insertCaptor.capture())).thenReturn(preparedStatementMock);
		when(preparedStatementMock.executeBatch()).thenReturn(new int[] { 1 });
		when(preparedStatementMock.executeQuery()).thenReturn(new InMemoryResultSet(Collections.emptyIterator()));
		
		IEntityPersister<Car, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
		Car entity = new Car(1L);
		entity.setModel("Renault");
		entity.setColor(new Color(123));
		entity.setTimestamp(new Timestamp());
		result.insert(entity);
		assertEquals(Arrays.asList(
				"insert into AbstractVehicle(creationDate, id, modificationDate) values (?, ?, ?)",
				"insert into Vehicle(color, id) values (?, ?)",
				"insert into Car(id, model) values (?, ?)"), insertCaptor.getAllValues());
		
		ArgumentCaptor<String> deleteCaptor = ArgumentCaptor.forClass(String.class);
		when(connectionMock.prepareStatement(deleteCaptor.capture())).thenReturn(preparedStatementMock);
		result.delete(entity);
		assertEquals(Arrays.asList(
				"delete from Car where id = ?",
				"delete from Vehicle where id = ?",
				"delete from AbstractVehicle where id = ?"), deleteCaptor.getAllValues());
		
		ArgumentCaptor<String> selectCaptor = ArgumentCaptor.forClass(String.class);
		when(connectionMock.prepareStatement(selectCaptor.capture())).thenReturn(preparedStatementMock);
		result.select(entity.getId());
		assertEquals(Arrays.asList(
				// the expected select may change in future as we don't care about select order nor joins order, tested in case of huge regression
				"select" 
						+ " Car.model as Car_model," 
						+ " Car.id as Car_id,"
						+ " AbstractVehicle.creationDate as AbstractVehicle_creationDate,"
						+ " AbstractVehicle.modificationDate as AbstractVehicle_modificationDate,"
						+ " AbstractVehicle.id as AbstractVehicle_id," 
						+ " Vehicle.color as Vehicle_color," 
						+ " Vehicle.id as Vehicle_id" 
						+ " from Car inner join AbstractVehicle on Car.id = AbstractVehicle.id" 
						+ " inner join Vehicle on Car.id = Vehicle.id" 
						+ " where Car.id in (?)"), selectCaptor.getAllValues());
	}
	
	public static class ToStringBuilder<E> {
		
		public static <E> Function<E, String> of(String separator, Function<E, String> ... properties) {
			ToStringBuilder<E> result = new ToStringBuilder<>(separator);
			for (Function<E, String> property : properties) {
				result.with(property);
			}
			return result::toString;
		}
		
		public static <E> Function<? extends Iterable<E>, String> asSeveral(Function<E, String> mapper) {
			return coll -> new StringAppender() {
				@Override
				public StringAppender cat(Object s) {
					return super.cat(s instanceof String ? s : mapper.apply((E) s));
				}
			}.ccat(coll, "").wrap("{", "}").toString();
		}
		
		private final String separator;
		private final KeepOrderSet<Function<E, String>> mappers = new KeepOrderSet<>();
		
		private ToStringBuilder(String separator) {
			this.separator = separator;
		}
		
		ToStringBuilder<E> with(Function<E, String> mapper) {
			this.mappers.add(mapper);
			return this;
		}
		
		String toString(E object) {
			return new StringAppender().ccat(Iterables.collectToList(mappers, m -> m.apply(object)), separator).toString();
		}
	}
}