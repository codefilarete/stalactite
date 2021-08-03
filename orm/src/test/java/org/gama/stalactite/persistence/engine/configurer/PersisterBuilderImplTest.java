package org.gama.stalactite.persistence.engine.configurer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.KeepOrderSet;
import org.gama.lang.collection.Maps;
import org.gama.lang.exception.Exceptions;
import org.gama.lang.function.Serie.IntegerSerie;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.ReversibleAccessor;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.EntityPersister;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.InMemoryCounterIdentifierGenerator;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable;
import org.gama.stalactite.persistence.engine.model.AbstractVehicle;
import org.gama.stalactite.persistence.engine.model.Car;
import org.gama.stalactite.persistence.engine.model.Color;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.engine.model.Vehicle;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.PrimaryKey;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.LambdaParameterBinder;
import org.gama.stalactite.sql.binder.NullAwareParameterBinder;
import org.gama.stalactite.sql.result.InMemoryResultSet;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.gama.lang.function.Functions.chain;
import static org.gama.lang.function.Functions.link;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		DIALECT.getSqlTypeRegistry().put(Color.class, "int");
		
		DIALECT.getDmlGenerator().sortColumnsAlphabetically();	// for steady checks on SQL orders
	}
	
	@BeforeEach
	void initEntityCandidates() {
		PersisterBuilderContext.CURRENT.set(new PersisterBuilderContext());
	}
	
	@AfterEach
	void removeEntityCandidates() {
		PersisterBuilderContext.CURRENT.remove();
	}
	
	@Test
	void build_connectionProviderIsNotRollbackObserver_throwsException() {
		PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(entityBuilder(Country.class, Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
				.versionedBy(Country::getVersion, new IntegerSerie())
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription));
		ConnectionConfigurationSupport connectionConfiguration = new ConnectionConfigurationSupport(new JdbcConnectionProvider(dataSource), 10);
		assertThatThrownBy(() -> testInstance.build(DIALECT, connectionConfiguration, persistenceContext, null))
				.isInstanceOf(UnsupportedOperationException.class);
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_fromMappedSuperClasses() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.add(Car::getModel)
						.mapSuperClass(embeddableBuilder(AbstractVehicle.class)
								.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
										.add(Timestamp::getCreationDate)
										.add(Timestamp::getModificationDate)
								)
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
		ArrayList<Entry<ReversibleAccessor, Column>> expected = new ArrayList<>(Maps
				.forHashMap(ReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getId), mutatorByField(Car.class, "id")),
						dummyTable.getColumn("id"))
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
		ArrayList<Entry<ReversibleAccessor, Column>> actual = new ArrayList<>(map.giveMapping(dummyTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actual)
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expected);
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_fromInheritedClasses() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getModel)
						.mapInheritance(entityBuilder(AbstractVehicle.class, Identifier.class)
								.add(AbstractVehicle::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
								.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
										.add(Timestamp::getCreationDate)
										.add(Timestamp::getModificationDate))
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
		ArrayList<Entry<ReversibleAccessor, Column>> expected = new ArrayList<>(Maps
				.forHashMap(ReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getId), mutatorByField(Car.class, "id")),
						dummyTable.getColumn("id"))
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
		assertThat(map.giveTables())
				.extracting(Table::getAbsoluteName)
				.containsExactly("Car");
		ArrayList<Entry<ReversibleAccessor, Column>> actual = new ArrayList<>(map.giveMapping(dummyTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actual)
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expected);
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
								.embed(Vehicle::getColor, embeddableBuilder(Color.class)
										.add(Color::getRgb))
								.mapInheritance(entityBuilder(AbstractVehicle.class, Identifier.class)
										.add(AbstractVehicle::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
										.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
												.add(Timestamp::getCreationDate)
												.add(Timestamp::getModificationDate)))
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
		
		assertThat(map.giveTables())
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
		List<Entry<ReversibleAccessor, Column>> actualCarMapping = new ArrayList<>(map.giveMapping(carTable).entrySet());
		actualCarMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actualCarMapping)
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
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
		List<Entry<ReversibleAccessor, Column>> actualVehicleMapping = new ArrayList<>(map.giveMapping(vehicleTable).entrySet());
		actualVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actualVehicleMapping)
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expectedVehicleMapping);
		
		// Checking AbstractVehicle mapping
		// we get the table instance created by builder because our (the one of this test) is only one with same name but without columns because
		// it wasn't given at mapping definition time
		abstractVehicleTable = Iterables.find(map.giveTables(), abstractVehicleTable::equals);
		List<Entry<ReversibleAccessor, Column>> expectedAbstractVehicleMapping = new ArrayList<>(Maps
				.forHashMap(ReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(AbstractVehicle::getId), mutatorByField(Car.class, "id")),
						abstractVehicleTable.getColumn("id"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(AbstractVehicle::getTimestamp), mutatorByMethodReference(AbstractVehicle::setTimestamp)),
						new PropertyAccessor<>(accessorByMethodReference(Timestamp::getCreationDate), mutatorByMethodReference(Timestamp::setCreationDate))),
						abstractVehicleTable.getColumn("creationDate"))
				.add(new AccessorChain<>(new PropertyAccessor<>(accessorByMethodReference(AbstractVehicle::getTimestamp), mutatorByMethodReference(AbstractVehicle::setTimestamp)),
						new PropertyAccessor<>(accessorByMethodReference(Timestamp::getModificationDate), mutatorByMethodReference(Timestamp::setModificationDate))),
						abstractVehicleTable.getColumn("modificationDate"))
				.entrySet());
		expectedAbstractVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		List<Entry<ReversibleAccessor, Column>> actualAbstractVehicleMapping = new ArrayList<>(map.giveMapping(abstractVehicleTable).entrySet());
		actualAbstractVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actualAbstractVehicleMapping)
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expectedAbstractVehicleMapping);
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_withoutHierarchy() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.add(Car::getModel)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.add(Timestamp::getCreationDate)
								.add(Timestamp::getModificationDate))
						);
		
		Table dummyTable = new Table("Car");
		testInstance.setColumnBinderRegistry(DIALECT.getColumnBinderRegistry())
				.setColumnNamingStrategy(ColumnNamingStrategy.DEFAULT)
				.setTable(dummyTable)
				.mapEntityConfigurationPerTable();
		
		MappingPerTable map = testInstance.collectEmbeddedMappingFromInheritance();
		
		// NB: containsOnly() doesn't work : returns false whereas result is good
		// (probably due to ValueAccessPoint Comparator not used by containsOnly() method)
		ArrayList<Entry<ReversibleAccessor, Column>> expected = new ArrayList<>(Maps
				.forHashMap(ReversibleAccessor.class, Column.class)
				.add(new PropertyAccessor<>(accessorByMethodReference(Car::getId), mutatorByField(Car.class, "id")),
						dummyTable.getColumn("id"))
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
		ArrayList<Entry<ReversibleAccessor, Column>> actual = new ArrayList<>(map.giveMapping(dummyTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actual)
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expected);
	}
	
	@Test
	void addIdentifyingPrimarykey_alreadyAssignedPolicy() {
		EntityMappingConfiguration<AbstractVehicle, Identifier> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.class)
				.add(AbstractVehicle::getId).identifier(IdentifierPolicy.alreadyAssigned(o -> {}, o -> true))
				.getConfiguration();
		
		Table mainTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration)
				.setTable(mainTable)
				.setColumnNamingStrategy(accessorDefinition -> "myId");
		testInstance.mapEntityConfigurationPerTable();
		testInstance.addIdentifyingPrimarykey(new Identification(identifyingConfiguration));
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		assertThat((Set<Column>) mainTable.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(mainTable.getColumn("myId"));
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isAutoGenerated()).isFalse();
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isNullable()).isFalse();
	}
	
	@Test
	void addIdentifyingPrimarykey_beforeInsertPolicy() {
		EntityMappingConfiguration<AbstractVehicle, Identifier> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.class)
				.add(AbstractVehicle::getId).identifier(IdentifierPolicy.beforeInsert(new InMemoryCounterIdentifierGenerator()))
				.getConfiguration();
		
		Table mainTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration)
				.setTable(mainTable)
				.setColumnNamingStrategy(accessorDefinition -> "myId");
		testInstance.mapEntityConfigurationPerTable();
		testInstance.addIdentifyingPrimarykey(new Identification(identifyingConfiguration));
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		assertThat((Set<Column>) mainTable.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(mainTable.getColumn("myId"));
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isAutoGenerated()).isFalse();
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isNullable()).isFalse();
	}
	
	@Test
	void addIdentifyingPrimarykey_afterInsertPolicy() {
		EntityMappingConfiguration<AbstractVehicle, Identifier> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.class)
				.add(AbstractVehicle::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.getConfiguration();
		
		Table mainTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration)
				.setTable(mainTable)
				.setColumnNamingStrategy(accessorDefinition -> "myId");
		testInstance.mapEntityConfigurationPerTable();
		testInstance.addIdentifyingPrimarykey(new Identification(identifyingConfiguration));
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		assertThat((Set<Column>) mainTable.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(mainTable.getColumn("myId"));
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isAutoGenerated()).isTrue();
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isNullable()).isFalse();
	}
	
	@Test
	void propagatePrimarykey() {
		EntityMappingConfiguration<AbstractVehicle, Identifier> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.class)
				.add(AbstractVehicle::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.getConfiguration();
		
		Table mainTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration)
				.setTable(mainTable)
				.setColumnNamingStrategy(ColumnNamingStrategy.DEFAULT)
				.setForeignKeyNamingStrategy(ForeignKeyNamingStrategy.DEFAULT);
		testInstance.mapEntityConfigurationPerTable();
		testInstance.addIdentifyingPrimarykey(new Identification(identifyingConfiguration));
		
		Table tableB = new Table("Vehicle");
		Table tableC = new Table("Car");
		testInstance.propagatePrimarykey(mainTable.getPrimaryKey(), Arrays.asSet(tableB, tableC));
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		assertThat((Set<Column>) mainTable.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(mainTable.getColumn("id"));
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isAutoGenerated()).isTrue();
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isNullable()).isFalse();
		assertThat((Set<Column>) tableB.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(tableB.getColumn("id"));
		assertThat(Iterables.first((Set<Column>) tableB.getPrimaryKey().getColumns()).isAutoGenerated()).isFalse();
		assertThat(Iterables.first((Set<Column>) tableB.getPrimaryKey().getColumns()).isNullable()).isFalse();
		assertThat((Set<Column>) tableC.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(tableC.getColumn("id"));
		assertThat(Iterables.first((Set<Column>) tableC.getPrimaryKey().getColumns()).isAutoGenerated()).isFalse();
		assertThat(Iterables.first((Set<Column>) tableC.getPrimaryKey().getColumns()).isNullable()).isFalse();
	}
	
	@Test
	void applyForeignKeys() {
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
		PrimaryKey primaryKey = testInstance.addIdentifyingPrimarykey(new Identification(identifyingConfiguration));
		testInstance.propagatePrimarykey(primaryKey, Arrays.asSet(tableB, tableC));
		testInstance.applyForeignKeys(primaryKey, Arrays.asSet(tableB, tableC));
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));  
		Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
				ForeignKey::getName,
				link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
				link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
		assertThat(tableB.getForeignKeys())
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(fkPrinter))
				.containsExactly(new ForeignKey("FK_Vehicle_id_AbstractVehicle_id", tableB.getColumn("id"), mainTable.getColumn("id")));
		assertThat(tableC.getForeignKeys())
				// Objects are similar but not equals so we compare them throught their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(fkPrinter))
				.containsExactly(new ForeignKey("FK_Car_id_Vehicle_id", tableC.getColumn("id"), tableB.getColumn("id")));
	}
	
	@Test
	void build_returnsAlreadyExisintgPersister() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getModel)
						.add(Vehicle::getColor)
						.add(AbstractVehicle::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.add(Timestamp::getCreationDate)
								.add(Timestamp::getModificationDate))
		);
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		PersistenceContext persistenceContext = new PersistenceContext(connectionProviderMock, DIALECT);
		assertThat(testInstance.build(persistenceContext)).isSameAs(testInstance.build(persistenceContext));
	}
	
	@Test
	void build_singleTable_singleClass() throws SQLException {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getModel)
						.add(Vehicle::getColor)
						.add(AbstractVehicle::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.add(Timestamp::getCreationDate)
								.add(Timestamp::getModificationDate))
		);
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		Connection connectionMock = mock(Connection.class);
		when(connectionProviderMock.getCurrentConnection()).thenReturn(connectionMock);
		ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatementMock);
		when(preparedStatementMock.executeBatch()).thenReturn(new int[] { 1 });
		EntityPersister<Car, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
		Car entity = new Car(1L);
		entity.setModel("Renault");
		entity.setColor(new Color(123));
		entity.setTimestamp(new Timestamp());
		result.insert(entity);
		assertThat(sqlCaptor.getAllValues())
				.containsExactly("insert into Car(color, creationDate, id, model, modificationDate) values (?, ?, ?, ?, ?)");
	}
	
	@Test
	void build_singleTable_withInheritance() throws SQLException {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getModel)
						.mapInheritance(entityBuilder(Vehicle.class, Identifier.class)
								.add(Vehicle::getColor)
								.mapInheritance(entityBuilder(AbstractVehicle.class, Identifier.class)
										.add(AbstractVehicle::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
										.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
												.add(Timestamp::getCreationDate)
												.add(Timestamp::getModificationDate))
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
		EntityPersister<Car, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
		Car entity = new Car(1L);
		entity.setModel("Renault");
		entity.setColor(new Color(123));
		entity.setTimestamp(new Timestamp());
		result.insert(entity);
		assertThat(sqlCaptor.getAllValues())
				.containsExactly("insert into Car(color, creationDate, id, model, modificationDate) values (?, ?, ?, ?, ?)");
	}
	
	@Test
	void build_joinedTables() throws SQLException {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getModel)
						.mapInheritance(entityBuilder(Vehicle.class, Identifier.class)
								.add(Vehicle::getColor)
								.mapInheritance(entityBuilder(AbstractVehicle.class, Identifier.class)
										.add(AbstractVehicle::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
										.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
												.add(Timestamp::getCreationDate)
												.add(Timestamp::getModificationDate))
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
		
		EntityPersister<Car, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
		Car entity = new Car(1L);
		entity.setModel("Renault");
		entity.setColor(new Color(123));
		entity.setTimestamp(new Timestamp());
		result.insert(entity);
		assertThat(insertCaptor.getAllValues()).containsExactly(
				"insert into AbstractVehicle(creationDate, id, modificationDate) values (?, ?, ?)",
				"insert into Vehicle(color, id) values (?, ?)",
				"insert into Car(id, model) values (?, ?)");
		
		ArgumentCaptor<String> deleteCaptor = ArgumentCaptor.forClass(String.class);
		when(connectionMock.prepareStatement(deleteCaptor.capture())).thenReturn(preparedStatementMock);
		result.delete(entity);
		assertThat(deleteCaptor.getAllValues()).containsExactly(
				"delete from Car where id = ?",
				"delete from Vehicle where id = ?",
				"delete from AbstractVehicle where id = ?");
		
		ArgumentCaptor<String> selectCaptor = ArgumentCaptor.forClass(String.class);
		when(connectionMock.prepareStatement(selectCaptor.capture())).thenReturn(preparedStatementMock);
		result.select(entity.getId());
		assertThat(selectCaptor.getAllValues()).containsExactly(
				// the expected select may change in future as we don't care about select order nor joins order, tested in case of huge regression
				"select"
						+ " Car.model as Car_model,"
						+ " Car.id as Car_id,"
						+ " AbstractVehicle.modificationDate as AbstractVehicle_modificationDate,"
						+ " AbstractVehicle.creationDate as AbstractVehicle_creationDate,"
						+ " AbstractVehicle.id as AbstractVehicle_id,"
						+ " Vehicle.color as Vehicle_color,"
						+ " Vehicle.id as Vehicle_id"
						+ " from Car inner join AbstractVehicle as AbstractVehicle on Car.id = AbstractVehicle.id"
						+ " inner join Vehicle as Vehicle on Car.id = Vehicle.id"
						+ " where Car.id in (?)");
	}
	
	@Test
	void build_createsAnInstanceThatDoesntRequiresTwoSelectsOnItsUpdateMethod() throws SQLException {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.class)
						.add(Car::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Car::getModel));
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		Connection connectionMock = mock(Connection.class);
		when(connectionProviderMock.getCurrentConnection()).thenReturn(connectionMock);
		ArgumentCaptor<String> insertCaptor = ArgumentCaptor.forClass(String.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(insertCaptor.capture())).thenReturn(preparedStatementMock);
		when(preparedStatementMock.executeBatch()).thenReturn(new int[] { 1 });
		when(preparedStatementMock.executeQuery()).thenReturn(new InMemoryResultSet(Arrays.asList(Maps.forHashMap(String.class, Object.class)
				.add("Car_id", 1L)
				.add("Car_model", "Renault"))));
		
		EntityPersister<Car, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		
		result.insert(dummyCar);
		
		// this should execute a SQL select only once thanks to cache
		result.update(dummyCar.getId(), vehicle -> vehicle.setModel("Peugeot"));
		
		// only 1 select was done whereas entity was updated
		assertThat(insertCaptor.getAllValues()).containsExactly(
				"insert into Car(id, model) values (?, ?)",
				"select Car.model as Car_model, Car.id as Car_id from Car where Car.id in (?)",
				"update Car set model = ? where id = ?"
				);
		ArgumentCaptor<Integer> valueIndexCaptor = ArgumentCaptor.forClass(int.class);
		ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
		verify(preparedStatementMock, times(2)).setString(valueIndexCaptor.capture(), valueCaptor.capture());
		assertThat(valueCaptor.getAllValues()).containsExactly(
				"Renault",
				"Peugeot");
		
		verify(preparedStatementMock).executeQuery();
		
	}
	
	@Test
	void build_resultAssertsThatPersisterManageGivenEntities() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(AbstractVehicle.class, Identifier.class)
						.add(AbstractVehicle::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.add(Timestamp::getCreationDate)
								.add(Timestamp::getModificationDate))
		);
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		EntityPersister<AbstractVehicle, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
		assertThatThrownBy(() -> result.persist(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Vehicle");
		assertThatThrownBy(() -> result.insert(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Vehicle");
		assertThatThrownBy(() -> result.update(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Vehicle");
		assertThatThrownBy(() -> result.updateById(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Vehicle");
		assertThatThrownBy(() -> result.delete(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Vehicle");
		assertThatThrownBy(() -> result.deleteById(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Vehicle");
	}
	
	@Test
	void build_withPolymorphismJoinedTables_resultAssertsThatPersisterManageGivenEntities() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(AbstractVehicle.class, Identifier.class)
						.add(AbstractVehicle::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.add(Timestamp::getCreationDate)
								.add(Timestamp::getModificationDate))
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
							.addSubClass(subentityBuilder(Vehicle.class, Identifier.class)))
		);
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		EntityPersister<AbstractVehicle, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
		assertThatThrownBy(() -> result.persist(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
		assertThatThrownBy(() -> result.insert(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
		assertThatThrownBy(() -> result.update(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
		assertThatThrownBy(() -> result.updateById(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
		assertThatThrownBy(() -> result.delete(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
		assertThatThrownBy(() -> result.deleteById(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
	}
	
	@Test
	void build_withPolymorphismSingleTable_resultAssertsThatPersisterManageGivenEntities() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(AbstractVehicle.class, Identifier.class)
						.add(AbstractVehicle::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.add(Timestamp::getCreationDate)
								.add(Timestamp::getModificationDate))
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
								.addSubClass(subentityBuilder(Vehicle.class, Identifier.class), "Vehicle"))
		);
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		EntityPersister<AbstractVehicle, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
		assertThatThrownBy(() -> result.persist(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
		assertThatThrownBy(() -> result.insert(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
		assertThatThrownBy(() -> result.update(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
		assertThatThrownBy(() -> result.updateById(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
		assertThatThrownBy(() -> result.delete(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
		assertThatThrownBy(() -> result.deleteById(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.g.s.p.e.m.AbstractVehicle is not configured to persist o.g.s.p.e.m.Car");
	}
	
	public static class ToStringBuilder<E> {
		
		@SafeVarargs
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