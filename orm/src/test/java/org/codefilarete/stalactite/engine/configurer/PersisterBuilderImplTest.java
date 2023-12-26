package org.codefilarete.stalactite.engine.configurer;

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
import org.codefilarete.reflection.AccessorByField;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.MappingPerTable;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.function.Sequence;
import org.codefilarete.tool.function.Serie.IntegerSerie;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.reflection.Accessors.accessorByMethodReference;
import static org.codefilarete.reflection.Accessors.mutatorByField;
import static org.codefilarete.reflection.Accessors.mutatorByMethodReference;
import static org.codefilarete.stalactite.engine.MappingEase.embeddableBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;
import static org.codefilarete.tool.function.Functions.chain;
import static org.codefilarete.tool.function.Functions.link;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * @author Guillaume Mary
 */
public class PersisterBuilderImplTest {
	
	private static final Dialect DIALECT = new Dialect();
	
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
	void assertCompositeKeyIdentifierOverridesEqualsHashcode() throws NoSuchFieldException {
		class DummyCompositeKeyIdentifier {
			
		}
		
		class DummyCompositeKeyIdentifierWithEqualsAndHashCode {
			
			@Override
			public boolean equals(Object obj) {
				// any kind of implementation is sufficient
				return super.equals(obj);
			}
			
			@Override
			public int hashCode() {
				// any kind of implementation is sufficient
				return super.hashCode();
			}
		}
		
		class DummyEntity {
			
			private DummyCompositeKeyIdentifier dummyCompositeKeyIdentifier;
			private DummyCompositeKeyIdentifierWithEqualsAndHashCode dummyCompositeKeyIdentifierWithEqualsAndHashCode;
		}
		
		EntityMappingConfiguration.CompositeKeyMapping<?, ?> compositeKeyMappingMock = mock(EntityMappingConfiguration.CompositeKeyMapping.class, RETURNS_MOCKS);
		when(compositeKeyMappingMock.getAccessor()).thenReturn(new AccessorByField<>(DummyEntity.class.getDeclaredField("dummyCompositeKeyIdentifier")));
		assertThatCode(() -> PersisterBuilderImpl.assertCompositeKeyIdentifierOverridesEqualsHashcode(compositeKeyMappingMock))
				.hasMessage("Composite key identifier class o.c.s.e.c.PersisterBuilderImplTest$DummyCompositeKeyIdentifier" +
						" seems to have default implementation of equals() and hashcode() methods," +
						" which is not supported (identifiers must be distinguishable), please make it implement them");
		
		when(compositeKeyMappingMock.getAccessor()).thenReturn(new AccessorByField<>(DummyEntity.class.getDeclaredField("dummyCompositeKeyIdentifierWithEqualsAndHashCode")));
		assertThatCode(() -> PersisterBuilderImpl.assertCompositeKeyIdentifierOverridesEqualsHashcode(compositeKeyMappingMock))
				.doesNotThrowAnyException();
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_fromMappedSuperClasses() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.LONG_TYPE)
						.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Car::getModel)
						.mapSuperClass(embeddableBuilder(AbstractVehicle.class)
								.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
										.map(Timestamp::getCreationDate)
										.map(Timestamp::getModificationDate)
								)
						)
		);
		
		Table dummyTable = new Table("Car");
		testInstance.setColumnBinderRegistry(DIALECT.getColumnBinderRegistry())
				.setTable(dummyTable)
				.mapEntityConfigurationPerTable();
		
		MappingPerTable map = testInstance.collectPropertiesMappingFromInheritance();
		
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
		ArrayList<Entry<ReversibleAccessor, Column>> actual = new ArrayList<>(map.giveMapping(dummyTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actual)
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expected);
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_fromInheritedClasses() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.LONG_TYPE)
						.map(Car::getModel)
						.mapSuperClass(entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
								.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
								.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
										.map(Timestamp::getCreationDate)
										.map(Timestamp::getModificationDate))
						)
		);
		
		Table dummyTable = new Table("Car");
		testInstance.setColumnBinderRegistry(DIALECT.getColumnBinderRegistry())
				.setTable(dummyTable)
				.mapEntityConfigurationPerTable();
		
		MappingPerTable<?> mappingPerTable = testInstance.collectPropertiesMappingFromInheritance();
		
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
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
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
						.withJoinedTable(vehicleTable));
		
		
		testInstance.setColumnBinderRegistry(DIALECT.getColumnBinderRegistry())
				.setTable(carTable)
				.mapEntityConfigurationPerTable();
		
		MappingPerTable<?> map = testInstance.collectPropertiesMappingFromInheritance();
		
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
		List<Entry<ReversibleAccessor, Column>> actualVehicleMapping = new ArrayList<>(map.giveMapping(vehicleTable).entrySet());
		actualVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actualVehicleMapping)
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expectedVehicleMapping);
		
		// Checking AbstractVehicle mapping
		// we get the table instance created by builder because our (the one of this test) is only one with same name but without columns because
		// it wasn't given at mapping definition time
		abstractVehicleTable = Iterables.find(map.giveTables(), abstractVehicleTable::equals);
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
		List<Entry<ReversibleAccessor, Column>> actualAbstractVehicleMapping = new ArrayList<>(map.giveMapping(abstractVehicleTable).entrySet());
		actualAbstractVehicleMapping.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actualAbstractVehicleMapping)
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expectedAbstractVehicleMapping);
	}
	
	@Test
	void collectEmbeddedMappingFromInheritance_withoutHierarchy() {
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.LONG_TYPE)
						.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Car::getModel)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate))
						);
		
		Table dummyTable = new Table("Car");
		testInstance.setColumnBinderRegistry(DIALECT.getColumnBinderRegistry())
				.setTable(dummyTable)
				.mapEntityConfigurationPerTable();
		
		MappingPerTable map = testInstance.collectPropertiesMappingFromInheritance();
		
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
		ArrayList<Entry<ReversibleAccessor, Column>> actual = new ArrayList<>(map.giveMapping(dummyTable).entrySet());
		actual.sort((e1, e2) -> String.CASE_INSENSITIVE_ORDER.compare(e1.toString(), e2.toString()));
		assertThat(actual)
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(Object::toString))
				.isEqualTo(expected);
	}
	
	@Test
	void addIdentifyingPrimaryKey_alreadyAssignedPolicy() {
		EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
				.mapKey(AbstractVehicle::getId, IdentifierPolicy.alreadyAssigned(o -> {}, o -> true))
				.withColumnNaming(accessorDefinition -> "myId")
				.getConfiguration();
		
		Table mainTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration)
				.setTable(mainTable);
		testInstance.mapEntityConfigurationPerTable();
		testInstance.addIdentifyingPrimarykey(AbstractIdentification.forSingleKey(identifyingConfiguration));

		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		assertThat((Set<Column>) mainTable.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(mainTable.getColumn("myId"));
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isAutoGenerated()).isFalse();
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isNullable()).isFalse();
	}
	
	@Test
	void addIdentifyingPrimarykey_beforeInsertPolicy() {
		EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
				.mapKey(AbstractVehicle::getId, IdentifierPolicy.beforeInsert(new Sequence<Identifier<Long>>() {
					
					private long identifier = 0;
					
					@Override
					public Identifier<Long> next() {
						return new PersistableIdentifier<>(identifier++);
					}
				}))
				.withColumnNaming(accessorDefinition -> "myId")
				.getConfiguration();
		
		Table mainTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration)
				.setTable(mainTable);
		testInstance.mapEntityConfigurationPerTable();
		testInstance.addIdentifyingPrimarykey(AbstractIdentification.forSingleKey(identifyingConfiguration));
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		assertThat((Set<Column>) mainTable.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(mainTable.getColumn("myId"));
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isAutoGenerated()).isFalse();
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isNullable()).isFalse();
	}
	
	@Test
	void addIdentifyingPrimarykey_afterInsertPolicy() {
		EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
				.mapKey(AbstractVehicle::getId, IdentifierPolicy.afterInsert())
				.withColumnNaming(accessorDefinition -> "myId")
				.getConfiguration();
		
		Table mainTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration)
				.setTable(mainTable);
		testInstance.mapEntityConfigurationPerTable();
		testInstance.addIdentifyingPrimarykey(AbstractIdentification.forSingleKey(identifyingConfiguration));
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		assertThat((Set<Column>) mainTable.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(mainTable.getColumn("myId"));
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isAutoGenerated()).isTrue();
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isNullable()).isFalse();
	}
	
	@Test
	void propagatePrimarykey() {
		EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
				.mapKey(AbstractVehicle::getId, IdentifierPolicy.afterInsert())
				.getConfiguration();
		
		Table mainTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration)
				.setTable(mainTable);
		testInstance.mapEntityConfigurationPerTable();
		testInstance.addIdentifyingPrimarykey(AbstractIdentification.forSingleKey(identifyingConfiguration));
		
		Table tableB = new Table("Vehicle");
		Table tableC = new Table("Car");
		PersisterBuilderImpl.propagatePrimaryKey(mainTable.getPrimaryKey(), Arrays.asSet(tableB, tableC));
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		assertThat((Set<Column>) mainTable.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(mainTable.getColumn("id"));
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isAutoGenerated()).isTrue();
		assertThat(Iterables.first((Set<Column>) mainTable.getPrimaryKey().getColumns()).isNullable()).isFalse();
		assertThat((Set<Column>) tableB.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(tableB.getColumn("id"));
		assertThat(Iterables.first((Set<Column>) tableB.getPrimaryKey().getColumns()).isAutoGenerated()).isFalse();
		assertThat(Iterables.first((Set<Column>) tableB.getPrimaryKey().getColumns()).isNullable()).isFalse();
		assertThat((Set<Column>) tableC.getPrimaryKey().getColumns())
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(columnPrinter))
				.containsExactly(tableC.getColumn("id"));
		assertThat(Iterables.first((Set<Column>) tableC.getPrimaryKey().getColumns()).isAutoGenerated()).isFalse();
		assertThat(Iterables.first((Set<Column>) tableC.getPrimaryKey().getColumns()).isNullable()).isFalse();
	}
	
	@Test
	void applyForeignKeys() {
		EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> identifyingConfiguration = entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
				.mapKey(AbstractVehicle::getId, IdentifierPolicy.afterInsert())
				.getConfiguration();
		
		Table mainTable = new Table("AbstractVehicle");
		PersisterBuilderImpl testInstance = new PersisterBuilderImpl(identifyingConfiguration)
				.setTable(mainTable);
		testInstance.mapEntityConfigurationPerTable();
		
		Table tableB = new Table("Vehicle");
		Table tableC = new Table("Car");
		PrimaryKey primaryKey = testInstance.addIdentifyingPrimarykey(AbstractIdentification.forSingleKey(identifyingConfiguration));
		PersisterBuilderImpl.propagatePrimaryKey(primaryKey, Arrays.asSet(tableB, tableC));
		testInstance.applyForeignKeys(primaryKey, Arrays.asSet(tableB, tableC));
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
																	Column::getAbsoluteName,
																	chain(Column::getJavaType, Reflections::toString));  
		Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
				ForeignKey::getName,
				link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
				link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
		assertThat(tableB.getForeignKeys())
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(fkPrinter))
				.containsExactly(new ForeignKey("FK_Vehicle_id_AbstractVehicle_id", tableB.getColumn("id"), mainTable.getColumn("id")));
		assertThat(tableC.getForeignKeys())
				// Objects are similar but not equals so we compare them through their footprint (truly comparing them is quite hard)
				.usingElementComparator(Comparator.comparing(fkPrinter))
				.containsExactly(new ForeignKey("FK_Car_id_Vehicle_id", tableC.getColumn("id"), tableB.getColumn("id")));
	}
	
	@Nested
	class BuildTest {
		
		@BeforeEach
		void removeEntityCandidates() {
			PersisterBuilderContext.CURRENT.remove();
		}
		
		@Test
		void build_connectionProviderIsNotRollbackObserver_throwsException() {
			DataSource dataSource = mock(DataSource.class);
			PersisterBuilderImpl testInstance = new PersisterBuilderImpl(entityBuilder(Country.class, Identifier.LONG_TYPE)
																			 // setting a foreign key naming strategy to be tested
																			 .withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
																			 .versionedBy(Country::getVersion, new IntegerSerie())
																			 .mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
																			 .map(Country::getName)
																			 .map(Country::getDescription));
			ConnectionConfigurationSupport connectionConfiguration = new ConnectionConfigurationSupport(new CurrentThreadConnectionProvider(dataSource), 10);
			assertThatThrownBy(() -> testInstance.build(DIALECT, connectionConfiguration, Mockito.mock(PersisterRegistry.class), null))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessage("Version control is only supported with o.c.s.s.ConnectionProvider that also implements o.c.s.s.RollbackObserver");
		}
		
		@Test
		void build_returnsAlreadyExisintgPersister() {
			PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.LONG_TYPE)
					.map(Car::getModel)
					.map(Vehicle::getColor)
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
			);
			
			ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
			PersistenceContext persistenceContext = new PersistenceContext(connectionProviderMock, DIALECT);
			assertThat(testInstance.build(persistenceContext)).isSameAs(testInstance.build(persistenceContext));
		}
		
		@Test
		void build_singleTable_singleClass() throws SQLException {
			PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.LONG_TYPE)
					.map(Car::getModel)
					.map(Vehicle::getColor)
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
			);
			
			ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
			Connection connectionMock = mock(Connection.class);
			when(connectionProviderMock.giveConnection()).thenReturn(connectionMock);
			ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
			PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
			when(connectionMock.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatementMock);
			when(preparedStatementMock.executeLargeBatch()).thenReturn(new long[] { 1 });
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
				entityBuilder(Car.class, Identifier.LONG_TYPE)
					.map(Car::getModel)
					.mapSuperClass(entityBuilder(Vehicle.class, Identifier.LONG_TYPE)
										.map(Vehicle::getColor)
										.mapSuperClass(entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
															.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
															.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
																.map(Timestamp::getCreationDate)
																.map(Timestamp::getModificationDate))
										)
					)
			);
			
			ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
			Connection connectionMock = mock(Connection.class);
			when(connectionProviderMock.giveConnection()).thenReturn(connectionMock);
			ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
			PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
			when(connectionMock.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatementMock);
			when(preparedStatementMock.executeLargeBatch()).thenReturn(new long[] { 1 });
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
				entityBuilder(Car.class, Identifier.LONG_TYPE)
					.map(Car::getModel)
					.mapSuperClass(entityBuilder(Vehicle.class, Identifier.LONG_TYPE)
										.map(Vehicle::getColor)
										.mapSuperClass(entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
															.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
															.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
																.map(Timestamp::getCreationDate)
																.map(Timestamp::getModificationDate))
										).withJoinedTable()
					).withJoinedTable()
			);
			
			ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
			Connection connectionMock = mock(Connection.class);
			when(connectionProviderMock.giveConnection()).thenReturn(connectionMock);
			ArgumentCaptor<String> insertCaptor = ArgumentCaptor.forClass(String.class);
			PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
			when(connectionMock.prepareStatement(insertCaptor.capture())).thenReturn(preparedStatementMock);
			when(preparedStatementMock.executeLargeBatch()).thenReturn(new long[] { 1 });
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
					+ " AbstractVehicle.creationDate as AbstractVehicle_creationDate,"
					+ " AbstractVehicle.modificationDate as AbstractVehicle_modificationDate,"
					+ " AbstractVehicle.id as AbstractVehicle_id,"
					+ " Vehicle.color as Vehicle_color,"
					+ " Vehicle.id as Vehicle_id"
					+ " from Car inner join AbstractVehicle as AbstractVehicle on Car.id = AbstractVehicle.id"
					+ " inner join Vehicle as Vehicle on Car.id = Vehicle.id"
					+ " where Car.id in (?)");
		}
		
		@Test
		void build_createsAnInstanceThatDoesntRequireTwoSelectsOnItsUpdateMethod() throws SQLException {
			PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(Car.class, Identifier.LONG_TYPE)
					.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Car::getModel));
			ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
			Connection connectionMock = mock(Connection.class);
			when(connectionProviderMock.giveConnection()).thenReturn(connectionMock);
			ArgumentCaptor<String> insertCaptor = ArgumentCaptor.forClass(String.class);
			PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
			when(connectionMock.prepareStatement(insertCaptor.capture())).thenReturn(preparedStatementMock);
			when(preparedStatementMock.executeLargeBatch()).thenReturn(new long[] { 1 });
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
				entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
			);
			ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
			EntityPersister<AbstractVehicle, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
			assertThatThrownBy(() -> result.persist(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Vehicle");
			assertThatThrownBy(() -> result.insert(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Vehicle");
			assertThatThrownBy(() -> result.update(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Vehicle");
			assertThatThrownBy(() -> result.updateById(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Vehicle");
			assertThatThrownBy(() -> result.delete(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Vehicle");
			assertThatThrownBy(() -> result.deleteById(new Vehicle(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Vehicle");
		}
		
		@Test
		void build_withPolymorphismJoinedTables_resultAssertsThatPersisterManageGivenEntities() {
			PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
										 .addSubClass(subentityBuilder(Vehicle.class, Identifier.LONG_TYPE)))
			);
			ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
			EntityPersister<AbstractVehicle, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
			assertThatThrownBy(() -> result.persist(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
			assertThatThrownBy(() -> result.insert(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
			assertThatThrownBy(() -> result.update(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
			assertThatThrownBy(() -> result.updateById(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
			assertThatThrownBy(() -> result.delete(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
			assertThatThrownBy(() -> result.deleteById(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
		}
		
		@Test
		void build_withPolymorphismSingleTable_resultAssertsThatPersisterManageGivenEntities() {
			PersisterBuilderImpl testInstance = new PersisterBuilderImpl(
				entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
										 .addSubClass(subentityBuilder(Vehicle.class, Identifier.LONG_TYPE), "Vehicle"))
			);
			ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
			EntityPersister<AbstractVehicle, Identifier> result = testInstance.build(new PersistenceContext(connectionProviderMock, DIALECT));
			assertThatThrownBy(() -> result.persist(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
			assertThatThrownBy(() -> result.insert(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
			assertThatThrownBy(() -> result.update(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
			assertThatThrownBy(() -> result.updateById(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
			assertThatThrownBy(() -> result.delete(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
			assertThatThrownBy(() -> result.deleteById(new Car(42L)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, UnsupportedOperationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Persister of o.c.s.e.m.AbstractVehicle is not configured to persist o.c.s.e.m.Car");
		}
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