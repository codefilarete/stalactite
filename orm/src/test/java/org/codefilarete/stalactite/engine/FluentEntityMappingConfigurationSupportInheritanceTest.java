package org.codefilarete.stalactite.engine;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Bicycle;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.engine.model.book.AbstractEntity;
import org.codefilarete.stalactite.engine.model.book.Author;
import org.codefilarete.stalactite.engine.model.book.Book;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.ddl.structure.UniqueConstraint;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy.databaseAutoIncrement;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.query.model.FluentQueries.select;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportInheritanceTest {
	
	private static final Dialect DIALECT = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(dataSource);
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		DIALECT.getSqlTypeRegistry().put(Color.class, "int");
	}
	
	@BeforeEach
	public void beforeTest() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
	}
	
	@Nested
	class MappedSuperClass {
		
		@Test
		void simpleCase() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					// concrete class defines id
					.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapSuperClass(embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor))
					.build(persistenceContext);
			
			// as a mapped super class, the table shouldn't be in the context, nor its persister exists
			Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables.contains(mappedSuperClassData.vehicleTable)).isFalse();
			assertThat(persistenceContext.findPersister(Vehicle.class)).isNull();
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.select(Car::new, mappedSuperClassData.carTable.idColumn, m -> m
					.add(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.add(mappedSuperClassData.carTable.colorColumn, Car::setColor));
			assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void simpleCase_withTableDefinedInSuperClass() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					// concrete class defines id
					.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapSuperClass(embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor))
					.build(persistenceContext);
			
			// as a mapped super class, the table shouldn't be in the context, nor its persister exists
			Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables.contains(mappedSuperClassData.vehicleTable)).isFalse();
			assertThat(persistenceContext.findPersister(Vehicle.class)).isNull();
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.select(Car::new, mappedSuperClassData.carTable.idColumn, m -> m
					.add(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.add(mappedSuperClassData.carTable.colorColumn, Car::setColor));
			assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void columnNamingStrategyChanged() {
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.withColumnNaming(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_col")
					// concrete class defines id
					.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapSuperClass(embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			ExecutableQuery<Car> carExecutableQuery = persistenceContext.newQuery("select id_col, model_col, color_col from Car", Car.class)
					.mapKey((SerializableFunction<Identifier<Long>, Car>) Car::new, "id_col", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model_col", Car::setModel)
					.map("color_col", Car::setColor);
			Set<Car> allCars = carExecutableQuery.execute(Accumulators.toSet());
			assertThat(allCars).containsExactly(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void columnNamingStrategyChanged_inParent() {
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					// concrete class defines id
					.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapSuperClass(embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor)
							.withColumnNaming(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_supercol"))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			ExecutableQuery<Car> carExecutableQuery = persistenceContext.newQuery("select id_supercol, model_supercol, color_supercol from Car", Car.class)
					.mapKey((SerializableFunction<Identifier<Long>, Car>) Car::new, "id_supercol", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model_supercol", Car::setModel)
					.map("color_supercol", Car::setColor);
			Set<Car> allCars = carExecutableQuery.execute(Accumulators.toSet());
			assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void columnNamingStrategyChanged_inBoth_lowestTakesPriority() {
			ColumnNamingStrategy columnNamingStrategy = accessor -> {
				if (accessor.getName().contains("Color")) {
					return ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_superCol";
				} else {
					return ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_col";
				}
			};
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.withColumnNaming(columnNamingStrategy)
					// concrete class defines id
					.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapSuperClass(embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			ExecutableQuery<Car> carExecutableQuery = persistenceContext.newQuery("select id_col, model_col, color_col from Car", Car.class)
					.mapKey((SerializableFunction<Identifier<Long>, Car>) Car::new, "id_col", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model_col", Car::setModel)
					.map("color_col", Car::setColor);
			Set<Car> allCars = carExecutableQuery.execute(Accumulators.toSet());
			assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void indexNamingStrategyChanged() {
			EntityPersister<Car, Identifier<Long>> persister = entityBuilder(Car.class, LONG_TYPE)
					.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Car::getModel).unique()
					.withUniqueConstraintNaming((accessor, columnName) -> AccessorDefinition.giveDefinition(accessor).getName() + "_UK")
					.mapSuperClass(embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor))
					.build(persistenceContext);
			
			assertThat(((ConfiguredPersister<Car, Identifier<Long>>) persister).giveImpliedTables().stream()
					.flatMap(table -> table.getUniqueConstraints().stream())
					.map(UniqueConstraint::getName)
					.collect(Collectors.toList())).containsExactlyInAnyOrder("model_UK");
		}
		
		@Test
		void indexNamingStrategyChanged_inParent() {
			EntityPersister<Car, Identifier<Long>> persister = entityBuilder(Car.class, LONG_TYPE)
					.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Car::getModel).unique()
					.mapSuperClass(embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor)
							.withUniqueConstraintNaming((accessor, columnName) -> AccessorDefinition.giveDefinition(accessor).getName() + "_Unique_Index"))
					.build(persistenceContext);
			
			assertThat(((ConfiguredPersister<Car, Identifier<Long>>) persister).giveImpliedTables().stream()
					.flatMap(table -> table.getUniqueConstraints().stream())
					.map(UniqueConstraint::getName)
					.collect(Collectors.toList())).containsExactlyInAnyOrder("model_Unique_Index");
		}
		
		@Test
		void indexNamingStrategyChanged_inBoth_lowestTakesPriority() {
			EntityPersister<Car, Identifier<Long>> persister = entityBuilder(Car.class, LONG_TYPE)
					.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Car::getModel).unique()
					.withUniqueConstraintNaming((accessor, columnName) -> AccessorDefinition.giveDefinition(accessor).getName() + "_UK")
					.mapSuperClass(embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor)
							.withUniqueConstraintNaming((accessor, columnName) -> AccessorDefinition.giveDefinition(accessor).getName() + "_Unique_Index"))
					.build(persistenceContext);
			
			assertThat(((ConfiguredPersister<Car, Identifier<Long>>) persister).giveImpliedTables().stream()
					.flatMap(table -> table.getUniqueConstraints().stream())
					.map(UniqueConstraint::getName)
					.collect(Collectors.toList())).containsExactlyInAnyOrder("model_UK");
		}
		
		@Test
		void withoutIdDefined_throwsException() {
			assertThatThrownBy(() -> entityBuilder(Car.class, LONG_TYPE)
						.map(Car::getModel)
						.map(Car::getColor)
						.mapSuperClass(embeddableBuilder(Vehicle.class)
								.map(Vehicle::getColor))
						.build(persistenceContext))
					.isInstanceOf(UnsupportedOperationException.class)
					.hasMessage("Identifier is not defined for o.c.s.e.m.Car,"
							+ " please add one through o.c.s.d.e.FluentEntityMappingBuilder.mapKey(o.d.j.u.f.s.SerializableBiConsumer, o.c.s.d.i.IdentifierPolicy)");
		}
		
		@Test
		void withEmbeddable() {
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					// concrete class defines id
					.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapSuperClass(embeddableBuilder(Vehicle.class)
							.embed(Vehicle::getColor, embeddableBuilder(Color.class)
									.map(Color::getRgb)))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
			
			// checking with query to understand what's under the hood : rgb column is created instead of color
			ExecutableQuery<Car> carExecutableQuery = persistenceContext.newQuery("select id, model, rgb from Car", Car.class)
					.mapKey((SerializableFunction<Identifier<Long>, Car>) Car::new, "id", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model", Car::setModel)
					.map("rgb", Car::setColor, int.class, Color::new);
			Set<Car> allCars = carExecutableQuery.execute(Accumulators.toSet());
			assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
		}
	}
	
	@Nested
	class Inheritance {
		
		@Test
		void withIdDefinedInSuperClass() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.getConfiguration();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.map(Car::getColor)	// note : we don't need to embed Color because it is defined in the Dialect registry
					.mapSuperClass(inheritanceConfiguration)
					.build(persistenceContext);
			
			// as an inherited entity of non joined_tables policy, the table should not be in the context
			Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables.contains(mappedSuperClassData.vehicleTable)).isFalse();
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.select(Car::new, mappedSuperClassData.carTable.idColumn, m -> m
					.add(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.add(mappedSuperClassData.carTable.colorColumn, Car::setColor));
			assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		/**
		 * Checking that, while having several times a shared inheritance configuration, persister build doesn't fail
		 */
		@Test
		void withIdDefinedInSuperClass_severalTimes() {
			FluentEntityMappingBuilder<AbstractEntity, Long> inheritanceConfiguration = entityBuilder(AbstractEntity.class, long.class)
					// mapped super class defines id
					.mapKey(AbstractEntity::getId, databaseAutoIncrement());
			
			FluentEntityMappingBuilder<Author, Long> authorConfiguration = entityBuilder(Author.class, long.class)
					.map(Author::getName)
					.mapOneToMany(Author::getWrittenBooks, entityBuilder(Book.class, long.class)
							.map(Book::getTitle)
							.mapSuperClass(inheritanceConfiguration))
					.mapSuperClass(inheritanceConfiguration);
			
			assertThatCode(() -> authorConfiguration.build(persistenceContext)).doesNotThrowAnyException();
		}
		
		@Test
		void identifierIsRedefined_throwsException() {
			EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED).getConfiguration();
			
			assertThatThrownBy(() -> entityBuilder(Vehicle.class, LONG_TYPE)
							.mapSuperClass(inheritanceConfiguration)
							.mapKey(Vehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.build(persistenceContext))
					.isInstanceOf(MappingConfigurationException.class)
					.hasMessage("Defining an identifier in conjunction with entity inheritance is not supported"
							+ " : o.c.s.e.m.Vehicle defines identifier AbstractVehicle::getId"
							+ " while it inherits from o.c.s.e.m.AbstractVehicle");
		}
		
		@Test
		void multipleInheritance() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.getConfiguration();
			
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration2 = entityBuilder(Vehicle.class, LONG_TYPE)
					.mapSuperClass(inheritanceConfiguration)
					.map(Vehicle::getColor)
					.getConfiguration();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.mapSuperClass(inheritanceConfiguration2)
					.build(persistenceContext);
			
			// as an inherited entity, the table should not be in the context
			Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables.contains(mappedSuperClassData.vehicleTable)).isFalse();
			assertThat(tables.contains(mappedSuperClassData.carTable)).isTrue();
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.select(Car::new, mappedSuperClassData.carTable.idColumn, m -> m
					.add(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.add(mappedSuperClassData.carTable.colorColumn, Car::setColor));
			assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void multipleInheritance_joinedTables() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.getConfiguration();
			
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration2 = entityBuilder(Vehicle.class, LONG_TYPE)
					.mapSuperClass(inheritanceConfiguration).withJoinedTable()
					.getConfiguration();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.map(Car::getColor)
					.mapSuperClass(inheritanceConfiguration2).withJoinedTable()
					.build(persistenceContext);
			
			// as an inherited entity, the table should be in the context, and its persister does exist
			assertThat(DDLDeployer.collectTables(persistenceContext).stream().map(Table::getName).collect(Collectors.toSet())).isEqualTo(Arrays.asHashSet("Car", "Vehicle", "AbstractVehicle"));
			assertThat(((ConfiguredRelationalPersister) persistenceContext.findPersister(Car.class)).getMapping().getTargetTable().getName()).isEqualTo("Car");
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.select(Car::new, mappedSuperClassData.carTable.idColumn, m -> m
					.add(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.add(mappedSuperClassData.carTable.colorColumn, Car::setColor));
			assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void mappedSuperClass_and_entityInheritance_throwsException() {
			EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.getConfiguration();
			
			FluentEntityMappingBuilder<Car, Identifier<Long>> mappingBuilder = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.map(Car::getColor)
					.mapSuperClass(embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor))
					.mapSuperClass(inheritanceConfiguration);
			assertThatThrownBy(() -> mappingBuilder.build(persistenceContext))
					.isInstanceOf(MappingConfigurationException.class)
					.hasMessage("Combination of mapped super class and inheritance is not supported, please remove one of them");
		}
		
		@Test
		void joinedTables() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Vehicle::getColor)
					.getConfiguration();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.onTable(mappedSuperClassData.carTable)
					.map(Car::getModel)
					.mapSuperClass(inheritanceConfiguration)
					.withJoinedTable()
					.build(persistenceContext);
			
			// as an inherited entity, the table should be in the context
			Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(Iterables.collectToList(tables, Table::getName).contains(Vehicle.class.getSimpleName())).isTrue();
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			ExecutableQuery<Car> carExecutableQuery = persistenceContext.newQuery(select(mappedSuperClassData.carTable.idColumn, mappedSuperClassData.carTable.modelColumn, mappedSuperClassData.vehicleTable.colorColumn)
					.from(mappedSuperClassData.carTable).innerJoin(mappedSuperClassData.carTable.idColumn, mappedSuperClassData.vehicleTable.idColumn), Car.class)
					.mapKey(Car::new, mappedSuperClassData.carTable.idColumn)
					.map(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.map(mappedSuperClassData.vehicleTable.colorColumn, Car::setColor);
			Set<Car> allCars = carExecutableQuery.execute(Accumulators.toSet());
			assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void withEmbeddable() {
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(Vehicle.class, LONG_TYPE)
					.mapKey(Vehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.embed(Vehicle::getColor, embeddableBuilder(Color.class)
							.map(Color::getRgb))
					.getConfiguration();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.mapSuperClass(inheritanceConfiguration)
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
			
			// checking with query to understand what's under the hood : rgb column is created instead of color
			ExecutableQuery<Car> carExecutableQuery = persistenceContext.newQuery("select id, model, rgb from Car", Car.class)
					.mapKey((SerializableFunction<Identifier<Long>, Car>) Car::new, "id", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model", Car::setModel)
					.map("rgb", Car::setColor, int.class, Color::new);
			Set<Car> allCars = carExecutableQuery.execute(Accumulators.toSet());
			assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
		}

		@Test
		void twiceInheritanceInRelation_schemaContainsAllTables() {
			FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> abstractVehicleConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, databaseAutoIncrement());
			FluentEntityMappingBuilder<Person, Identifier<Long>> personConfiguration = entityBuilder(Person.class, LONG_TYPE)
					.mapKey(Person::getId, databaseAutoIncrement())
					.map(Person::getName)
					.mapOneToOne(Person::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
							.mapSuperClass(abstractVehicleConfiguration))
						.mappedBy(Vehicle::getOwner)
					.mapOneToOne(Person::getMainBicycle, entityBuilder(Bicycle.class, LONG_TYPE)
							.mapSuperClass(abstractVehicleConfiguration)
							.map(Bicycle::getColor))
					.mappedBy(Bicycle::getOwner);
			
			personConfiguration.build(persistenceContext);
			
			Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables).extracting(Table::getName).containsExactlyInAnyOrder("Person", "Vehicle", "Bicycle");
			assertThat(tables.stream().flatMap(table -> table.getForeignKeys().stream()))
							.extracting(ForeignKey::getName).containsExactlyInAnyOrder("FK_Bicycle_ownerId_Person_id", "FK_Vehicle_ownerId_Person_id");

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			assertThat(ddlDeployer.getCreationScripts()).containsExactly(
					"create table Bicycle(color int, id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, ownerId int, unique (id))",
					"create table Person(name varchar(255), id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, unique (id))",
					"create table Vehicle(id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, ownerId int, unique (id))",
					"alter table Bicycle add constraint FK_Bicycle_ownerId_Person_id foreign key(ownerId) references Person(id)",
					"alter table Vehicle add constraint FK_Vehicle_ownerId_Person_id foreign key(ownerId) references Person(id)"
			);
		}
	}
	
	/**
	 * Data and method for common tests of building a strategy from a mapped super class
	 */
	static class MappedSuperClassData {
		
		abstract class AbstractVehicleTable<SELF extends AbstractVehicleTable<SELF>> extends Table<SELF> {
			final Column<SELF, Identifier<Long>> idColumn = addColumn("id", (Class<Identifier<Long>>) (Class) Identifier.class).primaryKey();
			final Column<SELF, Color> colorColumn = addColumn("color", Color.class);
			
			public AbstractVehicleTable(String name) {
				super(name);
			}
		}
		
		class VehicleTable extends AbstractVehicleTable<VehicleTable> {
			
			public VehicleTable(String name) {
				super(name);
			}
		}
		
		class CarTable extends AbstractVehicleTable<CarTable> {
			final Column<CarTable, String> modelColumn = addColumn("model", String.class);
			
			public CarTable(String name) {
				super(name);
			}
		}
		
		private final VehicleTable vehicleTable = new VehicleTable("vehicle");
		
		private final CarTable carTable = new CarTable("car");
	}
	
}
