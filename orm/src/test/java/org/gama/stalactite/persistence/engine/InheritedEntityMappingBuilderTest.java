package org.gama.stalactite.persistence.engine;

import java.sql.SQLException;
import java.util.List;

import org.gama.lang.Duo;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.binder.LambdaParameterBinder;
import org.gama.sql.binder.NullAwareParameterBinder;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.Car;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.Color;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.Engine;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.Vehicle;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.gama.lang.collection.Iterables.first;
import static org.gama.sql.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
class InheritedEntityMappingBuilderTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	
	@BeforeAll
	static void init() {
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Color.class, "int");
	}
	
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	void initTest() {
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
	}
	
	@Test
	void crud() throws SQLException {
		
		Persister<Car, Identifier, Table> carPersister = buildCRUDContext();
		
		// insert
		Car myVehicle = new Car(1L);
		myVehicle.setColor(new Color(123456));
		myVehicle.setModel("Renault");
		carPersister.insert(myVehicle);
		
		List<Duo> rawCars = persistenceContext.newQuery("select id, model from tata", Duo.class)
				.mapKey(Duo::new, "id", long.class, "model", String.class)
				.execute(persistenceContext.getConnectionProvider());
		assertEquals(new Duo<>(1L, "Renault"), first(rawCars));
		assertEquals(1, rawCars.size());
		
		List<Duo> rawVehicles = persistenceContext.newQuery("select id, color from toto", Duo.class)
				.mapKey(Duo::new, "id", long.class, "color", int.class)
				.execute(persistenceContext.getConnectionProvider());
		assertEquals(new Duo<>(1L, 123456), first(rawVehicles));
		assertEquals(1, rawVehicles.size());
		
		// update
		Car myCarModified = new Car(1L);
		myCarModified.setColor(new Color(654321));
		myCarModified.setModel("Peugeot");
		carPersister.update(myCarModified, myVehicle, true);
		
		rawCars = persistenceContext.newQuery("select id, model from tata", Duo.class)
				.mapKey(Duo::new, "id", long.class, "model", String.class)
				.execute(persistenceContext.getConnectionProvider());
		assertEquals(new Duo<>(1L, "Peugeot"), first(rawCars));
		assertEquals(1, rawCars.size());
		
		rawVehicles = persistenceContext.newQuery("select id, color from toto", Duo.class)
				.mapKey(Duo::new, "id", long.class, "color", int.class)
				.execute(persistenceContext.getConnectionProvider());
		assertEquals(new Duo<>(1L, 654321), first(rawVehicles));
		
		// update by id
		myCarModified = new Car(1L);
		myCarModified.setColor(new Color(123456));
		myCarModified.setModel("Renault");
		carPersister.updateById(myCarModified);
		
		rawCars = persistenceContext.newQuery("select id, model from tata", Duo.class)
				.mapKey(Duo::new, "id", long.class, "model", String.class)
				.execute(persistenceContext.getConnectionProvider());
		assertEquals(new Duo<>(1L, "Renault"), first(rawCars));
		assertEquals(1, rawCars.size());
		
		rawVehicles = persistenceContext.newQuery("select id, color from toto", Duo.class)
				.mapKey(Duo::new, "id", long.class, "color", int.class)
				.execute(persistenceContext.getConnectionProvider());
		assertEquals(new Duo<>(1L, 123456), first(rawVehicles));
		
		// select
		Car selectedCar = carPersister.select(new PersistedIdentifier<>(1L));
		assertEquals(new PersistedIdentifier<>(1L), selectedCar.getId());
		assertEquals("Renault", selectedCar.getModel());
		assertEquals(123456, selectedCar.getColor().getRgb());
		
		// delete
		// commit is here to allow delelte by id test some lines below
		persistenceContext.getConnectionProvider().getCurrentConnection().commit();
		int deletedRowCount = carPersister.delete(myCarModified);
		assertEquals(1, deletedRowCount);
		rawCars = persistenceContext.newQuery("select id, model from tata", Duo.class)
				.mapKey(Duo::new, "id", long.class, "model", String.class)
				.execute(persistenceContext.getConnectionProvider());
		assertTrue(rawCars.isEmpty());
		
		rawVehicles = persistenceContext.newQuery("select id, color from toto", Duo.class)
				.mapKey(Duo::new, "id", long.class, "color", int.class)
				.execute(persistenceContext.getConnectionProvider());
		assertTrue(rawVehicles.isEmpty());
		
		// delete by id
		// rollback so we can delete car again (by id)
		persistenceContext.getConnectionProvider().getCurrentConnection().rollback();
		deletedRowCount = carPersister.deleteById(myCarModified);
		assertEquals(1, deletedRowCount);
		rawCars = persistenceContext.newQuery("select id, model from tata", Duo.class)
				.mapKey(Duo::new, "id", long.class, "model", String.class)
				.execute(persistenceContext.getConnectionProvider());
		assertTrue(rawCars.isEmpty());
		
		rawVehicles = persistenceContext.newQuery("select id, color from toto", Duo.class)
				.mapKey(Duo::new, "id", long.class, "color", int.class)
				.execute(persistenceContext.getConnectionProvider());
		assertTrue(rawVehicles.isEmpty());
	}
	
	private Persister<Car, Identifier, Table> buildCRUDContext() {
		Table superTable = new Table("toto");
		ClassMappingStrategy<Vehicle, Identifier, Table> superClassMapping = new FluentEntityMappingConfigurationSupport<Vehicle, Identifier>(Vehicle.class)
				.add(Vehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Vehicle::getColor)
				.build(DIALECT, superTable);
		
		Table inheritedTable = new Table("tata");
		FluentEntityMappingConfigurationSupport<Car, Identifier> configurationSupport = new FluentEntityMappingConfigurationSupport<>(Car.class);
		configurationSupport
				.add(Car::getModel)
				.mapInheritance(superClassMapping);
		
		InheritedEntityMappingBuilder<Car, Identifier> testInstance = new InheritedEntityMappingBuilder<>(configurationSupport);
		Persister<Car, Identifier, Table> carPersister = testInstance.build(persistenceContext, inheritedTable);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		return carPersister;
	}
	
	@Test
	void crud_withEmbedded() {
		Persister<Car, Identifier, Table> carPersister = buildCRUDEmbeddedContext();
		
		// insert
		Car myVehicle = new Car(1L);
		myVehicle.setColor(new Color(123456));
		myVehicle.setModel("Renault");
		carPersister.insert(myVehicle);
		
		List<Duo> rawCars = persistenceContext.newQuery("select id, model from tata", Duo.class)
				.mapKey(Duo::new, "id", long.class, "model", String.class)
				.execute(persistenceContext.getConnectionProvider());
		assertEquals(new Duo<>(1L, "Renault"), first(rawCars));
		assertEquals(1, rawCars.size());
		
		List<Duo> rawVehicles = persistenceContext.newQuery("select id, rgb from toto", Duo.class)
				.mapKey(Duo::new, "id", long.class, "rgb", int.class)
				.execute(persistenceContext.getConnectionProvider());
		assertEquals(new Duo<>(1L, 123456), first(rawVehicles));
		assertEquals(1, rawVehicles.size());
	}
	
	private Persister<Car, Identifier, Table> buildCRUDEmbeddedContext() {
		Table superTable = new Table("toto");
		ClassMappingStrategy<Vehicle, Identifier, Table> superClassMapping = new FluentEntityMappingConfigurationSupport<Vehicle, Identifier>(Vehicle.class)
				.add(Vehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Vehicle::getColor)
				.build(DIALECT, superTable);
		
		Table inheritedTable = new Table("tata");
		FluentEntityMappingConfigurationSupport<Car, Identifier> configurationSupport = new FluentEntityMappingConfigurationSupport<>(Car.class);
		configurationSupport
				.add(Car::getModel)
				.mapInheritance(superClassMapping);
		
		InheritedEntityMappingBuilder<Car, Identifier> testInstance = new InheritedEntityMappingBuilder<>(configurationSupport);
		Persister<Car, Identifier, Table> carPersister = testInstance.build(persistenceContext, inheritedTable);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		return carPersister;
	}
	
	@Test
	void crud_withOneToOne() {
		
		Persister<Car, Identifier, Table> carPersister = buildCRUDOneToOneContext();
		
		// insert
		Car myVehicle = new Car(1L);
		myVehicle.setColor(new Color(123456));
		myVehicle.setModel("Renault");
		carPersister.insert(myVehicle);
		
		List<Duo> rawCars = persistenceContext.newQuery("select id, model from tata", Duo.class)
				.mapKey(Duo::new, "id", long.class, "model", String.class)
				.execute(persistenceContext.getConnectionProvider());
		assertEquals(new Duo<>(1L, "Renault"), first(rawCars));
		assertEquals(1, rawCars.size());
		
		List<Duo> rawVehicles = persistenceContext.newQuery("select t.id, t.engineId, engine.id as enginePK from toto t left outer join engine on t.id = engine.id", Duo.class)
				.mapKey(Duo::new, "engineId", long.class, "enginePK", long.class)
				.execute(persistenceContext.getConnectionProvider());
		assertEquals(new Duo<>(1L, 1L), first(rawVehicles));
		assertEquals(1, rawVehicles.size());
	}
	
	private Persister<Car, Identifier, Table> buildCRUDOneToOneContext() {
		Persister<Engine, Long, Table> enginePersister = new FluentEntityMappingConfigurationSupport<Engine, Long>(Engine.class)
				.add(Engine::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.build(persistenceContext);
		
		Table superTable = new Table("toto");
		ClassMappingStrategy<Vehicle, Identifier, Table> superClassMapping = new FluentEntityMappingConfigurationSupport<Vehicle, Identifier>(Vehicle.class)
				.add(Vehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToOne(Vehicle::getEngine, enginePersister).cascading(RelationshipMode.ALL)
				.build(DIALECT, superTable);
		
		Table inheritedTable = new Table("tata");
		FluentEntityMappingConfigurationSupport<Car, Identifier> configurationSupport = new FluentEntityMappingConfigurationSupport<>(Car.class);
		configurationSupport
				.add(Car::getModel)
				.mapInheritance(superClassMapping);
		
		InheritedEntityMappingBuilder<Car, Identifier> testInstance = new InheritedEntityMappingBuilder<>(configurationSupport);
		Persister<Car, Identifier, Table> carPersister = testInstance.build(persistenceContext, inheritedTable);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		return carPersister;
	}
	
}