package org.codefilarete.stalactite.engine.runtime.singletable;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PartialRepresentation;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Truck;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.AbstractIdentifier;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.trace.ObjectPrinterBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.subentityBuilder;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

class SingleTablePolymorphismWriterTest {
	
	private static final Dialect DIALECT = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
//	private final ConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(dataSource);
//	private PersistenceContext persistenceContext;
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		DIALECT.getSqlTypeRegistry().put(Color.class, "int");
	}
	
	@Test
	void crud() throws SQLException {
		PersistenceContext persistenceContext = new PersistenceContext(dataSource, DIALECT);
		ConnectionProvider connectionProvider = persistenceContext.getConnectionProvider();
		
		FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> personBuilder = entityBuilder(AbstractVehicle.class, LONG_TYPE)
				.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
						.addSubClass(subentityBuilder(Car.class)
								.map(Car::getId)
								.map(Car::getModel), "CAR")
						.addSubClass(subentityBuilder(Truck.class)
								.map(Truck::getId)
								.map(Truck::getColor), "TRUCK"));
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<AbstractVehicle, Identifier<Long>> persister = testInstance.resolve(personBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		Truck dummyTruck = new Truck(2L);
		dummyTruck.setColor(new Color(42));
		
		// insert test
		persister.insert(Arrays.asList(dummyCar, dummyTruck));
		
		Car dummyCarModified = new Car(1L);
		dummyCarModified.setModel("Peugeot");
		Truck dummyTruckModified = new Truck(2L);
		dummyTruckModified.setColor(new Color(99));
		
		persister.update(dummyCarModified, dummyCar, true);
		
		persister.update(dummyTruckModified, dummyTruck, true);
		
		connectionProvider.giveConnection().commit();
		persister.delete(dummyCarModified);
		persister.delete(dummyTruckModified);
		connectionProvider.giveConnection().rollback();
		
		persister.delete(Arrays.asList(dummyCarModified, dummyTruckModified));
		
		connectionProvider.giveConnection().rollback();
		
		assertThat(persister.select(dummyTruck.getId())).isEqualTo(dummyTruckModified);
		assertThat(persister.select(dummyCar.getId())).isEqualTo(dummyCarModified);
		assertThat(new HashSet<>(persister.select(Arrays.asSet(dummyCar.getId(), dummyTruck.getId())))).isEqualTo(Arrays.asSet(dummyCarModified,
				dummyTruckModified));
	}
	
	@Test
	void oneToSingleTable_crud() {
		PersistenceContext persistenceContext = new PersistenceContext(dataSource, DIALECT);
		
		FluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
				embeddableBuilder(Person.class)
						.map(Person::getName)
						.embed(Person::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate));
		
		FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleConfiguration =
				entityBuilder(Vehicle.class, LONG_TYPE)
						.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
						.map(Vehicle::getColor)
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
								.addSubClass(subentityBuilder(Truck.class), "T")
								.addSubClass(subentityBuilder(Car.class), "C")
						);
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Vehicle, Identifier<Long>> vehiclePersister = testInstance.resolve(vehicleConfiguration.getConfiguration());
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToOne(Person::getVehicle, vehicleConfiguration).cascading(ALL_ORPHAN_REMOVAL)
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// insert
		Person person = new Person(1);
		Car car = new Car(42L);
		car.setColor(new Color(17));
		person.setVehicle(car);
		personPersister.insert(person);
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson).isEqualTo(person);
		
		// updating embedded value
		person.setTimestamp(new Timestamp());
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
		ObjectPrinterBuilder.ObjectPrinter<Vehicle> vehiclePrinter = new ObjectPrinterBuilder<Vehicle>()
				.addProperty(Vehicle::getId)
				.addProperty(Vehicle::getClass)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinterBuilder.ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getId)
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
		// ensuring that reverse side is also set
		assertThat(loadedPerson.getVehicle().getOwner()).isEqualTo(loadedPerson);
		
		// updating one-to-one relation
		person.setVehicle(new Truck(666L));
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
		// checking for orphan removal (relation was marked as such)
		assertThat(vehiclePersister.select(new PersistedIdentifier<>(42L))).isNull();
		
		// nullifying one-to-one relation
		person.setVehicle(null);
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson).isEqualTo(person);
		// checking for orphan removal (relation was marked as such)
		assertThat(vehiclePersister.select(new PersistedIdentifier<>(666L))).isNull();
		
		
		// setting new one-to-one relation
		person.setVehicle(new Truck(17L));
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson).isEqualTo(person);
		
		// testing deletion
		personPersister.delete(person);
		assertThat(personPersister.select(person.getId())).isNull();
		// checking for orphan removal (relation was marked as such)
		assertThat(vehiclePersister.select(new PersistedIdentifier<>(17L))).isNull();
	}
}
