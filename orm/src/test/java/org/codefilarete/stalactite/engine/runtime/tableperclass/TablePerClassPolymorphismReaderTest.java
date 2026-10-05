package org.codefilarete.stalactite.engine.runtime.tableperclass;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Realm;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Town;
import org.codefilarete.stalactite.engine.model.Truck;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.engine.model.Village;
import org.codefilarete.stalactite.engine.runtime.test.UseCase;
import org.codefilarete.stalactite.engine.runtime.test.UseCase.Relation;
import org.codefilarete.stalactite.id.AbstractIdentifier;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.trace.ObjectPrinterBuilder;
import org.codefilarete.trace.ObjectPrinterBuilder.ObjectPrinter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.subentityBuilder;
import static org.codefilarete.stalactite.engine.runtime.test.UseCase.Polymorphism.TABLE_PER_CLASS;
import static org.codefilarete.stalactite.engine.runtime.test.UseCase.Relation.Fetch.JOIN;
import static org.codefilarete.stalactite.engine.runtime.test.UseCase.Relation.Fetch.SEPARATE_QUERY;
import static org.codefilarete.stalactite.engine.runtime.test.UseCase.Relation.RelationOwnership.BY_ASSOCIATION_TABLE;
import static org.codefilarete.stalactite.engine.runtime.test.UseCase.Relation.RelationOwnership.BY_SOURCE;
import static org.codefilarete.stalactite.engine.runtime.test.UseCase.Relation.RelationOwnership.BY_TARGET;
import static org.codefilarete.stalactite.engine.runtime.test.UseCase.Relation.Type.MANY_TO_MANY;
import static org.codefilarete.stalactite.engine.runtime.test.UseCase.Relation.Type.MANY_TO_ONE;
import static org.codefilarete.stalactite.engine.runtime.test.UseCase.Relation.Type.ONE_TO_MANY;
import static org.codefilarete.stalactite.engine.runtime.test.UseCase.Relation.Type.ONE_TO_ONE;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

class TablePerClassPolymorphismReaderTest {
	
	private static final Dialect DIALECT = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		DIALECT.getSqlTypeRegistry().put(Color.class, "int");
	}
	
	@Test
	@UseCase(relation = @Relation(type = ONE_TO_ONE, polymorphism = TABLE_PER_CLASS, ownership = BY_SOURCE, fetch = JOIN))
	void oneToTablePerClassOne() {
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
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
								.addSubClass(subentityBuilder(Truck.class), "TRUCK")
								.addSubClass(subentityBuilder(Car.class), "CAR")
						);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToOne(Person::getVehicle, vehicleConfiguration)
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
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
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
		ObjectPrinter<Vehicle> vehiclePrinter = new ObjectPrinterBuilder<Vehicle>()
				.addProperty(Vehicle::getId)
				.addProperty(Vehicle::getClass)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getId)
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getVehicle)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
	}
	
	@Test
	@UseCase(relation = @Relation(type = ONE_TO_ONE, polymorphism = TABLE_PER_CLASS, ownership = BY_SOURCE, fetch = SEPARATE_QUERY))
	void oneToTablePerClassOne_fetchSeparately() {
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
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
								.addSubClass(subentityBuilder(Truck.class), "TRUCK")
								.addSubClass(subentityBuilder(Car.class), "CAR")
						);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToOne(Person::getVehicle, vehicleConfiguration)
					.fetchSeparately()
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
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
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
		ObjectPrinter<Vehicle> vehiclePrinter = new ObjectPrinterBuilder<Vehicle>()
				.addProperty(Vehicle::getId)
				.addProperty(Vehicle::getClass)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getId)
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getVehicle)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
	}
	
	@Test
	@UseCase(relation = @Relation(type = ONE_TO_ONE, polymorphism = TABLE_PER_CLASS, ownership = BY_TARGET, fetch = JOIN))
	void oneToTablePerClassOne_ownedByTarget() {
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
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
								.addSubClass(subentityBuilder(Truck.class), "TRUCK")
								.addSubClass(subentityBuilder(Car.class), "CAR")
						);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToOne(Person::getVehicle, vehicleConfiguration)
					.mappedBy(Vehicle::getOwner)
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// insert
		Person person = new Person(1);
		Car car = new Car(42L);
		car.setColor(new Color(17));
		person.setVehicle(car);
		car.setOwner(person);
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion
		ObjectPrinter<Person> personIdPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getId)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Vehicle> vehiclePrinter = new ObjectPrinterBuilder<Vehicle>()
				.addProperty(Vehicle::getId)
				.addProperty(Vehicle::getClass)
				.addProperty(Vehicle::getOwner)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.withPrinter(Person.class, personIdPrinter::toString)
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getId)
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getVehicle)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
	}
	
	@Test
	@UseCase(relation = @Relation(type = ONE_TO_ONE, polymorphism = TABLE_PER_CLASS, ownership = BY_TARGET, fetch = SEPARATE_QUERY))
	void oneToTablePerClassOne_ownedByTarget_fetchSeparately() {
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
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
								.addSubClass(subentityBuilder(Truck.class), "TRUCK")
								.addSubClass(subentityBuilder(Car.class), "CAR")
						);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToOne(Person::getVehicle, vehicleConfiguration)
					.mappedBy(Vehicle::getOwner)
					.fetchSeparately()
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// insert
		Person person = new Person(1);
		Car car = new Car(42L);
		car.setColor(new Color(17));
		person.setVehicle(car);
		car.setOwner(person);
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion
		ObjectPrinter<Person> personIdPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getId)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Vehicle> vehiclePrinter = new ObjectPrinterBuilder<Vehicle>()
				.addProperty(Vehicle::getId)
				.addProperty(Vehicle::getClass)
				.addProperty(Vehicle::getOwner)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.withPrinter(Person.class, personIdPrinter::toString)
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getId)
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getVehicle)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
	}
	
	@Test
	@UseCase(relation = @Relation(type = ONE_TO_MANY, polymorphism = TABLE_PER_CLASS, ownership = BY_ASSOCIATION_TABLE, fetch = JOIN))
	void oneToTablePerClassMany() {
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
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
								.addSubClass(subentityBuilder(Truck.class), "TRUCK")
								.addSubClass(subentityBuilder(Car.class), "CAR")
						);
		
		Comparator<AbstractVehicle> vehicleComparator = Comparator.comparing(v -> v.getId().getDelegate());
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToMany(Person::getVehicles, vehicleConfiguration)
					.initializeWith(() -> new TreeSet<>(vehicleComparator))	// only for test stability
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// insert
		Person person = new Person(1);
		Car car = new Car(42L);
		car.setColor(new Color(42));
		Truck truck = new Truck(17L);
		person.setVehicles(Arrays.asTreeSet(vehicleComparator, car, truck));
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
		ObjectPrinter<AbstractVehicle> vehiclePrinter = new ObjectPrinterBuilder<AbstractVehicle>()
				.addProperty(AbstractVehicle::getId)
				.addProperty(Vehicle::getColor)
				.withPrinter(Identifier.class, Functions.chain(Identifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getVehicles, AbstractVehicle.class)
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
	}
	
	@Test
	@UseCase(relation = @Relation(type = ONE_TO_MANY, polymorphism = TABLE_PER_CLASS, ownership = BY_ASSOCIATION_TABLE, fetch = SEPARATE_QUERY))
	void oneToTablePerClassMany_fetchSeparately() {
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
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
								.addSubClass(subentityBuilder(Truck.class), "TRUCK")
								.addSubClass(subentityBuilder(Car.class), "CAR")
						);
		
		Comparator<AbstractVehicle> vehicleComparator = Comparator.comparing(v -> v.getId().getDelegate());
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToMany(Person::getVehicles, vehicleConfiguration)
					.fetchSeparately()
					.initializeWith(() -> new TreeSet<>(vehicleComparator))	// only for test stability
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// insert
		Person person = new Person(1);
		Car car = new Car(42L);
		car.setColor(new Color(42));
		Truck truck = new Truck(17L);
		person.setVehicles(Arrays.asTreeSet(vehicleComparator, car, truck));
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
		ObjectPrinter<AbstractVehicle> vehiclePrinter = new ObjectPrinterBuilder<AbstractVehicle>()
				.addProperty(AbstractVehicle::getId)
				.addProperty(Vehicle::getColor)
				.withPrinter(Identifier.class, Functions.chain(Identifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getVehicles, AbstractVehicle.class)
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
	}
	
	@Test
	@UseCase(relation = @Relation(type = ONE_TO_MANY, polymorphism = TABLE_PER_CLASS, ownership = BY_ASSOCIATION_TABLE, fetch = JOIN))
	void oneToTablePerClassMany_indexed() {
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
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
								.addSubClass(subentityBuilder(Truck.class), "TRUCK")
								.addSubClass(subentityBuilder(Car.class), "CAR")
						);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToMany(Person::getVehicles, vehicleConfiguration)
					.indexed()
					.initializeWith(KeepOrderSet::new)	// only for test stability
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// insert
		Person person = new Person(1);
		Car car = new Car(42L);
		car.setColor(new Color(42));
		Truck truck = new Truck(17L);
		List<Vehicle> vehicles = Arrays.asList(car, truck);
		Collections.shuffle(vehicles);	// we shuffle to check that order is important
		KeepOrderSet<Vehicle> treeSet = new KeepOrderSet<>(vehicles);
		person.setVehicles(treeSet);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
		ObjectPrinter<AbstractVehicle> vehiclePrinter = new ObjectPrinterBuilder<AbstractVehicle>()
				.addProperty(AbstractVehicle::getId)
				.addProperty(Vehicle::getColor)
				.withPrinter(Identifier.class, Functions.chain(Identifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getVehicles, AbstractVehicle.class)
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
	}
	
	@Test
	@UseCase(relation = @Relation(type = ONE_TO_MANY, polymorphism = TABLE_PER_CLASS, ownership = BY_ASSOCIATION_TABLE, fetch = JOIN))
	void oneToTablePerClassMany_indexed_fetchSeparately() {
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
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
								.addSubClass(subentityBuilder(Truck.class), "TRUCK")
								.addSubClass(subentityBuilder(Car.class), "CAR")
						);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToMany(Person::getVehicles, vehicleConfiguration)
					.indexed()
					.fetchSeparately()
					.initializeWith(KeepOrderSet::new)	// only for test stability
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// insert
		Person person = new Person(1);
		Car car = new Car(42L);
		car.setColor(new Color(42));
		Truck truck = new Truck(17L);
		List<Vehicle> vehicles = Arrays.asList(car, truck);
		Collections.shuffle(vehicles);	// we shuffle to check that order is important
		KeepOrderSet<Vehicle> treeSet = new KeepOrderSet<>(vehicles);
		person.setVehicles(treeSet);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
		ObjectPrinter<AbstractVehicle> vehiclePrinter = new ObjectPrinterBuilder<AbstractVehicle>()
				.addProperty(AbstractVehicle::getId)
				.addProperty(Vehicle::getColor)
				.withPrinter(Identifier.class, Functions.chain(Identifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getVehicles, AbstractVehicle.class)
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
	}
	
	@Test
	@UseCase(relation = @Relation(type = ONE_TO_MANY, polymorphism = TABLE_PER_CLASS, ownership = BY_TARGET, fetch = SEPARATE_QUERY))
	void oneToTablePerClassMany_ownedByTarget_fetchSeparately() {
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
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
								.addSubClass(subentityBuilder(Truck.class), "TRUCK")
								.addSubClass(subentityBuilder(Car.class), "CAR")
						);
		
		Comparator<AbstractVehicle> vehicleComparator = Comparator.comparing(v -> v.getId().getDelegate());
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToMany(Person::getVehicles, vehicleConfiguration)
					.fetchSeparately()
					.mappedBy(AbstractVehicle::getOwner)
					.initializeWith(() -> new TreeSet<>(vehicleComparator))	// only for test stability
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// insert
		Person person1 = new Person(1);
		Car car1 = new Car(142L);
		car1.setColor(new Color(42));
		Truck truck1 = new Truck(117L);
		person1.setVehicles(Arrays.asTreeSet(vehicleComparator, car1, truck1));
		Person person2 = new Person(2);
		Car car2 = new Car(242L);
		car2.setColor(new Color(42));
		Truck truck2 = new Truck(217L);
		person2.setVehicles(Arrays.asTreeSet(vehicleComparator, car2, truck2));
		
		personPersister.insert(person1, person2);
		
		Set<Person> loadedPersons = personPersister.select(person1.getId(), person2.getId());
		Map<Identifier<Long>, Person> loadedPersonsPerId = Iterables.map(loadedPersons, Person::getId);
		Person loadedPerson1 = loadedPersonsPerId.get(person1.getId());
		Person loadedPerson2 = loadedPersonsPerId.get(person2.getId());
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
		ObjectPrinter<Person> personIdPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getId)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<AbstractVehicle> vehiclePrinter = new ObjectPrinterBuilder<AbstractVehicle>()
				.addProperty(AbstractVehicle::getId)
				.addProperty(Vehicle::getColor)
				.addProperty(Vehicle::getOwner)
				.withPrinter(Identifier.class, Functions.chain(Identifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getVehicles, AbstractVehicle.class)
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		assertThat(loadedPerson1)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person1);
		
		assertThat(loadedPerson2)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person2);
	}
	
	@Test
	@UseCase(relation = @Relation(type = MANY_TO_MANY, polymorphism = TABLE_PER_CLASS, fetch = SEPARATE_QUERY))
	void manyToTablePerClassMany_fetchSeparately() {
		PersistenceContext persistenceContext = new PersistenceContext(dataSource, DIALECT);
		
		FluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
				embeddableBuilder(Person.class)
						.map(Person::getName)
						.embed(Person::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate));
		
		FluentEntityMappingBuilder<City, Identifier<Long>> vehicleConfiguration = entityBuilder(City.class, LONG_TYPE)
				.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(City::getName)
				.mapPolymorphism(PolymorphismPolicy.<City>tablePerClass()
						.addSubClass(subentityBuilder(Village.class)
								.map(Village::getBarCount), "V")
						.addSubClass(subentityBuilder(Town.class)
								.map(Town::getDiscotecCount), "T")
				);
		
		Comparator<City> vehicleComparator = Comparator.comparing(v -> v.getId().getDelegate());
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Person::getResidences, vehicleConfiguration)
					.fetchSeparately()
					.initializeWith(() -> new TreeSet<>(vehicleComparator))	// only for test stability
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// insert
		Village grenoble = new Village(42L);
		grenoble.setName("Grenoble");
		grenoble.setBarCount(51);
		Town lyon = new Town(17L);
		lyon.setDiscotecCount(123);
		lyon.setName("Lyon");
		Town paris = new Town(19L);
		paris.setName("Paris");
		
		Person person1 = new Person(1);
		Person person2 = new Person(2);
		
		person1.setResidences(Arrays.asTreeSet(vehicleComparator, grenoble, paris));
		person2.setResidences(Arrays.asTreeSet(vehicleComparator, lyon, paris));
		
		grenoble.setPersons(Arrays.asHashSet(person1));
		lyon.setPersons(Arrays.asHashSet(person2));
		paris.setPersons(Arrays.asHashSet(person1, person2));
		
		personPersister.insert(person1, person2);
		
		Set<Person> loadedPersons = personPersister.select(person1.getId(), person2.getId());
		Map<Identifier<Long>, Person> loadedPersonsPerId = Iterables.map(loadedPersons, Person::getId);
		Person loadedPerson1 = loadedPersonsPerId.get(person1.getId());
		Person loadedPerson2 = loadedPersonsPerId.get(person2.getId());
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
		ObjectPrinter<City> cityIdPrinter = new ObjectPrinterBuilder<City>()
				.addProperty(City::getId)
				.addProperty(City::getName)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getResidences, City.class)
				.withPrinter(City.class, cityIdPrinter::toString)
				.build();
		
		assertThat(loadedPerson1)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person1);
		
		assertThat(loadedPerson2)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person2);
	}
	
	@Test
	@UseCase(relation = @Relation(type = MANY_TO_MANY, polymorphism = TABLE_PER_CLASS, fetch = SEPARATE_QUERY))
	void manyToTablePerClassMany_indexed_fetchSeparately() {
		PersistenceContext persistenceContext = new PersistenceContext(dataSource, DIALECT);
		
		FluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
				embeddableBuilder(Person.class)
						.map(Person::getName)
						.embed(Person::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate));
		
		FluentEntityMappingBuilder<City, Identifier<Long>> vehicleConfiguration = entityBuilder(City.class, LONG_TYPE)
				.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(City::getName)
				.mapPolymorphism(PolymorphismPolicy.<City>tablePerClass()
						.addSubClass(subentityBuilder(Village.class)
								.map(Village::getBarCount), "V")
						.addSubClass(subentityBuilder(Town.class)
								.map(Town::getDiscotecCount), "T")
				);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapManyToMany(Person::getResidences, vehicleConfiguration)
					.fetchSeparately()
					.indexed()
					.initializeWith(KeepOrderSet::new)	// to be coherent with indexed nature of the relation
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// insert
		Village grenoble = new Village(42L);
		grenoble.setName("Grenoble");
		grenoble.setBarCount(51);
		Town lyon = new Town(17L);
		lyon.setDiscotecCount(123);
		lyon.setName("Lyon");
		Town paris = new Town(19L);
		paris.setName("Paris");
		
		Person person1 = new Person(1);
		Person person2 = new Person(2);
		
		List<City> cities1 = Arrays.asList(grenoble, paris);
		Collections.shuffle(cities1);
		person1.setResidences(new KeepOrderSet<>(cities1));
		List<City> cities2 = Arrays.asList(lyon, paris);
		Collections.shuffle(cities2);
		person2.setResidences(new KeepOrderSet<>(cities2));
		
		grenoble.setPersons(Arrays.asHashSet(person1));
		lyon.setPersons(Arrays.asHashSet(person2));
		paris.setPersons(Arrays.asHashSet(person1, person2));
		
		personPersister.insert(person1, person2);
		
		Set<Person> loadedPersons = personPersister.select(person1.getId(), person2.getId());
		Map<Identifier<Long>, Person> loadedPersonsPerId = Iterables.map(loadedPersons, Person::getId);
		Person loadedPerson1 = loadedPersonsPerId.get(person1.getId());
		Person loadedPerson2 = loadedPersonsPerId.get(person2.getId());
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
		ObjectPrinter<City> cityIdPrinter = new ObjectPrinterBuilder<City>()
				.addProperty(City::getId)
				.addProperty(City::getName)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getResidences, City.class)
				.withPrinter(City.class, cityIdPrinter::toString)
				.build();
		
		assertThat(loadedPerson1)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person1);
		
		assertThat(loadedPerson2)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person2);
	}
	
	@Nested
	class ManyToOne {
		
		@Test
		@UseCase(relation = @Relation(type = MANY_TO_ONE, polymorphism = TABLE_PER_CLASS, ownership = BY_SOURCE, fetch = JOIN))
		void toTablePerClassOne() {
			PersistenceContext persistenceContext = new PersistenceContext(dataSource, DIALECT);
			
			FluentEmbeddableMappingBuilder<City> timestampedPersistentBeanMapping =
					embeddableBuilder(City.class)
							.map(City::getName)
							.embed(City::getTimestamp, embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate));
			
			FluentEntityMappingBuilder<Country, Identifier<Long>> vehicleConfiguration =
					entityBuilder(Country.class, LONG_TYPE)
							.mapKey(Country::getId, ALREADY_ASSIGNED)
							.map(Country::getName)
							.mapPolymorphism(PolymorphismPolicy.<Country>tablePerClass()
									.addSubClass(subentityBuilder(Realm.class), "REALM")
									.addSubClass(subentityBuilder(Republic.class), "REPUBLIC")
							);
			
			FluentEntityMappingBuilder<City, Identifier<Long>> personBuilder = entityBuilder(City.class, LONG_TYPE)
					.mapKey(City::getId, ALREADY_ASSIGNED)
					.mapManyToOne(City::getCountry, vehicleConfiguration)
					.mapSuperClass(timestampedPersistentBeanMapping);
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<City, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// insert
			Republic france = new Republic(42L);
			france.setName("France");
			City grenoble = new City(1);
			grenoble.setCountry(france);
			City lyon = new City(2);
			lyon.setCountry(france);
			Realm uk = new Realm(142L);
			uk.setName("United Kingdom");
			City london = new City(3);
			london.setCountry(uk);
			personPersister.insert(grenoble, lyon, london);
			
			Set<City> loadedCities = personPersister.select(Arrays.asList(grenoble.getId(), lyon.getId(), london.getId()));
			
			// we use a printer to compare our results because entities override equals() which only keep "id" into account
			// which is far from sufficient for ou checking
			// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
			ObjectPrinter<Country> vehiclePrinter = new ObjectPrinterBuilder<Country>()
					.addProperty(Country::getId)
					.addProperty(Country::getClass)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
					.build();
			ObjectPrinter<City> cityPrinter = new ObjectPrinterBuilder<City>()
					.addProperty(City::getId)
					.addProperty(City::getName)
					.addProperty(City::getTimestamp)
					.addProperty(City::getCountry)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
					.withPrinter(Country.class, vehiclePrinter::toString)
					.build();
			
			Map<Identifier<Long>, City> loadedCitiesPerId = Iterables.map(loadedCities, City::getId);
			City loadedCity1 = loadedCitiesPerId.get(grenoble.getId());
			City loadedCity2 = loadedCitiesPerId.get(lyon.getId());
			City loadedCity3 = loadedCitiesPerId.get(london.getId());
			
			assertThat(loadedCity1)
					.usingComparator(Comparator.comparing(cityPrinter::toString))
					.withRepresentation(new PartialRepresentation<>(City.class, cityPrinter))
					.isEqualTo(grenoble);
			
			assertThat(loadedCity2)
					.usingComparator(Comparator.comparing(cityPrinter::toString))
					.withRepresentation(new PartialRepresentation<>(City.class, cityPrinter))
					.isEqualTo(lyon);
			
			assertThat(loadedCity3)
					.usingComparator(Comparator.comparing(cityPrinter::toString))
					.withRepresentation(new PartialRepresentation<>(City.class, cityPrinter))
					.isEqualTo(london);
		}
		
		@Test
		@UseCase(relation = @Relation(type = MANY_TO_ONE, polymorphism = TABLE_PER_CLASS, ownership = BY_SOURCE, fetch = SEPARATE_QUERY))
		void toTablePerClassOne_fetchSeparately() {
			PersistenceContext persistenceContext = new PersistenceContext(dataSource, DIALECT);
			
			FluentEmbeddableMappingBuilder<City> timestampedPersistentBeanMapping =
					embeddableBuilder(City.class)
							.map(City::getName)
							.embed(City::getTimestamp, embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate));
			
			FluentEntityMappingBuilder<Country, Identifier<Long>> vehicleConfiguration =
					entityBuilder(Country.class, LONG_TYPE)
							.mapKey(Country::getId, ALREADY_ASSIGNED)
							.map(Country::getName)
							.mapPolymorphism(PolymorphismPolicy.<Country>tablePerClass()
									.addSubClass(subentityBuilder(Realm.class), "REALM")
									.addSubClass(subentityBuilder(Republic.class), "REPUBLIC")
							);
			
			FluentEntityMappingBuilder<City, Identifier<Long>> personBuilder = entityBuilder(City.class, LONG_TYPE)
					.mapKey(City::getId, ALREADY_ASSIGNED)
					.mapManyToOne(City::getCountry, vehicleConfiguration)
						.fetchSeparately()
					.mapSuperClass(timestampedPersistentBeanMapping);
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<City, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// insert
			Republic france = new Republic(42L);
			france.setName("France");
			City grenoble = new City(1);
			grenoble.setCountry(france);
			City lyon = new City(2);
			lyon.setCountry(france);
			Realm uk = new Realm(142L);
			uk.setName("United Kingdom");
			City london = new City(3);
			london.setCountry(uk);
			personPersister.insert(grenoble, lyon, london);
			
			Set<City> loadedCities = personPersister.select(Arrays.asList(grenoble.getId(), lyon.getId(), london.getId()));
			
			// we use a printer to compare our results because entities override equals() which only keep "id" into account
			// which is far from sufficient for ou checking
			// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
			ObjectPrinter<Country> vehiclePrinter = new ObjectPrinterBuilder<Country>()
					.addProperty(Country::getId)
					.addProperty(Country::getClass)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
					.build();
			ObjectPrinter<City> cityPrinter = new ObjectPrinterBuilder<City>()
					.addProperty(City::getId)
					.addProperty(City::getName)
					.addProperty(City::getTimestamp)
					.addProperty(City::getCountry)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
					.withPrinter(Country.class, vehiclePrinter::toString)
					.build();
			
			Map<Identifier<Long>, City> loadedCitiesPerId = Iterables.map(loadedCities, City::getId);
			City loadedCity1 = loadedCitiesPerId.get(grenoble.getId());
			City loadedCity2 = loadedCitiesPerId.get(lyon.getId());
			City loadedCity3 = loadedCitiesPerId.get(london.getId());
			
			assertThat(loadedCity1)
					.usingComparator(Comparator.comparing(cityPrinter::toString))
					.withRepresentation(new PartialRepresentation<>(City.class, cityPrinter))
					.isEqualTo(grenoble);
			
			assertThat(loadedCity2)
					.usingComparator(Comparator.comparing(cityPrinter::toString))
					.withRepresentation(new PartialRepresentation<>(City.class, cityPrinter))
					.isEqualTo(lyon);
			
			assertThat(loadedCity3)
					.usingComparator(Comparator.comparing(cityPrinter::toString))
					.withRepresentation(new PartialRepresentation<>(City.class, cityPrinter))
					.isEqualTo(london);
		}
	}
}
