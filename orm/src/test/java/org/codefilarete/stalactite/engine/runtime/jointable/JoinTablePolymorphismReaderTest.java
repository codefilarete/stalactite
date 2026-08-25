package org.codefilarete.stalactite.engine.runtime.jointable;

import java.util.Comparator;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PartialRepresentation;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Truck;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.AbstractIdentifier;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.trace.ObjectPrinterBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

class JoinTablePolymorphismReaderTest {
	
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
	void oneToJoinTable_crud() {
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
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
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
	}
}
