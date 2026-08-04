package org.codefilarete.stalactite.engine.runtime.jointable;

import java.sql.SQLException;
import java.util.HashSet;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Truck;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

class JoinTablePolymorphismWriterTest {
	
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
				.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
						.addSubClass(subentityBuilder(Car.class)
								.map(Car::getId)
								.map(Car::getModel))
						.addSubClass(subentityBuilder(Truck.class)
								.map(Truck::getId)
								.map(Truck::getColor)));
		
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
}
