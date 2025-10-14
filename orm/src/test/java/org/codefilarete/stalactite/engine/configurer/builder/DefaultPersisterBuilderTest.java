package org.codefilarete.stalactite.engine.configurer.builder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PersisterRegistry.DefaultPersisterRegistry;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.function.Serie.IntegerSerie;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.dsl.MappingEase.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.dsl.MappingEase.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class DefaultPersisterBuilderTest {
	
	private DefaultDialect dialect;
	
	@BeforeEach
	void initializeDialect() {
		dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		dialect.getSqlTypeRegistry().put(Color.class, "int");
		// we ensure a steady column order in final SQL to have reproducible tests
		dialect.getDmlGenerator().sortColumnsAlphabetically();
	}
	
	@Test
	void build_connectionProviderIsNotRollbackObserver_throwsException() {
		FluentEntityMappingBuilder<Country, Identifier<Long>> persisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
				.versionedBy(Country::getVersion, new IntegerSerie())
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription);
		
		ConnectionConfigurationSupport notARollbackObserverConnectionConfiguration = new ConnectionConfigurationSupport(new CurrentThreadConnectionProvider(mock(DataSource.class)), 10);
		DefaultPersisterBuilder testInstance = new DefaultPersisterBuilder(
				dialect,
				notARollbackObserverConnectionConfiguration,
				mock(PersisterRegistry.class));
		assertThatThrownBy(() -> testInstance.build(persisterConfiguration))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessage("Version control is only supported for o.c.s.s.ConnectionProvider that implements o.c.s.s.RollbackObserver");
	}
	
	@Disabled	// TODO: reactivate and fix: build(..), from a semantic point of view, should build things, not return existing Persister
	@Test
	void build_twiceForSameClass_throwsException() {
		FluentEntityMappingBuilder<Car, Identifier<Long>> persisterConfiguration = entityBuilder(Car.class, Identifier.LONG_TYPE)
				.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Car::getModel)
				.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate));
		
		ConnectionConfigurationSupport connectionConfiguration = new ConnectionConfigurationSupport(mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS)), 10);
		DefaultPersisterBuilder testInstance = new DefaultPersisterBuilder(
				dialect,
				connectionConfiguration,
				new DefaultPersisterRegistry());
		testInstance.build(persisterConfiguration);
		assertThatCode(() -> testInstance.build(persisterConfiguration))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessage("Persister already exists for class o.c.s.e.m.Car");
	}
	
	@Test
	void build_singleTable_singleClass() throws SQLException {
		FluentEntityMappingBuilder<Car, Identifier<Long>> persisterConfiguration = entityBuilder(Car.class, Identifier.LONG_TYPE)
				.map(Car::getModel)
				.map(Car::getColor)
				.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate));
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		Connection connectionMock = mock(Connection.class);
		when(connectionProviderMock.giveConnection()).thenReturn(connectionMock);
		ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatementMock);
		when(preparedStatementMock.executeLargeBatch()).thenReturn(new long[] { 1 });
		
		DefaultPersisterBuilder testInstance = new DefaultPersisterBuilder(
				dialect,
				new ConnectionConfigurationSupport(connectionProviderMock, 10),
				new DefaultPersisterRegistry());
		testInstance.build(persisterConfiguration);
		EntityPersister<Car, Identifier<Long>> result = testInstance.build(persisterConfiguration);
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
		FluentEntityMappingBuilder<Car, Identifier<Long>> persisterConfiguration = entityBuilder(Car.class, Identifier.LONG_TYPE)
				.map(Car::getModel)
				.mapSuperClass(entityBuilder(Vehicle.class, Identifier.LONG_TYPE)
						.map(Vehicle::getColor)
						.mapSuperClass(entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
								.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
								.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
										.map(Timestamp::getCreationDate)
										.map(Timestamp::getModificationDate))
						)
				);
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		Connection connectionMock = mock(Connection.class);
		when(connectionProviderMock.giveConnection()).thenReturn(connectionMock);
		ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatementMock);
		when(preparedStatementMock.executeLargeBatch()).thenReturn(new long[] { 1 });
		
		DefaultPersisterBuilder testInstance = new DefaultPersisterBuilder(
				dialect,
				new ConnectionConfigurationSupport(connectionProviderMock, 10),
				new DefaultPersisterRegistry());
		EntityPersister<Car, Identifier<Long>> result = testInstance.build(persisterConfiguration);
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
		FluentEntityMappingBuilder<Car, Identifier<Long>> persisterConfiguration = entityBuilder(Car.class, Identifier.LONG_TYPE)
				.map(Car::getModel)
				.mapSuperClass(entityBuilder(Vehicle.class, Identifier.LONG_TYPE)
						.map(Vehicle::getColor)
						.mapSuperClass(entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
								.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
								.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
										.map(Timestamp::getCreationDate)
										.map(Timestamp::getModificationDate))
						).withJoinedTable()
				).withJoinedTable();
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		Connection connectionMock = mock(Connection.class);
		when(connectionProviderMock.giveConnection()).thenReturn(connectionMock);
		ArgumentCaptor<String> insertCaptor = ArgumentCaptor.forClass(String.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(insertCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatementMock);
		when(preparedStatementMock.executeLargeBatch()).thenReturn(new long[] { 1 });
		when(preparedStatementMock.executeQuery()).thenReturn(new InMemoryResultSet(Collections.emptyIterator()));
		
		DefaultPersisterBuilder testInstance = new DefaultPersisterBuilder(
				dialect,
				new ConnectionConfigurationSupport(connectionProviderMock, 10),
				new DefaultPersisterRegistry());
		EntityPersister<Car, Identifier<Long>> result = testInstance.build(persisterConfiguration);
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
		FluentEntityMappingBuilder<Car, Identifier<Long>> persisterConfiguration = entityBuilder(Car.class, Identifier.LONG_TYPE)
						.mapKey(Car::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Car::getModel);
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		Connection connectionMock = mock(Connection.class);
		when(connectionProviderMock.giveConnection()).thenReturn(connectionMock);
		ArgumentCaptor<String> insertCaptor = ArgumentCaptor.forClass(String.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(insertCaptor.capture())).thenReturn(preparedStatementMock);
		when(connectionMock.prepareStatement(insertCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(preparedStatementMock);
		when(preparedStatementMock.executeLargeBatch()).thenReturn(new long[] { 1 });
		when(preparedStatementMock.executeQuery()).thenReturn(new InMemoryResultSet(Arrays.asList(Maps.forHashMap(String.class, Object.class)
				.add("Car_id", 1L)
				.add("Car_model", "Renault"))));
		
		DefaultPersisterBuilder testInstance = new DefaultPersisterBuilder(
				dialect,
				new ConnectionConfigurationSupport(connectionProviderMock, 10),
				new DefaultPersisterRegistry());
		EntityPersister<Car, Identifier<Long>> result = testInstance.build(persisterConfiguration);
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
		FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> persisterConfiguration =
				entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
						.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate));
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		
		DefaultPersisterBuilder testInstance = new DefaultPersisterBuilder(
				dialect,
				new ConnectionConfigurationSupport(connectionProviderMock, 10),
				new DefaultPersisterRegistry());
		EntityPersister<AbstractVehicle, Identifier<Long>> result = testInstance.build(persisterConfiguration);
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
		FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> persisterConfiguration =
				entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
						.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate))
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
								.addSubClass(subentityBuilder(Vehicle.class, Identifier.LONG_TYPE)));
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		DefaultPersisterBuilder testInstance = new DefaultPersisterBuilder(
				dialect,
				new ConnectionConfigurationSupport(connectionProviderMock, 10),
				new DefaultPersisterRegistry());
		EntityPersister<AbstractVehicle, Identifier<Long>> result = testInstance.build(persisterConfiguration);
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
		FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> persisterConfiguration =
				entityBuilder(AbstractVehicle.class, Identifier.LONG_TYPE)
						.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.embed(AbstractVehicle::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate))
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
								.addSubClass(subentityBuilder(Vehicle.class, Identifier.LONG_TYPE), "Vehicle"));
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class, withSettings().defaultAnswer(Answers.RETURNS_MOCKS));
		DefaultPersisterBuilder testInstance = new DefaultPersisterBuilder(
				dialect,
				new ConnectionConfigurationSupport(connectionProviderMock, 10),
				new DefaultPersisterRegistry());
		EntityPersister<AbstractVehicle, Identifier<Long>> result = testInstance.build(persisterConfiguration);
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