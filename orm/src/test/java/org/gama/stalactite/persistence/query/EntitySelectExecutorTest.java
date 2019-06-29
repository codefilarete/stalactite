package org.gama.stalactite.persistence.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.gama.lang.Strings;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.result.InMemoryResultSet;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Operator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.gama.lang.function.Functions.chain;
import static org.gama.lang.function.Functions.link;
import static org.gama.lang.test.Assertions.assertAllEquals;
import static org.gama.lang.test.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntitySelectExecutorTest {
	
	private Dialect dialect;
	private ConnectionProvider connectionProviderMock;
	private ArgumentCaptor<String> sqlCaptor;
	private Connection connectionMock;
	
	@BeforeEach
	void initTest() {
		dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "bigint");
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identified.class, "bigint");
	}
	
	private void createConnectionProvider(List<Map<String, Object>> data) {
		// creation of a Connection that will give our test case data
		connectionProviderMock = mock(ConnectionProvider.class);
		connectionMock = mock(Connection.class);
		when(connectionProviderMock.getCurrentConnection()).thenReturn(connectionMock);
		try {
			PreparedStatement statementMock = mock(PreparedStatement.class);
			sqlCaptor = ArgumentCaptor.forClass(String.class);
			when(connectionMock.prepareStatement(any())).thenReturn(statementMock);
			when(statementMock.executeQuery()).thenReturn(new InMemoryResultSet(data));
		} catch (SQLException e) {
			// impossible since there's no real database connection
			throw Exceptions.asRuntimeException(e);
		}
	}
	
	@Test
	void select_simpleCase() {
		createConnectionProvider(Arrays.asList(
				Maps.asMap("Country_name", (Object) "France").add("Country_id", 12L)
		));
		
		Persister<Country, Identifier, Table> persister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.build(new PersistenceContext(connectionProviderMock, dialect));
		
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		JoinedStrategiesSelect<Country, Identifier, Table> joinedStrategiesSelect =
				((JoinedTablesPersister<Country, Identifier, Table>) persister).getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect();
		EntitySelectExecutor<Country, Identifier, Table> testInstance = new EntitySelectExecutor<>(joinedStrategiesSelect, connectionProviderMock, columnBinderRegistry);
		
		EntityCriteria<Country> countryEntityCriteriaSupport = new EntityCriteriaSupport<>(persister.getMappingStrategy(), Country::getName, Operator.eq(""))
				// actually we don't care about criteria since data is hardly tied to the connection (see createConnectionProvider(..))
				.and(Country::getId, Operator.<Identifier>in(new PersistedIdentifier<>(11L)))
				.and(Country::getName, Operator.eq("toto"));
		
		List<Country> select = testInstance.select(((EntityCriteriaSupport<Country>) countryEntityCriteriaSupport));
		Country expectedCountry = new Country(new PersistedIdentifier<>(12L));
		expectedCountry.setName("France");
		assertEquals(expectedCountry, Iterables.first(select), Country::getId);
		assertEquals(expectedCountry, Iterables.first(select), Country::getName);
	}
	
	@Test
	void select_embedCase() throws SQLException {
		createConnectionProvider(Arrays.asList(
				Maps.asMap("Country_name", (Object) "France").add("Country_id", 12L)
						.add("Country_creationDate", new java.sql.Timestamp(0)).add("Country_modificationDate", new java.sql.Timestamp(0))
		));
		
		Persister<Country, Identifier, Table> persister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.embed(Country::getTimestamp)
				.build(new PersistenceContext(connectionProviderMock, dialect));
		
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		JoinedStrategiesSelect<Country, Identifier, Table> joinedStrategiesSelect =
				((JoinedTablesPersister<Country, Identifier, Table>) persister).getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect();
		EntitySelectExecutor<Country, Identifier, Table> testInstance = new EntitySelectExecutor<>(joinedStrategiesSelect, connectionProviderMock, columnBinderRegistry);
		
		EntityCriteria<Country> countryEntityCriteriaSupport = new EntityCriteriaSupport<>(persister.getMappingStrategy(), Country::getName, Operator.eq(""))
				// actually we don't care about criteria since data is hardly tied to the connection (see createConnectionProvider(..))
				.and(Country::getId, Operator.<Identifier>in(new PersistedIdentifier<>(11L)))
				.and(Country::getName, Operator.eq("toto"))
				// but we care about this since we can check that criteria on embedded values works
				.and(Country::getTimestamp, Timestamp::getModificationDate, Operator.eq(new Date()));
		
		List<Country> select = testInstance.select(((EntityCriteriaSupport<Country>) countryEntityCriteriaSupport));
		Country expectedCountry = new Country(new PersistedIdentifier<>(12L));
		expectedCountry.setName("France");
		expectedCountry.setTimestamp(new Timestamp(new Date(0), new Date(0)));
		assertEquals(expectedCountry, Iterables.first(select), Country::getId);
		assertEquals(expectedCountry, Iterables.first(select), Country::getName);
		assertEquals(expectedCountry, Iterables.first(select), chain(Country::getTimestamp, Timestamp::getCreationDate));
		assertEquals(expectedCountry, Iterables.first(select), chain(Country::getTimestamp, Timestamp::getModificationDate));
		
		// checking that criteria is in the where clause
		verify(connectionMock).prepareStatement(sqlCaptor.capture());
		assertEquals("and Country.name = ? and Country.modificationDate = ?)", sqlCaptor.getValue(), s -> Strings.tail(s, 29));
	}
	
	@Test
	void select_oneToOneCase() throws SQLException {
		createConnectionProvider(Arrays.asList(
				Maps.asMap("Country_name", (Object) "France").add("Country_id", 12L).add("Country_capitalId", 42L)
						.add("City_id", 42L).add("City_name", "Paris")
		));
		
		JoinedTablesPersister<Country, Identifier, Table> persister = (JoinedTablesPersister<Country, Identifier, Table>)
				FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.addOneToOne(Country::getCapital, FluentEntityMappingConfigurationSupport.from(City.class, Identifier.class)
						.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName)
						.getConfiguration())
				.build(new PersistenceContext(connectionProviderMock, dialect));
		
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		JoinedStrategiesSelect<Country, Identifier, Table> joinedStrategiesSelect =
				persister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect();
		EntitySelectExecutor<Country, Identifier, Table> testInstance = new EntitySelectExecutor<>(joinedStrategiesSelect, connectionProviderMock, columnBinderRegistry);
		
		EntityCriteria<Country> countryEntityCriteriaSupport = persister
				// actually we don't care about criteria since data is hardly tied to the connection (see createConnectionProvider(..))
				.selectWhere(Country::getId, Operator.<Identifier>in(new PersistedIdentifier<>(11L)))
				.and(Country::getName, Operator.eq("toto"))
				.and(Country::getCapital, City::getName, Operator.eq("Grenoble"));
		
		List<Country> select = testInstance.select(((EntityCriteriaSupport<Country>) countryEntityCriteriaSupport));
		Country expectedCountry = new Country(new PersistedIdentifier<>(12L));
		expectedCountry.setName("France");
		City capital = new City(new PersistedIdentifier<>(42L));
		capital.setName("Paris");
		expectedCountry.setCapital(capital);
		assertEquals(expectedCountry, Iterables.first(select), Country::getId);
		assertEquals(expectedCountry, Iterables.first(select), Country::getName);
		assertEquals(expectedCountry, Iterables.first(select), link(Country::getCapital, City::getId));
		assertEquals(expectedCountry, Iterables.first(select), link(Country::getCapital, City::getName));
		
		// checking that criteria is in the where clause
		verify(connectionMock).prepareStatement(sqlCaptor.capture());
		assertEquals("and Country.name = ? and City.name = ?)", sqlCaptor.getValue(), s -> Strings.tail(s, 29));
	}
	
	@Test
	void select_oneToManyCase() throws SQLException {
		createConnectionProvider(Arrays.asList(
				Maps.asMap("Country_name", (Object) "France").add("Country_id", 12L)
						.add("Country_cities_Country_id", 12L).add("Country_cities_City_id", 42L)
						.add("City_id", 42L).add("City_name", "Paris"),
				Maps.asMap("Country_name", (Object) "France").add("Country_id", 12L)
						.add("Country_cities_Country_id", 12L).add("Country_cities_City_id", 43L)
						.add("City_id", 43L).add("City_name", "Lyon"),
				Maps.asMap("Country_name", (Object) "France").add("Country_id", 12L)
						.add("Country_cities_Country_id", 12L).add("Country_cities_City_id", 44L)
						.add("City_id", 44L).add("City_name", "Grenoble")
		));
		
		JoinedTablesPersister<Country, Identifier, Table> persister = (JoinedTablesPersister<Country, Identifier, Table>)
				FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.addOneToManySet(Country::getCities, FluentEntityMappingConfigurationSupport.from(City.class, Identifier.class)
						.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName)
						.getConfiguration())
				.build(new PersistenceContext(connectionProviderMock, dialect));
		
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		JoinedStrategiesSelect<Country, Identifier, Table> joinedStrategiesSelect =
				persister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect();
		EntitySelectExecutor<Country, Identifier, Table> testInstance = new EntitySelectExecutor<>(joinedStrategiesSelect, connectionProviderMock, columnBinderRegistry);
		
		// actually we don't care about criteria since data is hardly tied to the connection (see createConnectionProvider(..))
		EntityCriteria<Country> countryEntityCriteriaSupport =
				persister.selectWhere(Country::getId, Operator.<Identifier>in(new PersistedIdentifier<>(11L)))
				.and(Country::getName, Operator.eq("toto"))
				.andMany(Country::getCities, City::getName, Operator.eq("Grenoble"));
		
		
		Country expectedCountry = new Country(new PersistedIdentifier<>(12L));
		expectedCountry.setName("France");
		City paris = new City(new PersistedIdentifier<>(42L));
		paris.setName("Paris");
		City lyon = new City(new PersistedIdentifier<>(43L));
		lyon.setName("Lyon");
		City grenoble = new City(new PersistedIdentifier<>(44L));
		grenoble.setName("Grenoble");
		expectedCountry.setCities(Arrays.asSet(paris, lyon, grenoble));
		
		// we must wrap the select call into the select listener because it is the way expected by ManyCascadeConfigurer to initialize some variables (ThreadLocal ones)
		List<Country> select = persister.getPersisterListener().doWithSelectListener(Collections.emptyList(), () ->
				testInstance.select(((EntityCriteriaSupport<Country>) countryEntityCriteriaSupport))
		);
		
		assertEquals(expectedCountry, Iterables.first(select), Country::getId);
		assertEquals(expectedCountry, Iterables.first(select), Country::getName);
		assertAllEquals(expectedCountry.getCities(), Iterables.first(select).getCities(), City::getId);
		assertAllEquals(expectedCountry.getCities(), Iterables.first(select).getCities(), City::getName);
		
		// checking that criteria is in the where clause
		verify(connectionMock).prepareStatement(sqlCaptor.capture());
		assertEquals("and Country.name = ? and City.name = ?)", sqlCaptor.getValue(), s -> Strings.tail(s, 29));
	}
	
}