package org.gama.stalactite.persistence.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.engine.EntityPersister.EntityCriteria;
import org.gama.stalactite.persistence.engine.EntityPersister.ExecutableEntityQuery;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.SimpleRelationalEntityPersister.CriteriaProvider;
import org.gama.stalactite.persistence.engine.runtime.OptimizedUpdatePersister;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Operators;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.DataSourceConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.result.InMemoryResultSet;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.gama.lang.function.Functions.chain;
import static org.gama.lang.function.Functions.link;
import static org.gama.stalactite.persistence.engine.MappingEase.embeddableBuilder;
import static org.gama.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.gama.stalactite.query.model.Operators.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntityGraphSelectExecutorTest {
	
	private Dialect dialect;
	private ConnectionProvider connectionProviderMock;
	private ArgumentCaptor<String> sqlCaptor;
	private Connection connectionMock;
	
	@BeforeEach
	void initTest() {
		dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "bigint");
	}
	
	private void createConnectionProvider(List<Map<String, Object>> data) {
		// creation of a Connection that will give our test case data
		connectionProviderMock = mock(ConnectionProvider.class);
		connectionMock = mock(Connection.class);
		when(connectionProviderMock.giveConnection()).thenReturn(connectionMock);
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
	void loadSelection_simpleCase() {
		createConnectionProvider(Arrays.asList(
				Maps.asMap("Country_name", (Object) "France").add("Country_id", 12L)
		));
		
		EntityConfiguredJoinedTablesPersister<Country, Identifier> persister = (EntityConfiguredJoinedTablesPersister<Country, Identifier>) entityBuilder(Country.class, Identifier.class)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.build(new PersistenceContext(connectionProviderMock, dialect));
		
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		EntityGraphSelectExecutor<Country, Identifier, Table> testInstance = new EntityGraphSelectExecutor<>(persister.getEntityJoinTree(), connectionProviderMock, columnBinderRegistry);
		
		EntityCriteriaSupport<Country> countryEntityCriteriaSupport = new EntityCriteriaSupport<>(persister.getMappingStrategy(), Country::getName, eq(""))
				// actually we don't care about criteria since data is hardly tied to the connection (see createConnectionProvider(..))
				.and(Country::getId, Operators.<Identifier>in(new PersistedIdentifier<>(11L)))
				.and(Country::getName, eq("toto"));
		
		// When
		List<Country> select = testInstance.loadSelection(countryEntityCriteriaSupport.getCriteria());
		Country expectedCountry = new Country(new PersistedIdentifier<>(12L));
		expectedCountry.setName("France");
		assertThat(Iterables.first(select))
				.usingComparator(Comparator.comparing(chain(Country::getId, Identifier::getSurrogate)))
				.isEqualTo(expectedCountry)
				.usingComparator(Comparator.comparing(Country::getName))
				.isEqualTo(expectedCountry);
	}
	
	@Test
	void loadSelection_embedCase() throws SQLException {
		createConnectionProvider(Arrays.asList(
				Maps.asMap("Country_name", (Object) "France").add("Country_id", 12L)
						.add("Country_creationDate", new java.sql.Timestamp(0)).add("Country_modificationDate", new java.sql.Timestamp(0))
		));
		
		EntityConfiguredJoinedTablesPersister<Country, Identifier> persister = (EntityConfiguredJoinedTablesPersister<Country, Identifier>) entityBuilder(Country.class, Identifier.class)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.embed(Country::getTimestamp, embeddableBuilder(Timestamp.class)
						.add(Timestamp::getCreationDate)
						.add(Timestamp::getModificationDate))
				.build(new PersistenceContext(connectionProviderMock, dialect));
		
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		EntityGraphSelectExecutor<Country, Identifier, Table> testInstance = new EntityGraphSelectExecutor<>(persister.getEntityJoinTree(), connectionProviderMock, columnBinderRegistry);
		
		EntityCriteriaSupport<Country> countryEntityCriteriaSupport = new EntityCriteriaSupport<>(persister.getMappingStrategy(), Country::getName, eq(""))
				// actually we don't care about criteria since data is hardly tied to the connection (see createConnectionProvider(..))
				.and(Country::getId, Operators.<Identifier>in(new PersistedIdentifier<>(11L)))
				.and(Country::getName, eq("toto"))
				// but we care about this since we can check that criteria on embedded values works
				.and(Country::getTimestamp, Timestamp::getModificationDate, eq(new Date()));
		
		// When
		List<Country> select = testInstance.loadSelection(countryEntityCriteriaSupport.getCriteria());
		Country expectedCountry = new Country(new PersistedIdentifier<>(12L));
		expectedCountry.setName("France");
		expectedCountry.setTimestamp(new Timestamp(new Date(0), new Date(0)));
		assertThat(Iterables.first(select))
				.usingComparator(Comparator.comparing(chain(Country::getId, Identifier::getSurrogate)))
				.isEqualTo(expectedCountry)
				.usingComparator(Comparator.comparing(Country::getName))
				.isEqualTo(expectedCountry)
				.usingComparator(Comparator.comparing(chain(Country::getTimestamp, Timestamp::getCreationDate)))
				.isEqualTo(expectedCountry)
				.usingComparator(Comparator.comparing(chain(Country::getTimestamp, Timestamp::getModificationDate)))
				.isEqualTo(expectedCountry);
		
		// checking that criteria is in the where clause
		verify(connectionMock).prepareStatement(sqlCaptor.capture());
		assertThat(sqlCaptor.getValue())
				.contains("and Country.name = ? and Country.modificationDate = ?)");
	}
	
	@Test
	void loadSelection_oneToOneCase() throws SQLException {
		createConnectionProvider(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
						.add("Country_name", "France")
						.add("Country_id", 12L)
						.add("capital_id", 42L)
						.add("capital_name", "Paris")
		));
		
		EntityConfiguredJoinedTablesPersister<Country, Identifier> persister = (EntityConfiguredJoinedTablesPersister<Country, Identifier>) entityBuilder(Country.class, Identifier.class)
			.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
			.add(Country::getName)
			.addOneToOne(Country::getCapital,
					entityBuilder(City.class, Identifier.class)
					.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(City::getName))
			.build(new PersistenceContext(connectionProviderMock, dialect));
		
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		EntityGraphSelectExecutor<Country, Identifier, Table> testInstance = new EntityGraphSelectExecutor<>(persister.getEntityJoinTree(), connectionProviderMock, columnBinderRegistry);
		
		EntityCriteria<Country> countryEntityCriteriaSupport = persister
				// actually we don't care about criteria since data is hardly tied to the connection (see createConnectionProvider(..))
				.selectWhere(Country::getId, Operators.<Identifier>in(new PersistedIdentifier<>(11L)))
				.and(Country::getName, eq("toto"))
				.and(Country::getCapital, City::getName, eq("Grenoble"));
		
		// When
		List<Country> select = testInstance.loadSelection(((CriteriaProvider) countryEntityCriteriaSupport).getCriteria());
		Country expectedCountry = new Country(new PersistedIdentifier<>(12L));
		expectedCountry.setName("France");
		City capital = new City(new PersistedIdentifier<>(42L));
		capital.setName("Paris");
		expectedCountry.setCapital(capital);
		assertThat(Iterables.first(select))
				.usingComparator(Comparator.comparing(chain(Country::getId, Identifier::getSurrogate)))
				.isEqualTo(expectedCountry)
				.usingComparator(Comparator.comparing(Country::getName))
				.isEqualTo(expectedCountry)
				.usingComparator(Comparator.comparing(link(Country::getCapital, City::getId, Identifier::getSurrogate)))
				.isEqualTo(expectedCountry)
				.usingComparator(Comparator.comparing(link(Country::getCapital, City::getName)))
				.isEqualTo(expectedCountry);
		
		// checking that criteria is in the where clause
		verify(connectionMock).prepareStatement(sqlCaptor.capture());
		assertThat(sqlCaptor.getValue())
				.contains("and Country.name = ? and City.name = ?)");
	}
	
	@Test
	void loadSelection_oneToManyCase() throws SQLException {
		createConnectionProvider(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
						.add("Country_name", "France")
						.add("Country_id", 12L)
						.add("Country_cities_Country_id", 12L)
						.add("Country_cities_City_id", 42L)
						.add("Country_cities_City_name", "Paris"),
				Maps.forHashMap(String.class, Object.class)
						.add("Country_name", "France")
						.add("Country_id", 12L)
						.add("Country_cities_Country_id", 12L)
						.add("Country_cities_City_id", 43L)
						.add("Country_cities_City_name", "Lyon"),
				Maps.forHashMap(String.class, Object.class)
						.add("Country_name", "France")
						.add("Country_id", 12L)
						.add("Country_cities_Country_id", 12L)
						.add("Country_cities_City_id", 44L)
						.add("Country_cities_City_name", "Grenoble")
		));
		
		EntityConfiguredJoinedTablesPersister<Country, Identifier> persister =
				(EntityConfiguredJoinedTablesPersister<Country, Identifier>) entityBuilder(Country.class, Identifier.class)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.addOneToManySet(Country::getCities, entityBuilder(City.class, Identifier.class)
						.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName))
				.build(new PersistenceContext(connectionProviderMock, dialect));
		
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		EntityGraphSelectExecutor<Country, Identifier, Table> testInstance = new EntityGraphSelectExecutor<>(persister.getEntityJoinTree(), connectionProviderMock, columnBinderRegistry);
		
		// actually we don't care about criteria since data is hardly tied to the connection (see createConnectionProvider(..))
		EntityCriteria<Country> countryEntityCriteriaSupport = persister
				.selectWhere(Country::getId, Operators.<Identifier>in(new PersistedIdentifier<>(11L)))
				.and(Country::getName, eq("toto"))
				.andMany(Country::getCities, City::getName, eq("Grenoble"));
		
		
		Country expectedCountry = new Country(new PersistedIdentifier<>(12L));
		expectedCountry.setName("France");
		City paris = new City(new PersistedIdentifier<>(42L));
		paris.setName("Paris");
		City lyon = new City(new PersistedIdentifier<>(43L));
		lyon.setName("Lyon");
		City grenoble = new City(new PersistedIdentifier<>(44L));
		grenoble.setName("Grenoble");
		expectedCountry.setCities(Arrays.asSet(paris, lyon, grenoble));
		
		// When
		// we must wrap the loadSelection(..) call into the select listener because it is the way expected by ManyCascadeConfigurer
		// to initialize some variables (ThreadLocal ones)
		List<Country> select = persister.getPersisterListener().doWithSelectListener(Collections.emptyList(), () ->
				testInstance.loadSelection(((CriteriaProvider) countryEntityCriteriaSupport).getCriteria())
		);
		
		assertThat(Iterables.first(select))
				.usingComparator(Comparator.comparing(chain(Country::getId, Identifier::getSurrogate)))
				.isEqualTo(expectedCountry)
				.usingComparator(Comparator.comparing(Country::getName))
				.isEqualTo(expectedCountry);
		
		// checking that criteria is in the where clause
		verify(connectionMock).prepareStatement(sqlCaptor.capture());
		assertThat(sqlCaptor.getValue())
				.contains("and Country.name = ? and City.name = ?)");
	}
	
	@Test
	void loadGraph() throws SQLException {
		// This test must be done with a real Database because 2 queries are executed which can hardly be mocked
		// Hence this test is more an integration test, but since it runs fast, we don't care
		DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(new HSQLDBInMemoryDataSource());
		
		HSQLDBDialect dialect = new HSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "bigint");
		
		PersistenceContext persistenceContext = new PersistenceContext(connectionProvider, dialect);
		OptimizedUpdatePersister<Country, Identifier> persister = (OptimizedUpdatePersister<Country, Identifier>) entityBuilder(Country.class, Identifier.class)
			.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
			.add(Country::getName)
			.addOneToManySet(Country::getCities, entityBuilder(City.class, Identifier.class)
					.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(City::getName))
					.mappedBy(City::getCountry)
			.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Connection currentConnection = connectionProvider.giveConnection();
		currentConnection.prepareStatement("insert into Country(id, name) values(12, 'France')").execute();
		currentConnection.prepareStatement("insert into City(id, name, countryId) values(42, 'Paris', 12)").execute();
		currentConnection.prepareStatement("insert into City(id, name, countryId) values(43, 'Lyon', 12)").execute();
		currentConnection.prepareStatement("insert into City(id, name, countryId) values(44, 'Grenoble', 12)").execute();
		
		ColumnBinderRegistry columnBinderRegistry = dialect.getColumnBinderRegistry();
		EntityGraphSelectExecutor<Country, Identifier, Table> testInstance = new EntityGraphSelectExecutor<>(persister.getEntityJoinTree(), connectionProvider, columnBinderRegistry);
		
		// Criteria tied to data formerly persisted
		EntityCriteria<Country> countryEntityCriteriaSupport =
				persister.selectWhere(Country::getName, eq("France"))
				.andMany(Country::getCities, City::getName, eq("Grenoble"))
				;
		
		
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
				testInstance.loadGraph(((CriteriaProvider) countryEntityCriteriaSupport).getCriteria())
		);
		
		assertThat(Iterables.first(select))
				.usingComparator(Comparator.comparing(chain(Country::getId, Identifier::getSurrogate)))
				.isEqualTo(expectedCountry)
				.usingComparator(Comparator.comparing(Country::getName))
				.isEqualTo(expectedCountry);
	}
	
	@Test
	void loadGraph_emptyResult() {
		// This test must be done with a real Database because 2 queries are executed which can hardly be mocked
		// Hence this test is more an integration test, but since it runs fast, we don't care
		DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(new HSQLDBInMemoryDataSource());
		
		HSQLDBDialect dialect = new HSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "bigint");
		
		PersistenceContext persistenceContext = new PersistenceContext(connectionProvider, dialect);
		EntityConfiguredJoinedTablesPersister<Country, Identifier> persister = (EntityConfiguredJoinedTablesPersister<Country, Identifier>) entityBuilder(Country.class, Identifier.class)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.addOneToManySet(Country::getCities, entityBuilder(City.class, Identifier.class)
						.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName))
				.mappedBy(City::getCountry)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// this won't retrieve any result since database is empty
		ExecutableEntityQuery<Country> countryEntityCriteriaSupport = persister.selectWhere(Country::getName, eq("France"))
				.andMany(Country::getCities, City::getName, eq("Grenoble"));
		
		// we must wrap the select call into the select listener because it is the way expected by ManyCascadeConfigurer to initialize some variables (ThreadLocal ones)
		List<Country> select = countryEntityCriteriaSupport.execute();
		
		assertThat(select).isEmpty();
	}
	
}