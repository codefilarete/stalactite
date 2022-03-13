package org.codefilarete.stalactite.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.OptimizedUpdatePersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister.CriteriaProvider;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.query.model.Operators.eq;
import static org.codefilarete.tool.function.Functions.chain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
	void loadGraph() throws SQLException {
		// This test must be done with a real Database because 2 queries are executed which can hardly be mocked
		// Hence this test is more an integration test, but since it runs fast, we don't care
		CurrentThreadConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(new HSQLDBInMemoryDataSource());
		
		HSQLDBDialect dialect = new HSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "bigint");
		
		PersistenceContext persistenceContext = new PersistenceContext(connectionProvider, dialect);
		OptimizedUpdatePersister<Country, Identifier> persister = (OptimizedUpdatePersister<Country, Identifier>) entityBuilder(Country.class, Identifier.class)
			.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
			.map(Country::getName)
			.mapOneToManySet(Country::getCities, entityBuilder(City.class, Identifier.class)
					.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(City::getName))
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
		CurrentThreadConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(new HSQLDBInMemoryDataSource());
		
		HSQLDBDialect dialect = new HSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "bigint");
		
		PersistenceContext persistenceContext = new PersistenceContext(connectionProvider, dialect);
		EntityConfiguredJoinedTablesPersister<Country, Identifier> persister = (EntityConfiguredJoinedTablesPersister<Country, Identifier>) entityBuilder(Country.class, Identifier.class)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.mapOneToManySet(Country::getCities, entityBuilder(City.class, Identifier.class)
						.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(City::getName))
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