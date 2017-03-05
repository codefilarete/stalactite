package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.gama.lang.function.Sequence;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.TransactionObserverConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderColumnOptions;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilderVersioningTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
	private Persister<Person, PersistedIdentifier<Long>> personPersister;
	private Persister<City, PersistedIdentifier<Long>> cityPersister;
	private PersistenceContext persistenceContext;
	
	@BeforeClass
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders
				.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders
				.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
	}
	
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
	@Before
	public void initTest() {
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
		
		IFluentMappingBuilderColumnOptions<Person, PersistedIdentifier<Long>> personMappingBuilder = FluentMappingBuilder.from(Person.class,
				(Class<PersistedIdentifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName);
		personPersister = personMappingBuilder.build(persistenceContext);
		
		IFluentMappingBuilderColumnOptions<City, PersistedIdentifier<Long>> cityMappingBuilder = FluentMappingBuilder.from(City.class,
				(Class<PersistedIdentifier<Long>>) (Class) PersistedIdentifier.class)
				.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName)
				.add(City::getCountry);
		cityPersister = cityMappingBuilder.build(persistenceContext);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testBuild_connectionProviderIsNotRollbackObserver_throwsException() throws SQLException {
		PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
		FluentMappingBuilder.from(Country.class,
				(Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				// setting a foreign key naming strategy to be tested
				.foreignKeyNamingStrategy(ForeignKeyNamingStrategy.DEFAULT)
				.versionedBy(Country::getVersion, new IntSequence())
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.build(persistenceContext);
	}
	
	@Test
	public void testUpdate_versionIsUpgraded() throws SQLException {
		JdbcConnectionProvider surrogateConnectionProvider = new JdbcConnectionProvider(dataSource);
		ConnectionProvider connectionProvider = new TransactionObserverConnectionProvider(surrogateConnectionProvider);
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class,
				(Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				// setting a foreign key naming strategy to be tested
				.foreignKeyNamingStrategy(ForeignKeyNamingStrategy.DEFAULT)
				.versionedBy(Country::getVersion, new IntSequence())
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// creation of test data
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		countryPersister.insert(dummyCountry);
		
		
		// test case: the version of an updated entity is upgraded
		Country dummyCountryClone1 = countryPersister.select(dummyCountry.getId());
		dummyCountryClone1.setName("Toto");
		countryPersister.update(dummyCountryClone1, dummyCountry, true);
		
		// checking
		assertEquals(1, dummyCountryClone1.getVersion());
		assertEquals(0, dummyCountry.getVersion());
		
		// the reloaded version should be up to date
		Country dummyCountryClone2 = countryPersister.select(dummyCountry.getId());
		assertEquals(1, dummyCountryClone2.getVersion());
		
		// another update should upgraded the entity again
		dummyCountryClone2.setName("Tutu");
		countryPersister.update(dummyCountryClone2, dummyCountryClone1, true);
		assertEquals(2, dummyCountryClone2.getVersion());
		assertEquals(0, dummyCountry.getVersion());
	}
	
	@Test
	public void testUpdate_entityIsOutOfSync_databaseIsNotUpdated() throws SQLException {
		JdbcConnectionProvider surrogateConnectionProvider = new JdbcConnectionProvider(dataSource);
		ConnectionProvider connectionProvider = new TransactionObserverConnectionProvider(surrogateConnectionProvider);
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class,
				(Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				// setting a foreign key naming strategy to be tested
				.foreignKeyNamingStrategy(ForeignKeyNamingStrategy.DEFAULT)
				.versionedBy(Country::getVersion, new IntSequence())
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// creation of a test data
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		countryPersister.insert(dummyCountry);
		
		// test case : we out of sync the entity by loading it from database while another process will update it
		Country dummyCountryClone = countryPersister.select(dummyCountry.getId());
		// another process updates it (dumb update)
		connectionProvider.getCurrentConnection().createStatement().executeUpdate(
				"update Country set version = version + 1 where id = " + dummyCountry.getId().getSurrogate());
		
		// the update must fail because the updated object is out of sync
		dummyCountryClone.setName("Tata");
		// the following should go wrong since version is not up to date on the clone
		assertThatExceptionOfType(StaleObjectExcepion.class).isThrownBy(() -> countryPersister.update(dummyCountryClone, dummyCountry, true));
//		assertEquals(0, countryPersister.update(dummyCountryClone, dummyCountry, true));
		// version is not reverted because rollback wasn't invoked 
		assertEquals(1, dummyCountryClone.getVersion());
		// ... but it is when we rollback
		connectionProvider.getCurrentConnection().rollback();
		assertEquals(0, dummyCountryClone.getVersion());
	}
	
	
	private static class IntSequence implements Sequence<Integer> {
		
		private int pawn = 0;
		
		@Override
		public Integer next() {
			return ++pawn;
		}
	}
}
