package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.gama.lang.function.Serie.IntegerSerie;
import org.gama.lang.function.Serie.NowSerie;
import org.gama.lang.test.Assertions;
import org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.TransactionAwareConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.IFluentEntityMappingBuilder.IFluentMappingBuilderPropertyOptions;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportVersioningTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
	private IEntityPersister<Person, Identifier<Long>> personPersister;
	private IEntityPersister<City, Identifier<Long>> cityPersister;
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders
				.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders
				.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
	}
	
	@BeforeEach
	public void initTest() {
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
		
		IFluentMappingBuilderPropertyOptions<Person, Identifier<Long>> personMappingBuilder = MappingEase.entityBuilder(Person.class,
				Identifier.LONG_TYPE)
				.add(Person::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName);
		personPersister = personMappingBuilder.build(persistenceContext);
		
		IFluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityMappingBuilder = MappingEase.entityBuilder(City.class,
				Identifier.LONG_TYPE)
				.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName)
				.add(City::getCountry);
		cityPersister = cityMappingBuilder.build(persistenceContext);
	}
	
	@Test
	public void testBuild_versionedPropertyIsOfUnsupportedType_throwsException() {
		PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
		assertThrows(UnsupportedOperationException.class, () -> MappingEase.entityBuilder(Country.class,
				Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
				.versionedBy(Country::getName)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.build(persistenceContext));
	}
	
	@Test
	public void testUpdate_versionIsUpgraded_integerVersion() throws SQLException {
		JdbcConnectionProvider surrogateConnectionProvider = new JdbcConnectionProvider(dataSource);
		ConnectionProvider connectionProvider = new TransactionAwareConnectionProvider(surrogateConnectionProvider);
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		// mapping building thantks to fluent API
		IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class,
				Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
				.versionedBy(Country::getVersion, new IntegerSerie())
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
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
		assertEquals(2, dummyCountryClone1.getVersion());
		assertEquals(1, dummyCountry.getVersion());
		
		// the reloaded version should be up to date
		Country dummyCountryClone2 = countryPersister.select(dummyCountry.getId());
		assertEquals(2, dummyCountryClone2.getVersion());
		
		// another update should upgraded the entity again
		dummyCountryClone2.setName("Tutu");
		countryPersister.update(dummyCountryClone2, dummyCountryClone1, true);
		assertEquals(3, dummyCountryClone2.getVersion());
		assertEquals(1, dummyCountry.getVersion());
	}
	
	@Test
	public void testUpdate_versionIsUpgraded_dateVersion() throws SQLException {
		JdbcConnectionProvider surrogateConnectionProvider = new JdbcConnectionProvider(dataSource);
		ConnectionProvider connectionProvider = new TransactionAwareConnectionProvider(surrogateConnectionProvider);
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		// mapping building thantks to fluent API
		List<LocalDateTime> nowHistory = new ArrayList<>();
		IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class,
				Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
				.versionedBy(Country::getModificationDate, new NowSerie() {
					@Override
					public LocalDateTime next(LocalDateTime input) {
						LocalDateTime now = super.next(input);
						nowHistory.add(now);
						return now;
					}
				})
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
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
		assertEquals(nowHistory.get(1), dummyCountryClone1.getModificationDate());
		assertEquals(nowHistory.get(0), dummyCountry.getModificationDate());
		
		// the reloaded version should be up to date
		Country dummyCountryClone2 = countryPersister.select(dummyCountry.getId());
		assertEquals(nowHistory.get(1), dummyCountryClone2.getModificationDate());
		
		// another update should upgraded the entity again
		dummyCountryClone2.setName("Tutu");
		countryPersister.update(dummyCountryClone2, dummyCountryClone1, true);
		assertEquals(nowHistory.get(2), dummyCountryClone2.getModificationDate());
		assertEquals(nowHistory.get(0), dummyCountry.getModificationDate());
	}
	
	@Test
	public void testUpdate_entityIsOutOfSync_databaseIsNotUpdated() throws SQLException {
		JdbcConnectionProvider surrogateConnectionProvider = new JdbcConnectionProvider(dataSource);
		persistenceContext = new PersistenceContext(surrogateConnectionProvider, DIALECT);
		ConnectionProvider connectionProvider = persistenceContext.getConnectionProvider();
		// mapping building thanks to fluent API
		IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class,
				Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
				.versionedBy(Country::getVersion, new IntegerSerie())
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
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
		assertEquals(1, dummyCountry.getVersion());
		
		// test case : we out of sync the entity by loading it from database while another process will update it
		Country dummyCountryClone = countryPersister.select(dummyCountry.getId());
		// another process updates it (dumb update)
		connectionProvider.getCurrentConnection().createStatement().executeUpdate(
				"update Country set version = version + 1 where id = " + dummyCountry.getId().getSurrogate());
		
		// the update must fail because the updated object is out of sync
		dummyCountryClone.setName("Tata");
		// the following should go wrong since version is not up to date on the clone and the original
		Assertions.assertThrows(() -> countryPersister.update(dummyCountryClone, dummyCountry, true),
				Assertions.hasExceptionInCauses(StaleObjectExcepion.class).andProjection(Assertions.hasMessage("1 rows were expected to be hit but only 0 were effectively")));
		Assertions.assertThrows(() -> countryPersister.delete(dummyCountry),
				Assertions.hasExceptionInCauses(StaleObjectExcepion.class).andProjection(Assertions.hasMessage("1 rows were expected to be hit but only 0 were effectively")));
		// version is not reverted because rollback wasn't invoked 
		assertEquals(2, dummyCountryClone.getVersion());
		// ... but it is when we rollback
		connectionProvider.getCurrentConnection().rollback();
		assertEquals(1, dummyCountryClone.getVersion());
		
		// check that version is robust to multiple rollback
		connectionProvider.getCurrentConnection().rollback();
		assertEquals(1, dummyCountryClone.getVersion());
	}
}
