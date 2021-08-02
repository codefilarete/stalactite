package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.gama.lang.exception.Exceptions;
import org.gama.lang.function.Serie.IntegerSerie;
import org.gama.lang.function.Serie.NowSerie;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.TransactionAwareConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportVersioningTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders
				.LONG_PRIMITIVE_BINDER));
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
	}
	
	@BeforeEach
	public void initTest() {
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
	}
	
	@Test
	public void testBuild_versionedPropertyIsOfUnsupportedType_throwsException() {
		PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> MappingEase.entityBuilder(Country.class,
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
		// mapping building thanks to fluent API
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class,
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
		assertThat(dummyCountryClone1.getVersion()).isEqualTo(2);
		assertThat(dummyCountry.getVersion()).isEqualTo(1);
		
		// the reloaded version should be up to date
		Country dummyCountryClone2 = countryPersister.select(dummyCountry.getId());
		assertThat(dummyCountryClone2.getVersion()).isEqualTo(2);
		
		// another update should upgraded the entity again
		dummyCountryClone2.setName("Tutu");
		countryPersister.update(dummyCountryClone2, dummyCountryClone1, true);
		assertThat(dummyCountryClone2.getVersion()).isEqualTo(3);
		assertThat(dummyCountry.getVersion()).isEqualTo(1);
	}
	
	@Test
	public void testUpdate_versionIsUpgraded_dateVersion() throws SQLException {
		JdbcConnectionProvider surrogateConnectionProvider = new JdbcConnectionProvider(dataSource);
		ConnectionProvider connectionProvider = new TransactionAwareConnectionProvider(surrogateConnectionProvider);
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		// mapping building thanks to fluent API
		List<LocalDateTime> nowHistory = new ArrayList<>();
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class,
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
		assertThat(dummyCountryClone1.getModificationDate()).isEqualTo(nowHistory.get(1));
		assertThat(dummyCountry.getModificationDate()).isEqualTo(nowHistory.get(0));
		
		// the reloaded version should be up to date
		Country dummyCountryClone2 = countryPersister.select(dummyCountry.getId());
		assertThat(dummyCountryClone2.getModificationDate()).isEqualTo(nowHistory.get(1));
		
		// another update should upgraded the entity again
		dummyCountryClone2.setName("Tutu");
		countryPersister.update(dummyCountryClone2, dummyCountryClone1, true);
		assertThat(dummyCountryClone2.getModificationDate()).isEqualTo(nowHistory.get(2));
		assertThat(dummyCountry.getModificationDate()).isEqualTo(nowHistory.get(0));
	}
	
	@Test
	public void testUpdate_entityIsOutOfSync_databaseIsNotUpdated() throws SQLException {
		JdbcConnectionProvider surrogateConnectionProvider = new JdbcConnectionProvider(dataSource);
		persistenceContext = new PersistenceContext(surrogateConnectionProvider, DIALECT);
		ConnectionProvider connectionProvider = persistenceContext.getConnectionProvider();
		// mapping building thanks to fluent API
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class,
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
		assertThat(dummyCountry.getVersion()).isEqualTo(1);
		
		// test case : we out of sync the entity by loading it from database while another process will update it
		Country dummyCountryClone = countryPersister.select(dummyCountry.getId());
		// another process updates it (dumb update)
		connectionProvider.getCurrentConnection().createStatement().executeUpdate(
				"update Country set version = version + 1 where id = " + dummyCountry.getId().getSurrogate());
		
		// the update must fail because the updated object is out of sync
		dummyCountryClone.setName("Tata");
		// the following should go wrong since version is not up to date on the clone and the original
		assertThatThrownBy(() -> countryPersister.update(dummyCountryClone, dummyCountry, true))
				.extracting(t -> Exceptions.findExceptionInCauses(t, StaleObjectExcepion.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("1 rows were expected to be hit but only 0 were effectively");
		assertThatThrownBy(() -> countryPersister.delete(dummyCountry))
				.extracting(t -> Exceptions.findExceptionInCauses(t, StaleObjectExcepion.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("1 rows were expected to be hit but only 0 were effectively");
		// version is not reverted because rollback wasn't invoked 
		assertThat(dummyCountryClone.getVersion()).isEqualTo(2);
		// ... but it is when we rollback
		connectionProvider.getCurrentConnection().rollback();
		assertThat(dummyCountryClone.getVersion()).isEqualTo(1);
		
		// check that version is robust to multiple rollback
		connectionProvider.getCurrentConnection().rollback();
		assertThat(dummyCountryClone.getVersion()).isEqualTo(1);
	}
}
