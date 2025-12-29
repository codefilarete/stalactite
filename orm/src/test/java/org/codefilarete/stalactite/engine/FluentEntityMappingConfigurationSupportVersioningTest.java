package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.data.TemporalUnitWithinOffset;
import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.dsl.MappingEase;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.TransactionAwareConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.function.Serie.IntegerSerie;
import org.codefilarete.tool.function.Serie.NowSerie;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportVersioningTest {
	
	private static final Dialect DIALECT = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
	}
	
	@BeforeEach
	public void initTest() {
		persistenceContext = new PersistenceContext(dataSource, DIALECT);
	}
	
	@Test
	void build_versionedPropertyIsOfUnsupportedType_throwsException() {
		PersistenceContext persistenceContext = new PersistenceContext(dataSource, DIALECT);
		assertThatCode(() -> MappingEase.entityBuilder(Country.class, LONG_TYPE)
				.versionedBy(Country::getName)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription)
				.build(persistenceContext))
				.isInstanceOf(UnsupportedOperationException.class);
	}
	
	@Test
	void schemaIsCorrect() {
		ConnectionProvider connectionProvider = new TransactionAwareConnectionProvider(new CurrentThreadConnectionProvider(dataSource));
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		// mapping building thanks to fluent API
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, LONG_TYPE)
				.versionedBy(Country::getVersion, new IntegerSerie())
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription)
				.build(persistenceContext);
		
		Map<String, Table<?>> tablePerName = Iterables.map(DDLDeployer.collectTables(persistenceContext), Table::getName);
		assertThat(tablePerName.get("Country").mapColumnsOnName().get("version").isNullable()).isFalse();
	}
	
	@Test
	void update_versionIsUpgraded_integerVersion() {
		ConnectionProvider connectionProvider = new TransactionAwareConnectionProvider(new CurrentThreadConnectionProvider(dataSource));
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		// mapping building thanks to fluent API
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, LONG_TYPE)
				.versionedBy(Country::getVersion, new IntegerSerie())
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription)
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
		
		// the reloaded version should be up-to-date
		Country dummyCountryClone2 = countryPersister.select(dummyCountry.getId());
		assertThat(dummyCountryClone2.getVersion()).isEqualTo(2);
		
		// another update should upgrade the entity again
		dummyCountryClone2.setName("Tutu");
		countryPersister.update(dummyCountryClone2, dummyCountryClone1, true);
		assertThat(dummyCountryClone2.getVersion()).isEqualTo(3);
		assertThat(dummyCountry.getVersion()).isEqualTo(1);
	}
	
	@Test
	void update_versionIsUpgraded_dateVersion() {
		ConnectionProvider connectionProvider = new TransactionAwareConnectionProvider(new CurrentThreadConnectionProvider(dataSource));
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		List<LocalDateTime> nowHistory = new ArrayList<>();
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, LONG_TYPE)
				.versionedBy(Country::getModificationDate, new NowSerie() {
					@Override
					public LocalDateTime next(LocalDateTime input) {
						LocalDateTime now = super.next(input);
						nowHistory.add(now);
						return now;
					}
				})
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription)
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

		// Since Java 9 LocalDateTime.now() changed its precision : when available by OS it takes nanosecond precision,
		// (https://bugs.openjdk.java.net/browse/JDK-8068730)
		// this implies a comparison failure because many databases don't store nanosecond by default (with SQL TIMESTAMP type, which is the default
		// in DefaultTypeMapping), therefore the LocalDateTime read by binder doesn't contain nanoseconds, so when it is compared to original value
		// (which contains nanos) it fails. To overcome this problem we consider not using LocalDateTime.now(), and taking the loss of precision
		// in the test
		assertThat(dummyCountryClone2.getModificationDate()).isCloseTo(nowHistory.get(1), new TemporalUnitWithinOffset(1000, ChronoUnit.NANOS));
		
		// another update should upgraded the entity again
		dummyCountryClone2.setName("Tutu");
		countryPersister.update(dummyCountryClone2, dummyCountryClone1, true);
		assertThat(dummyCountryClone2.getModificationDate()).isEqualTo(nowHistory.get(2));
		assertThat(dummyCountry.getModificationDate()).isEqualTo(nowHistory.get(0));
	}
	
	@Test
	void update_entityIsOutOfSync_databaseIsNotUpdated() throws SQLException {
		persistenceContext = new PersistenceContext(dataSource, DIALECT);
		ConnectionProvider connectionProvider = persistenceContext.getConnectionProvider();
		
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, LONG_TYPE)
				.versionedBy(Country::getVersion, new IntegerSerie())
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription)
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
		connectionProvider.giveConnection().createStatement().executeUpdate(
				"update Country set version = version + 1 where id = " + dummyCountry.getId().getDelegate());
		
		// the update must fail because the updated object is out of sync
		dummyCountryClone.setName("Tata");
		// the following should go wrong since version is not up to date on the clone and the original
		assertThatThrownBy(() -> countryPersister.update(dummyCountryClone, dummyCountry, true))
				.extracting(t -> Exceptions.findExceptionInCauses(t, StaleStateObjectException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("1 rows were expected to be hit but 0 were effectively");
		assertThatThrownBy(() -> countryPersister.delete(dummyCountry))
				.extracting(t -> Exceptions.findExceptionInCauses(t, StaleStateObjectException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("1 rows were expected to be hit but 0 were effectively");
		// version is not reverted because rollback wasn't invoked 
		assertThat(dummyCountryClone.getVersion()).isEqualTo(2);
		// ... but it is when we rollback
		connectionProvider.giveConnection().rollback();
		assertThat(dummyCountryClone.getVersion()).isEqualTo(1);
		
		// check that version is robust to multiple rollback
		connectionProvider.giveConnection().rollback();
		assertThat(dummyCountryClone.getVersion()).isEqualTo(1);
	}
	
	@Test
	void versionByFieldName_update_happyPath() {
		ConnectionProvider connectionProvider = new TransactionAwareConnectionProvider(new CurrentThreadConnectionProvider(dataSource));
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		// mapping building thanks to fluent API
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, LONG_TYPE)
				.versionedBy("versionWithoutAccessor")
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription)
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
		
		
		Accessor<Country, Integer> versionWithoutAccessor = Accessors.accessor(Country.class, "versionWithoutAccessor");
		
		// checking
		assertThat(versionWithoutAccessor.get(dummyCountryClone1)).isEqualTo(2);
		assertThat(versionWithoutAccessor.get(dummyCountry)).isEqualTo(1);
		
		// the reloaded version should be up-to-date
		Country dummyCountryClone2 = countryPersister.select(dummyCountry.getId());
		assertThat(versionWithoutAccessor.get(dummyCountryClone2)).isEqualTo(2);
		
		// another update should upgrade the entity again
		dummyCountryClone2.setName("Tutu");
		countryPersister.update(dummyCountryClone2, dummyCountryClone1, true);
		assertThat(versionWithoutAccessor.get(dummyCountryClone2)).isEqualTo(3);
		assertThat(versionWithoutAccessor.get(dummyCountry)).isEqualTo(1);
	}
}
