package org.codefilarete.stalactite.sql.spring.repository.query;


import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Set;

import org.codefilarete.stalactite.engine.ColumnOptions;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.TransactionalConnectionProvider;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.sql.spring.transaction.StalactitePlatformTransactionManager;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;

/**
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		DerivedQueriesTest.StalactiteRepositoryContextConfiguration.class
})
@Transactional
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.sql.spring.repository.query")
class DerivedQueriesTest {
	
	@Autowired
	private DerivedQueriesRepository derivedQueriesRepository;
	
	
	@Test
	void twoCriteria() {
		Country country1 = new Country(42);
		country1.setName("Toto");
		Country country2 = new Country(43);
		country2.setName("Toto");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Country loadedCountry = derivedQueriesRepository.findByIdAndName(new PersistedIdentifier<>(42L), "Toto");
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void oneToOneCriteria() {
		Country country1 = new Country(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Country country2 = new Country(43);
		country2.setName("Toto");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Country loadedCountry = derivedQueriesRepository.findByPresidentId(new PersistedIdentifier<>(666L));
		assertThat(loadedCountry).isEqualTo(country1);
		
		loadedCountry = derivedQueriesRepository.findByPresidentName("me");
		assertThat(loadedCountry).isEqualTo(country1);
		
		loadedCountry = derivedQueriesRepository.findByPresidentVehicleColor(new Color(123));
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void oneResultExpected_severalResults_throwsException() {
		Country country1 = new Country(42);
		country1.setName("Toto");
		Country country2 = new Country(43);
		country2.setName("Toto");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		assertThatCode(() -> derivedQueriesRepository.findByName("Toto"))
				.isInstanceOf(Accumulators.NonUniqueObjectException.class);
	}
	
	@Nested
	class Criteria {
		
		@Test
		void equal() {
			Country country1 = new Country(42);
			country1.setName("Toto");
			derivedQueriesRepository.save(country1);
			
			Country loadedCountry = derivedQueriesRepository.findByName("Toto");
			assertThat(loadedCountry).isEqualTo(country1);
		}
		
		@Test
		void notEqual() {
			Country country1 = new Country(42);
			country1.setName("Toto");
			derivedQueriesRepository.save(country1);
			
			Country loadedCountry = derivedQueriesRepository.findByNameNot("Titi");
			assertThat(loadedCountry).isEqualTo(country1);
		}
	
		@Test
		void in() {
			Country country1 = new Country(42);
			country1.setName("Toto");
			Country country2 = new Country(43);
			country2.setName("Toto");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByIdIn(Arrays.asList(new PersistedIdentifier<>(42L), new PersistedIdentifier<>(43L), new PersistedIdentifier<>(44L)));
			assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
		}
	
		@Test
		void notIn() {
			Country country1 = new Country(42);
			country1.setName("Toto");
			Country country2 = new Country(43);
			country2.setName("Toto");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByIdNotIn(Arrays.asList(new PersistedIdentifier<>(42L)));
			assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
		}
		
		@Test
		void like() {
			Country country1 = new Country(42);
			country1.setDescription("a description with a keyword");
			Country country2 = new Country(43);
			country2.setDescription("a keyword contained in the description");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByDescriptionLike("keyword");
			assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
		}
	
		@Test
		void notLike() {
			Country country1 = new Country(42);
			country1.setDescription("a description with a keyword");
			Country country2 = new Country(43);
			country2.setDescription("a keyword contained in the description");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByDescriptionNotLike("contained");
			assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
		}
		
		@Test
		void startsWith() {
			Country country1 = new Country(42);
			country1.setDescription("a description with a keyword");
			Country country2 = new Country(43);
			country2.setDescription("a keyword contained in the description");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByDescriptionStartsWith("a keyword");
			assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
		}
	
		@Test
		void endsWith() {
			Country country1 = new Country(42);
			country1.setDescription("a description with a keyword");
			Country country2 = new Country(43);
			country2.setDescription("a keyword contained in the description");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByDescriptionEndsWith("a keyword");
			assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
		}
	
		@Test
		void isNull() {
			Country country1 = new Country(42);
			country1.setName("Toto");
			derivedQueriesRepository.save(country1);
			Country country2 = new Country(43);
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByNameIsNull();
			assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
		}
		
		@Test
		void isNotNull() {
			Country country1 = new Country(42);
			country1.setName("Toto");
			derivedQueriesRepository.save(country1);
			Country country2 = new Country(43);
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByNameIsNotNull();
			assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
		}
		
		@Test
		void lower() {
			Country country1 = new Country(42);
			derivedQueriesRepository.save(country1);
			Country country2 = new Country(43);
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByIdLessThan(new PersistedIdentifier<>(43L));
			assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
		}
		
		@Test
		void lowerEquals() {
			Country country1 = new Country(42);
			derivedQueriesRepository.save(country1);
			Country country2 = new Country(43);
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByIdLessThanEqual(new PersistedIdentifier<>(43L));
			assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
		}
		
		@Test
		void greater() {
			Country country1 = new Country(42);
			derivedQueriesRepository.save(country1);
			Country country2 = new Country(43);
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByIdGreaterThan(new PersistedIdentifier<>(42L));
			assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
		}
		
		@Test
		void greaterEquals() {
			Country country1 = new Country(42);
			derivedQueriesRepository.save(country1);
			Country country2 = new Country(43);
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByIdGreaterThanEqual(new PersistedIdentifier<>(42L));
			assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
		}
		
		
		@Test
		void between() {
			Country country1 = new Country(42);
			derivedQueriesRepository.save(country1);
			Country country2 = new Country(43);
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByIdBetween(new PersistedIdentifier<>(40L), new PersistedIdentifier<>(50L));
			assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
		}
	}
	
	public static class StalactiteRepositoryContextConfiguration {
		
		@Bean
		public DataSource dataSource() {
			return new HSQLDBInMemoryDataSource();
		}
		
		@Bean
		public StalactitePlatformTransactionManager transactionManager(DataSource dataSource) {
			return new StalactitePlatformTransactionManager(dataSource);
		}
		
		@Bean
		public PersistenceContext persistenceContext(StalactitePlatformTransactionManager dataSource) {
			HSQLDBDialect dialect = new HSQLDBDialect();
			dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
			dialect.getSqlTypeRegistry().put(Identifier.class, "int");
			
			dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
			dialect.getSqlTypeRegistry().put(Color.class, "int");
			
			return new PersistenceContext(dataSource, dialect);
		}
		
		@Bean
		public EntityPersister<Country, Identifier<Long>> countryPersister(PersistenceContext persistenceContext) {
			return MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ColumnOptions.IdentifierPolicy.<Country, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToOne(Country::getPresident, MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
							.mapKey(Person::getId, ColumnOptions.IdentifierPolicy.<Person, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.map(Person::getName)
							.mapOneToOne(Person::getVehicle, MappingEase.entityBuilder(Vehicle.class, Identifier.LONG_TYPE)
									.mapKey(Vehicle::getId, ColumnOptions.IdentifierPolicy.<Vehicle, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
									.map(Vehicle::getColor)))
					.build(persistenceContext);
		}
		
		@EventListener
		public void onApplicationEvent(ContextRefreshedEvent event) {
			PersistenceContext persistenceContext = event.getApplicationContext().getBean(PersistenceContext.class);
			DataSource dataSource = event.getApplicationContext().getBean(DataSource.class);
			Dialect dialect = persistenceContext.getDialect();
			DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlTableGenerator(), new TransactionalConnectionProvider(dataSource));
			ddlDeployer.getDdlGenerator().addTables(DDLDeployer.collectTables(persistenceContext));
			ddlDeployer.deployDDL();
		}
	}
}