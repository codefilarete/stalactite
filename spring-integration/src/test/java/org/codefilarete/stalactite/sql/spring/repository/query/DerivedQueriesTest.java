package org.codefilarete.stalactite.sql.spring.repository.query;


import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.Set;

import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.TransactionalConnectionProvider;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Language;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.State;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
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
import org.codefilarete.tool.Dates;
import org.codefilarete.tool.collection.Arrays;
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
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.tool.collection.Arrays.asHashSet;

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
	void embeddedCriteria() {
		Country country1 = new Country(42);
		country1.setName("Toto");
		country1.setTimestamp(new Timestamp(
				LocalDateTime.of(2010, Month.JANUARY, 22, 11, 10, 23),
				LocalDateTime.of(2024, Month.MAY, 10, 10, 30, 45)));
		
		Country country2 = new Country(43);
		country2.setName("Toto");
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Country loadedCountry = derivedQueriesRepository.findByTimestampCreationDateLessThan(Dates.nowAsDate());
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void oneToManyCriteria() {
		Country country1 = new Country(42);
		country1.setName("Toto");
		country1.addState(new State(new PersistableIdentifier<>(100L)));
		country1.addState(new State(new PersistableIdentifier<>(200L)));
		Person president1 = new Person(666);
		president1.setName("me");
		president1.initNicknames();
		president1.addNickname("John Do");
		president1.addNickname("Jane Do");
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
		
		Country loadedCountry = derivedQueriesRepository.findByStatesIdIn(Arrays.asList(new PersistableIdentifier<>(100L)));
		assertThat(loadedCountry).isEqualTo(country1);
		
		loadedCountry = derivedQueriesRepository.findByPresidentNicknamesIn(Arrays.asList("John Do"));
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void manyToManyCriteria() {
		Country country1 = new Country(42);
		country1.setName("Toto");
		Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
		Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
		Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
		country1.setLanguages(asHashSet(frFr, enEn));
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Country country2 = new Country(43);
		country2.setName("Toto");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		country2.setLanguages(asHashSet(frFr, esEs));
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Country> loadedCountries = derivedQueriesRepository.findByLanguagesCodeIs("fr_fr");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
		
		loadedCountries = derivedQueriesRepository.findByLanguagesCodeIs("en_en");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
	}
	
	@Test
	void countByCriteria() {
		Country country1 = new Country(42);
		country1.setName("Toto");
		Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
		Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
		Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
		country1.setLanguages(asHashSet(frFr, enEn));
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Country country2 = new Country(43);
		country2.setName("Toto");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		country2.setLanguages(asHashSet(frFr, esEs));
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		long loadedCountries = derivedQueriesRepository.countByLanguagesCodeIs("fr_fr");
		assertThat(loadedCountries).isEqualTo(2);
		
		loadedCountries = derivedQueriesRepository.countByLanguagesCodeIs("en_en");
		assertThat(loadedCountries).isEqualTo(1);
	}
	
	@Test
	void countDistinctByCriteria() {
		Country country1 = new Country(42);
		country1.setName("Toto");
		Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
		// we add a second language to make the query returns several time country1 id
		Language frFr2 = new Language(new PersistableIdentifier<>(78L), "fr_fr");
		Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
		Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
		country1.setLanguages(asHashSet(frFr, frFr2, enEn));
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Country country2 = new Country(43);
		country2.setName("Toto");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		country2.setLanguages(asHashSet(frFr, esEs));
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		// this is only to ensure that database is correct, else next assertions are useless
		long loadedCountries = derivedQueriesRepository.countByLanguagesCodeIs("fr_fr");
		assertThat(loadedCountries).isEqualTo(3);
		
		loadedCountries = derivedQueriesRepository.countDistinctByLanguagesCodeIs("fr_fr");
		assertThat(loadedCountries).isEqualTo(2);
		
		loadedCountries = derivedQueriesRepository.countDistinctByLanguagesCodeIs("en_en");
		assertThat(loadedCountries).isEqualTo(1);
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
	class delete {
		
		@Test
		void manyToManyCriteria() {
			Country country1 = new Country(42);
			country1.setName("Toto");
			Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
			Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
			Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
			country1.setLanguages(asHashSet(frFr, enEn));
			Person president1 = new Person(666);
			president1.setName("me");
			country1.setPresident(president1);
			
			Country country2 = new Country(43);
			country2.setName("Toto");
			Person president2 = new Person(237);
			president2.setName("you");
			country2.setPresident(president2);
			country2.setLanguages(asHashSet(frFr, esEs));
			
			Vehicle vehicle = new Vehicle(1438L);
			vehicle.setColor(new Color(123));
			president1.setVehicle(vehicle);
			
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			long loadedCountries = derivedQueriesRepository.deleteByLanguagesCodeIs("fr_fr");
			assertThat(loadedCountries).isEqualTo(2);
		}
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
		void equalBoolean() {
			Country country1 = new Country(42);
			country1.setName("Toto");
			Country country2 = new Country(43);
			country2.setName("Toto");
			country2.setEuMember(true);
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));

			Country loadedCountry = derivedQueriesRepository.findByEuMemberIsTrue();
			assertThat(loadedCountry).isEqualTo(country2);

			loadedCountry = derivedQueriesRepository.findByEuMemberIsFalse();
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
		void contains() {
			Country country1 = new Country(42);
			country1.setDescription("a description with a keyword");
			Country country2 = new Country(43);
			country2.setDescription("a keyword contained in the description");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByDescriptionContains("contained");
			assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
		}
		
		@Test
		void notContains() {
			Country country1 = new Country(42);
			country1.setDescription("a description with a keyword");
			Country country2 = new Country(43);
			country2.setDescription("a keyword contained in the description");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByDescriptionNotContains("contained");
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
		void before() {
			Country country1 = new Country(42);
			derivedQueriesRepository.save(country1);
			Country country2 = new Country(43);
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByIdBefore(new PersistedIdentifier<>(43L));
			assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
		}
		
		@Test
		void after() {
			Country country1 = new Country(42);
			derivedQueriesRepository.save(country1);
			Country country2 = new Country(43);
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByIdAfter(new PersistedIdentifier<>(42L));
			assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
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
			dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
			dialect.getSqlTypeRegistry().put(Identifier.class, "int");
			
			dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
			dialect.getSqlTypeRegistry().put(Color.class, "int");
			
			return new PersistenceContext(dataSource, dialect);
		}
		
		@Bean
		public EntityPersister<Country, Identifier<Long>> countryPersister(PersistenceContext persistenceContext) {
			return entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, IdentifierPolicy.<Country, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
					.map(Country::getName)
					.map(Country::getDescription)
					.map(Country::isEuMember)
					.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapOneToOne(Country::getPresident, entityBuilder(Person.class, LONG_TYPE)
							.mapKey(Person::getId, IdentifierPolicy.<Person, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.map(Person::getName)
							.mapCollection(Person::getNicknames, String.class)
							.mapOneToOne(Person::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
									.mapKey(Vehicle::getId, IdentifierPolicy.<Vehicle, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
									.map(Vehicle::getColor)))
					.mapOneToMany(Country::getStates, entityBuilder(State.class, LONG_TYPE)
							.mapKey(State::getId, IdentifierPolicy.<State, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.map(State::getName))
					.mapManyToMany(Country::getLanguages, entityBuilder(Language.class, LONG_TYPE)
							.mapKey(Language::getId, IdentifierPolicy.<Language, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.usingConstructor(Language::new, "id", "code")
							.map(Language::getCode).setByConstructor()
					)
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