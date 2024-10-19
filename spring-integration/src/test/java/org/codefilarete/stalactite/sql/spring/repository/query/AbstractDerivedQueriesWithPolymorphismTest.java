package org.codefilarete.stalactite.sql.spring.repository.query;


import java.time.LocalDateTime;
import java.time.Month;
import java.util.Collection;
import java.util.Set;

import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Language;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.State;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.sql.spring.repository.query.DerivedQueriesRepository.NamesOnly;
import org.codefilarete.stalactite.sql.spring.repository.query.DerivedQueriesRepository.NamesOnly.SimplePerson;
import org.codefilarete.tool.Dates;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.codefilarete.tool.collection.Arrays.asHashSet;
import static org.codefilarete.tool.function.Functions.chain;

/**
 * @author Guillaume Mary
 */
@Transactional
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.sql.spring.repository.query")
abstract class AbstractDerivedQueriesWithPolymorphismTest {
	
	@Autowired
	private DerivedQueriesRepository derivedQueriesRepository;
	
	@Test
	void projection() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		DerivedQueriesRepository.NamesOnly loadedCountry = derivedQueriesRepository.getByName("Toto");
		assertThat(loadedCountry.getName()).isEqualTo(country1.getName());
		assertThat(loadedCountry.getPresidentName()).isEqualTo(country1.getPresident().getName());
		assertThat(loadedCountry.getPresident().getName()).isEqualTo(country1.getPresident().getName());
	}
	
	@Test
	void projection_byExtraArgument() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("John Do");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Person president2 = new Person(777);
		president2.setName("Jane Do");
		country2.setPresident(president2);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Collection<NamesOnly> loadedCountries = derivedQueriesRepository.getByName("Toto", NamesOnly.class);
		assertThat(loadedCountries).extracting(NamesOnly::getName)
				.containsExactlyInAnyOrder(country1.getName(), country2.getName());
		assertThat(loadedCountries).extracting(NamesOnly::getPresidentName)
				.containsExactlyInAnyOrder(country1.getPresident().getName(), country2.getPresident().getName());
		assertThat(loadedCountries).extracting(chain(NamesOnly::getPresident, SimplePerson::getName))
				.containsExactlyInAnyOrder(country1.getPresident().getName(), country2.getPresident().getName());
	}
	
	@Test
	void exists() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		boolean loadedCountry = derivedQueriesRepository.existsByName("Toto");
		assertThat(loadedCountry).isTrue();
		loadedCountry = derivedQueriesRepository.existsByName("Tutu");
		assertThat(loadedCountry).isFalse();
	}
	
	@Test
	void twoCriteria() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Republic loadedCountry = derivedQueriesRepository.findByIdAndName(new PersistedIdentifier<>(42L), "Toto");
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void oneToOneCriteria() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Republic loadedCountry = derivedQueriesRepository.findByPresidentId(new PersistedIdentifier<>(666L));
		assertThat(loadedCountry).isEqualTo(country1);
		
		loadedCountry = derivedQueriesRepository.findByPresidentName("me");
		assertThat(loadedCountry).isEqualTo(country1);
		
		loadedCountry = derivedQueriesRepository.findByPresidentVehicleColor(new Color(123));
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void embeddedCriteria() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		country1.setTimestamp(new Timestamp(
				LocalDateTime.of(2010, Month.JANUARY, 22, 11, 10, 23),
				LocalDateTime.of(2024, Month.MAY, 10, 10, 30, 45)));
		
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Republic loadedCountry = derivedQueriesRepository.findByTimestampCreationDateLessThan(Dates.nowAsDate());
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void oneToManyCriteria() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		country1.addState(new State(new PersistableIdentifier<>(100L)));
		country1.addState(new State(new PersistableIdentifier<>(200L)));
		Person president1 = new Person(666);
		president1.setName("me");
		president1.initNicknames();
		president1.addNickname("John Do");
		president1.addNickname("Jane Do");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Republic loadedCountry = derivedQueriesRepository.findByStatesIdIn(Arrays.asList(new PersistableIdentifier<>(100L)));
		assertThat(loadedCountry).isEqualTo(country1);
		
		loadedCountry = derivedQueriesRepository.findByPresidentNicknamesIn(Arrays.asList("John Do"));
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void manyToManyCriteria() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
		Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
		Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
		country1.setLanguages(asHashSet(frFr, enEn));
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		country2.setLanguages(asHashSet(frFr, esEs));
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByLanguagesCodeIs("fr_fr");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
		
		loadedCountries = derivedQueriesRepository.findByLanguagesCodeIs("en_en");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
	}
	
	@Test
	void countByCriteria() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
		Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
		Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
		country1.setLanguages(asHashSet(frFr, enEn));
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
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
		Republic country1 = new Republic(42);
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
		
		Republic country2 = new Republic(43);
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
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		assertThatCode(() -> derivedQueriesRepository.findByName("Toto"))
				.isInstanceOf(Accumulators.NonUniqueObjectException.class);
	}
	
	@Test
	void delete_manyToManyCriteria() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
		Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
		Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
		country1.setLanguages(asHashSet(frFr, enEn));
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
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
	
	// simple Criteria tests from here. Can't use @Nested with Junit and inheritance :'(
	// (test fails by not getting enclosing class context which is Spring info here) 
		
	@Test
	void equal() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		derivedQueriesRepository.save(country1);
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Republic loadedCountry = derivedQueriesRepository.findByName("Toto");
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void notEqual() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		derivedQueriesRepository.save(country1);
		
		Republic loadedCountry = derivedQueriesRepository.findByNameNot("Titi");
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void equalBoolean() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		country2.setEuMember(true);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));

		Republic loadedCountry = derivedQueriesRepository.findByEuMemberIsTrue();
		assertThat(loadedCountry).isEqualTo(country2);

		loadedCountry = derivedQueriesRepository.findByEuMemberIsFalse();
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void in() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries;
		loadedCountries = derivedQueriesRepository.findByIdIn(Arrays.asList(new PersistedIdentifier<>(42L), new PersistedIdentifier<>(43L), new PersistedIdentifier<>(44L)));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
		
		loadedCountries = derivedQueriesRepository.findByIdIn(new PersistedIdentifier<>(42L), new PersistedIdentifier<>(43L), new PersistedIdentifier<>(44L));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
	}
	
	@Test
	void in_string() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3));
		
		Set<Republic> loadedCountries;
		loadedCountries = derivedQueriesRepository.findByNameIn("Titi");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
		
		loadedCountries = derivedQueriesRepository.findByNameIgnoreCaseIn("tiTI");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
		
		loadedCountries = derivedQueriesRepository.findByNameIgnoreCaseIn("tiTI", "TOto");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2, country1);
	}
	
	@Test
	void notIn() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByIdNotIn(Arrays.asList(new PersistedIdentifier<>(42L)));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
	}
	
	@Test
	void notIn_string() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByNameIgnoreCaseNotIn("TATA");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2, country1);
	}
	
	@Test
	void like() {
		Republic country1 = new Republic(42);
		country1.setDescription("a description with a keyword");
		Republic country2 = new Republic(43);
		country2.setDescription("a keyword contained in the description");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByDescriptionLike("keyword");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
	}
	
	@Test
	void like_ignoreCase() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		Republic country3 = new Republic(44);
		country3.setName("Tutu");
		Republic country4 = new Republic(45);
		country4.setName("Tonton");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByNameLikeIgnoreCase("O");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country4);
		
	}
	
	@Test
	void notLike() {
		Republic country1 = new Republic(42);
		country1.setDescription("a description with a keyword");
		Republic country2 = new Republic(43);
		country2.setDescription("a keyword contained in the description");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByDescriptionNotLike("contained");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
	}
	
	@Test
	void notLike_ignoreCase() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		Republic country3 = new Republic(44);
		country3.setName("Tutu");
		Republic country4 = new Republic(45);
		country4.setName("Tonton");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByNameNotLikeIgnoreCase("O");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2, country3);
	}
	
	@Test
	void startsWith() {
		Republic country1 = new Republic(42);
		country1.setDescription("a description with a keyword");
		Republic country2 = new Republic(43);
		country2.setDescription("a keyword contained in the description");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByDescriptionStartsWith("a keyword");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
	}
	
	@Test
	void endsWith() {
		Republic country1 = new Republic(42);
		country1.setDescription("a description with a keyword");
		Republic country2 = new Republic(43);
		country2.setDescription("a keyword contained in the description");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByDescriptionEndsWith("a keyword");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
	}
	
	@Test
	void contains() {
		Republic country1 = new Republic(42);
		country1.setDescription("a description with a keyword");
		Republic country2 = new Republic(43);
		country2.setDescription("a keyword contained in the description");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByDescriptionContains("contained");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
	}
	
	@Test
	void notContains() {
		Republic country1 = new Republic(42);
		country1.setDescription("a description with a keyword");
		Republic country2 = new Republic(43);
		country2.setDescription("a keyword contained in the description");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByDescriptionNotContains("contained");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
	}
	
	@Test
	void isNull() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		derivedQueriesRepository.save(country1);
		Republic country2 = new Republic(43);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByNameIsNull();
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
	}
	
	@Test
	void isNotNull() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		derivedQueriesRepository.save(country1);
		Republic country2 = new Republic(43);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByNameIsNotNull();
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
	}
	
	@Test
	void lesser() {
		Republic country1 = new Republic(42);
		derivedQueriesRepository.save(country1);
		Republic country2 = new Republic(43);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByIdLessThan(new PersistedIdentifier<>(43L));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
	}
	
	@Test
	void lesserEquals() {
		Republic country1 = new Republic(42);
		derivedQueriesRepository.save(country1);
		Republic country2 = new Republic(43);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByIdLessThanEqual(new PersistedIdentifier<>(43L));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
	}
	
	@Test
	void greater() {
		Republic country1 = new Republic(42);
		derivedQueriesRepository.save(country1);
		Republic country2 = new Republic(43);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByIdGreaterThan(new PersistedIdentifier<>(42L));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
	}
	
	@Test
	void greaterEquals() {
		Republic country1 = new Republic(42);
		derivedQueriesRepository.save(country1);
		Republic country2 = new Republic(43);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByIdGreaterThanEqual(new PersistedIdentifier<>(42L));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
	}
	
	@Test
	void before() {
		Republic country1 = new Republic(42);
		derivedQueriesRepository.save(country1);
		Republic country2 = new Republic(43);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByIdBefore(new PersistedIdentifier<>(43L));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1);
	}
	
	@Test
	void after() {
		Republic country1 = new Republic(42);
		derivedQueriesRepository.save(country1);
		Republic country2 = new Republic(43);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByIdAfter(new PersistedIdentifier<>(42L));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
	}
	
	@Test
	void between() {
		Republic country1 = new Republic(42);
		derivedQueriesRepository.save(country1);
		Republic country2 = new Republic(43);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByIdBetween(new PersistedIdentifier<>(40L), new PersistedIdentifier<>(50L));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
	}
	
	@Test
	void equals_ignoreCase() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Republic loadedCountry;

		loadedCountry = derivedQueriesRepository.findByNameIgnoreCase("TOTO");
		assertThat(loadedCountry).isEqualTo(country1);

		loadedCountry = derivedQueriesRepository.findByNameIgnoreCase("toto");
		assertThat(loadedCountry).isEqualTo(country1);

		loadedCountry = derivedQueriesRepository.findByNameIgnoringCase("TOTO");
		assertThat(loadedCountry).isEqualTo(country1);

		loadedCountry = derivedQueriesRepository.findByNameIgnoringCase("toto");
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void ignoreCase_dynamic() {
		Republic country1 = new Republic(42);
		country1.setName("Toto_b");
		Republic country2 = new Republic(43);
		country2.setName("TOtO_c");
		Republic country3 = new Republic(44);
		country3.setName("toTO_a");
		Republic country4 = new Republic(45);
		country4.setName("TonTon");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		Set<Republic> loadedCountries;

		loadedCountries = derivedQueriesRepository.findByNameLike("t", Sort.by("name"));
		assertThat(loadedCountries).containsExactly(country2, country1, country3);

		loadedCountries = derivedQueriesRepository.findByNameLike("t", Sort.by(Sort.Order.by("name").ignoreCase()));
		assertThat(loadedCountries).containsExactly(country3, country1, country2);
	}
	
	@Test
	void ignoreCase_dynamic_inMemory() {
		Republic country1 = new Republic(42);
		country1.setName("Toto_b");
		Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
		Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
		Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
		country1.setLanguages(asHashSet(frFr, enEn));
		
		Republic country2 = new Republic(43);
		country2.setName("TOtO_c");
		country2.setLanguages(asHashSet(frFr, esEs));
		
		Republic country3 = new Republic(44);
		country3.setName("toTO_a");
		country3.setLanguages(asHashSet(frFr, esEs));
		
		Republic country4 = new Republic(45);
		country4.setName("TonTon");
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		Set<Republic> loadedCountries;
		
		loadedCountries = derivedQueriesRepository.findByLanguagesCodeLike("_", Sort.by("name"));
		assertThat(loadedCountries).containsExactly(country2, country1, country3);
		
		loadedCountries = derivedQueriesRepository.findByLanguagesCodeLike("_", Sort.by(Sort.Order.by("name").ignoreCase()));
		assertThat(loadedCountries).containsExactly(country3, country1, country2);
	}
	
	@Test
	void ignoreCase_and() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		country1.setDescription("a description with a keyword");
		Republic country2 = new Republic(43);
		country2.setName("TOtO");
		country2.setDescription("a keyword contained in the description");
		Republic country3 = new Republic(44);
		country3.setName("toTO");
		Republic country4 = new Republic(45);
		country4.setName("TonTon");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		Set<Republic> loadedCountries;
		
		loadedCountries = derivedQueriesRepository.findByNameIgnoreCaseAndDescriptionLike("toTO", "contained");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
	}
	
	@Test
	void ignoreCase_all() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		country1.setDescription("a description with a keyword");
		Republic country2 = new Republic(43);
		country2.setName("TOtO");
		country2.setDescription("a keyword contained in the description");
		Republic country3 = new Republic(44);
		country3.setName("toTO");
		Republic country4 = new Republic(45);
		country4.setName("TonTon");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		Set<Republic> loadedCountries;
		
		loadedCountries = derivedQueriesRepository.findByNameAndDescriptionLikeAllIgnoreCase("toTO", "CoNtAINed");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
	}
	
	@Test
	void orderBy() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
		Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
		Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
		country1.setLanguages(asHashSet(frFr, enEn));
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		country2.setLanguages(asHashSet(frFr, esEs));
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByLanguagesCodeIsOrderByNameAsc(frFr.getCode());
		assertThat(loadedCountries).containsExactly(country2, country1);
		loadedCountries = derivedQueriesRepository.findByLanguagesCodeIsOrderByNameDesc(frFr.getCode());
		assertThat(loadedCountries).containsExactly(country1, country2);
	}
	
	@Test
	void orderBy_dynamic() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
		Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
		Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
		country1.setLanguages(asHashSet(frFr, enEn));
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		country2.setLanguages(asHashSet(frFr, esEs));
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByLanguagesCodeIs(frFr.getCode(), Sort.by("name"));
		assertThat(loadedCountries).containsExactly(country2, country1);
		loadedCountries = derivedQueriesRepository.findByLanguagesCodeIs(frFr.getCode(), Sort.by("name").descending());
		assertThat(loadedCountries).containsExactly(country1, country2);
	}
	
	@Test
	void orderBy_onDepthProperty() {
		Republic country1 = new Republic(42);
		country1.setName("Tonton");
		Person president1 = new Person(666);
		president1.setName("C");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
		country2.setName("Tintin");
		Person president2 = new Person(237);
		president2.setName("A");
		country2.setPresident(president2);
		
		Republic country3 = new Republic(44);
		country3.setName("Tantan");
		Person president3 = new Person(123);
		president3.setName("B");
		country3.setPresident(president3);
		
		Republic country4 = new Republic(45);
		country4.setName("Tata");
		Person president4 = new Person(456);
		president4.setName("me");
		country4.setPresident(president4);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByNameLikeOrderByPresidentNameAsc("T%n");
		assertThat(loadedCountries).containsExactly(country2, country3, country1);
	}
	
	@Test
	void orderBy_criteriaOnCollection_onDepthProperty() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
		Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
		Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
		country1.setLanguages(asHashSet(frFr, enEn));
		Person president1 = new Person(666);
		president1.setName("C");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		Person president2 = new Person(237);
		president2.setName("A");
		country2.setPresident(president2);
		country2.setLanguages(asHashSet(frFr, esEs));
		
		Republic country3 = new Republic(44);
		country3.setName("Titi");
		Person president3 = new Person(123);
		president3.setName("B");
		country3.setPresident(president3);
		country3.setLanguages(asHashSet(frFr));
		
		Republic country4 = new Republic(45);
		country4.setName("Tutu");
		Person president4 = new Person(456);
		president4.setName("me");
		country4.setPresident(president4);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findByLanguagesCodeIsOrderByPresidentNameAsc(frFr.getCode());
		assertThat(loadedCountries).containsExactly(country2, country3, country1);
	}
	
	@Test
	void limit_throwsExceptionBecauseOfCollectionPropertyMapping() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Language frFr = new Language(new PersistableIdentifier<>(77L), "fr_fr");
		Language enEn = new Language(new PersistableIdentifier<>(88L), "en_en");
		Language esEs = new Language(new PersistableIdentifier<>(99L), "es_es");
		country1.setLanguages(asHashSet(frFr, enEn));
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		country2.setLanguages(asHashSet(frFr, esEs));
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		assertThatCode(() -> derivedQueriesRepository.findFirstByLanguagesCodeIs(frFr.getCode()))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessage("Can't limit query when entity graph contains Collection relations");
	}
	
	@Test
	void or() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		country1.setDescription("a description with a keyword");
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
		country2.setName("TOtO");
		country2.setDescription("a keyword contained in the description");
		country2.setEuMember(true);
		Person president2 = new Person(237);
		president2.setName("you");
		
		Republic country3 = new Republic(44);
		country3.setName("toTO");
		country3.setDescription("a keyword contained in the description");
		country3.setEuMember(false);
		
		Republic country4 = new Republic(45);
		country4.setName("TonTon");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		Set<Republic> loadedCountries;
		
		loadedCountries = derivedQueriesRepository.findByNameOrDescription("TOtO", "a description with a keyword");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
		loadedCountries = derivedQueriesRepository.findByNameOrDescriptionAndEuMemberOrPresidentName("TonTon", "a keyword contained in the description", true, "me");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2, country4);
	}
}