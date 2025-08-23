package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.Collection;
import java.util.Set;

import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Language;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.spring.repository.query.nativ.NativeQueriesRepository.NamesOnly;
import org.codefilarete.stalactite.spring.repository.query.nativ.NativeQueriesRepository.NamesOnly.SimplePerson;
import org.codefilarete.stalactite.spring.repository.query.nativ.NativeQueriesRepository.NamesOnlyWithValue;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.codefilarete.tool.collection.Arrays.asHashSet;
import static org.codefilarete.tool.function.Functions.chain;

/**
 * Dedicated test class for orderBy and limit cases : both work only with a mapping that doesn't imply Collection property
 * 
 * @author Guillaume Mary
 */
@Transactional
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.spring.repository.query",
		// because we have another repository in the same package, we filter them to keep only the appropriate one (it also checks that filtering works !)
		includeFilters = @Filter(
				type = FilterType.ASSIGNABLE_TYPE,
				classes = NativeQueriesRepository.class)
)
abstract class AbstractNativeQueriesTest {
	
	@Autowired
	private NativeQueriesRepository derivedQueriesRepository;
	
	@Test
	void projection_resultIsSingle() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		NamesOnly loadedCountry = derivedQueriesRepository.getByName("Toto");
		assertThat(loadedCountry.getName()).isEqualTo(country1.getName());
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

		Collection<NamesOnlyWithValue> loadedProjectionWithValueAnnotation = derivedQueriesRepository.getByName("Toto", NamesOnlyWithValue.class);
		assertThat(loadedProjectionWithValueAnnotation).extracting(NamesOnly::getName)
				.containsExactlyInAnyOrder(country1.getName(), country2.getName());
		assertThat(loadedProjectionWithValueAnnotation).extracting(NamesOnlyWithValue::getPresidentName)
				.containsExactlyInAnyOrder(
						country1.getPresident().getName() + "-" + country1.getPresident().getId().getDelegate(),
						country2.getPresident().getName() + "-" + country2.getPresident().getId().getDelegate()
				);
		assertThat(loadedProjectionWithValueAnnotation).extracting(chain(NamesOnly::getPresident, SimplePerson::getName))
				.containsExactlyInAnyOrder(country1.getPresident().getName(), country2.getPresident().getName());

		Collection<NamesOnly> loadedProjectionWithoutValueAnnotation = derivedQueriesRepository.getByName("Toto", NamesOnly.class);
		assertThat(loadedProjectionWithoutValueAnnotation).extracting(NamesOnly::getName)
				.containsExactlyInAnyOrder(country1.getName(), country2.getName());
		assertThat(loadedProjectionWithoutValueAnnotation).extracting(chain(NamesOnly::getPresident, SimplePerson::getName))
				.containsExactlyInAnyOrder(country1.getPresident().getName(), country2.getPresident().getName());
	}
	
	@Test
	void projection_orderBy() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("Me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		Person president2 = new Person(667);
		president2.setName("John Do");
		country2.setPresident(president2);
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		Person president3 = new Person(668);
		president3.setName("Jane Do");
		country3.setPresident(president3);
		Republic country4 = new Republic(45);
		country4.setName("Tonton");
		Person president4 = new Person(669);
		president4.setName("Saca do");
		country4.setPresident(president4);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));

		Set<NamesOnly> loadedNamesOnly1 = derivedQueriesRepository.getByNameLikeOrderByPresidentNameAsc("o");
		assertThat(loadedNamesOnly1).extracting(NamesOnly::getName).containsExactly(country1.getName(), country4.getName());
	}
	
	@Test
	void projection_pageable() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("Me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		Person president2 = new Person(667);
		president2.setName("John Do");
		country2.setPresident(president2);
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		Person president3 = new Person(668);
		president3.setName("Jane Do");
		country3.setPresident(president3);
		Republic country4 = new Republic(45);
		country4.setName("Tonton");
		Person president4 = new Person(669);
		president4.setName("Saca do");
		country4.setPresident(president4);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));

		PageRequest pageable = PageRequest.ofSize(3);
		Slice<NamesOnly> loadedNamesOnly;
		loadedNamesOnly = derivedQueriesRepository.getByNameLikeOrderByPresidentNameAsc("t", pageable);
		assertThat(loadedNamesOnly).extracting(NamesOnly::getName)
				.containsExactly(country3.getName(), country2.getName(), country1.getName());

		loadedNamesOnly = derivedQueriesRepository.getByNameLikeOrderByPresidentNameAsc("t", pageable.next());
		assertThat(loadedNamesOnly).extracting(NamesOnly::getName)
				.containsExactly(country4.getName());
		
		// Page type return test
		loadedNamesOnly = derivedQueriesRepository.getByNameLikeOrderByPresidentNameDesc("t", pageable);
		assertThat(loadedNamesOnly).extracting(NamesOnly::getName)
				.containsExactly(country4.getName(), country1.getName(), country2.getName());
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
		
		Republic loadedCountry = derivedQueriesRepository.loadByIdAndName(new PersistedIdentifier<>(42L), "Toto");
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
		
		Republic loadedCountry;
		
		loadedCountry = derivedQueriesRepository.loadByPresidentVehicleColor(new Color(123));
		assertThat(loadedCountry).isEqualTo(country1);
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
	}
	
	@Test
	void oneResultExpected_severalResults_throwsException() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		assertThatCode(() -> derivedQueriesRepository.loadByName("Toto"))
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
	
	// Hereafter are simple Criteria tests. We can't use @Nested with Junit and inheritance :'(
	// (test fails by not getting enclosing class context which is Spring info here) 
	
	@Test
	void equal() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		derivedQueriesRepository.save(country1);
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Republic loadedCountry = derivedQueriesRepository.loadByName("Toto");
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
		
		Republic loadedCountry = derivedQueriesRepository.loadByEuMemberIsTrue();
		assertThat(loadedCountry).isEqualTo(country2);
	}
	
	@Test
	void in() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Republic> loadedCountries;
		loadedCountries = derivedQueriesRepository.loadByIdIn(Arrays.asList(new PersistedIdentifier<>(42L), new PersistedIdentifier<>(43L), new PersistedIdentifier<>(44L)));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
		
		loadedCountries = derivedQueriesRepository.loadByIdIn(new PersistedIdentifier<>(42L), new PersistedIdentifier<>(43L), new PersistedIdentifier<>(44L));
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
		loadedCountries = derivedQueriesRepository.loadByNameIn("Titi");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
	}
	
	@Test
	void severalNativeQueries_theOneMatchingDatabaseIsChosen() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3));

		Set<Republic> loadedCountries;
		loadedCountries = derivedQueriesRepository.loadByNameIn("Titi");
		assertThat(loadedCountries).containsExactlyInAnyOrder(country2);
	}
}
