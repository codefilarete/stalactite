package org.codefilarete.stalactite.spring.repository.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.dsl.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Language;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.State;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.tool.bean.Randomizer;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.codefilarete.stalactite.dsl.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;

/**
 * Those tests are aimed at detecting some eventual non thread-safety while loading some entities or projections.
 * It's like trying to catch neutrinos, well, with the difference we hope to never catch any problem !
 * Hence, we do our best by creating some race-condition to detect eventual non-thread-safe loading.
 *
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		StalactiteRepositoryContextConfigurationBase.class,
		ThreadSafetyTest.StalactiteRepositoryContextConfiguration.class
})
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.spring.repository.query",
		// because we have another repository in the same package, we filter them to keep only the appropriate one (it also checks that filtering works !)
		includeFilters = @Filter(
				type = FilterType.ASSIGNABLE_TYPE,
				classes = ThreadSafetyRepository.class)
)
@Transactional
public class ThreadSafetyTest {
	
	@Autowired
	protected ThreadSafetyRepository threadSafetyRepository;
	
	@Test
	void in() throws InterruptedException, ExecutionException {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		Republic country3 = new Republic(44);
		country3.setName("Titi");
		Republic country4 = new Republic(45);
		country4.setName("Tutu");
		Republic country5 = new Republic(46);
		country5.setName("Tonton");
		Republic country6 = new Republic(47);
		country6.setName("Toutou");
		Republic country7 = new Republic(48);
		country7.setName("Tintin");
		
		List<Republic> allCountries = Arrays.asList(country1, country2, country3, country4, country5, country6, country7);
		threadSafetyRepository.saveAll(allCountries);
		
		Map<Identifier<Long>, Republic> countryById = Iterables.map(allCountries, Republic::getId);
		Set<Callable<Runnable>> tasks = buildInAsserters(countryById);
		
		// 3 Threads is arbitrary, I hope it's sufficient to detect eventual not-thread-safe loading
		ExecutorService executors = Executors.newFixedThreadPool(3);
		List<Future<Runnable>> executionsResult = executors.invokeAll(tasks);
		for (Future<Runnable> executionResult : executionsResult) {
			executionResult.get().run();
		}
	}
	
	private Set<Callable<Runnable>> buildInAsserters(Map<Identifier<Long>, Republic> countryById) {
		Set<Callable<Runnable>> tasks = new HashSet<>();
		// 200 is arbitrary, we only need a "sufficient amount" of task to eventually make some conflicts happen
		for (int i = 0; i < 200; i++) {
			tasks.add(() -> {
				try {
					List<Identifier<Long>> randomIdentifiers = Randomizer.INSTANCE.drawElements(countryById.keySet(), Randomizer.INSTANCE.drawInt(1, 7));
					Set<Republic> loadedCountries = threadSafetyRepository.findByIdIn(randomIdentifiers);
					Set<Republic> expectedCountries = countryById.entrySet().stream()
							.filter(entry -> randomIdentifiers.contains(entry.getKey()))
							.map(Entry::getValue).collect(Collectors.toSet());
					return () -> assertThat(loadedCountries).containsExactlyInAnyOrderElementsOf(expectedCountries);
				} catch (RuntimeException e) {
					return () -> fail("An exception occurred: " + e.getMessage(), e);
				}
			});
		}
		return tasks;
	}
	
	@Test
	void in_pageable() throws InterruptedException, ExecutionException {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		Republic country3 = new Republic(44);
		country3.setName("Titi");
		Republic country4 = new Republic(45);
		country4.setName("Tutu");
		Republic country5 = new Republic(46);
		country5.setName("Tonton");
		Republic country6 = new Republic(47);
		country6.setName("Toutou");
		Republic country7 = new Republic(48);
		country7.setName("Tintin");
		
		List<Republic> allCountries = Arrays.asList(country1, country2, country3, country4, country5, country6, country7);
		threadSafetyRepository.saveAll(allCountries);
		
		Map<Identifier<Long>, Republic> countryById = Iterables.map(allCountries, Republic::getId);
		Set<Callable<Runnable>> tasks = buildInPageableAsserters(countryById);
		
		// 3 Threads is arbitrary, I hope it's sufficient to detect eventual not-thread-safe loading
		ExecutorService executors = Executors.newFixedThreadPool(3);
		List<Future<Runnable>> executionsResult = executors.invokeAll(tasks);
		for (Future<Runnable> executionResult : executionsResult) {
			executionResult.get().run();
		}
	}
	
	private Set<Callable<Runnable>> buildInPageableAsserters(Map<Identifier<Long>, Republic> countryById) {
		Set<Callable<Runnable>> tasks = new HashSet<>();
		Map<String, Set<Republic>> countriesPerLike = new HashMap<>();
		countriesPerLike.put("%x%", Collections.emptySet());
		countriesPerLike.put("%in%", Arrays.asSet(
				countryById.get(new PersistedIdentifier<>(48L))));
		countriesPerLike.put("%i%", Arrays.asSet(
				countryById.get(new PersistedIdentifier<>(44L)),
				countryById.get(new PersistedIdentifier<>(48L))));
		countriesPerLike.put("%o%", Arrays.asSet(
				countryById.get(new PersistedIdentifier<>(42L)),
				countryById.get(new PersistedIdentifier<>(46L)),
				countryById.get(new PersistedIdentifier<>(47L))));
		countriesPerLike.put("%n%", Arrays.asSet(
				countryById.get(new PersistedIdentifier<>(46L)),
				countryById.get(new PersistedIdentifier<>(48L))));
		countriesPerLike.put("T%", Arrays.asSet(
				countryById.get(new PersistedIdentifier<>(42L)),
				countryById.get(new PersistedIdentifier<>(43L)),
				countryById.get(new PersistedIdentifier<>(44L)),
				countryById.get(new PersistedIdentifier<>(45L)),
				countryById.get(new PersistedIdentifier<>(46L)),
				countryById.get(new PersistedIdentifier<>(47L)),
				countryById.get(new PersistedIdentifier<>(48L))));
		Map<String, Function<Republic, Comparable>> possibleSorts = new HashMap<>();
		possibleSorts.put("name", Republic::getName);
		possibleSorts.put("id", Republic::hashCode);
		// 200 is arbitrary, we only need a "sufficient amount" of task to eventually make some conflicts happen
		for (int i = 0; i < 200; i++) {
			tasks.add(() -> {
				try {
					Entry<String, Set<Republic>> randomLike = Randomizer.INSTANCE.drawElement(new ArrayList<>(countriesPerLike.entrySet()));
					Entry<String, Function<Republic, Comparable>> sort = Randomizer.INSTANCE.drawElement(new ArrayList<>(possibleSorts.entrySet()));
					Set<Republic> loadedCountries = threadSafetyRepository.findByNameLike(randomLike.getKey(), Sort.by(sort.getKey()));
					Set<Republic> expectedCountries = new TreeSet<>(Comparator.comparing(sort.getValue()));
					expectedCountries.addAll(randomLike.getValue());
					return () -> assertThat(loadedCountries).containsExactlyInAnyOrderElementsOf(expectedCountries);
				} catch (RuntimeException e) {
					return () -> fail("An exception occurred: " + e.getMessage(), e);
				}
			});
		}
		return tasks;
	}
	
	public static class StalactiteRepositoryContextConfiguration {
		
		@Bean
		public EntityPersister<Republic, Identifier<Long>> countryPersister(PersistenceContext persistenceContext) {
			// Because this test class inherits from an abstract one that instantiates Republic entities (because it is shared with other
			// polymorphic test classes), we map Republic instead of Country, else, we get some exception because a persister can only persist
			// instance of its defined type
			return entityBuilder(Republic.class, LONG_TYPE)
					.mapKey(Republic::getId, IdentifierPolicy.<Country, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
					.map(Republic::getName)
					.map(Republic::getDescription)
					.map(Republic::isEuMember)
					.embed(Republic::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapOneToOne(Republic::getPresident, entityBuilder(Person.class, LONG_TYPE)
							.mapKey(Person::getId, IdentifierPolicy.<Person, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.map(Person::getName)
							.mapCollection(Person::getNicknames, String.class)
							.mapMap(Person::getPhoneNumbers, String.class, String.class)
							.mapOneToOne(Person::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
									.mapKey(Vehicle::getId, IdentifierPolicy.<Vehicle, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
									.map(Vehicle::getColor)))
					.mapOneToMany(Republic::getStates, entityBuilder(State.class, LONG_TYPE)
							.mapKey(State::getId, IdentifierPolicy.<State, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.map(State::getName))
					.mapManyToMany(Republic::getLanguages, entityBuilder(Language.class, LONG_TYPE)
							.mapKey(Language::getId, IdentifierPolicy.<Language, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.usingConstructor(Language::new, "id", "code")
							.map(Language::getCode).setByConstructor()
					)
					.build(persistenceContext);
		}
	}
}
