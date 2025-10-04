package org.codefilarete.stalactite.spring.repository.query.bean;

import java.util.Collection;
import java.util.Set;

import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
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
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Selectable.SimpleSelectable;
import org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.spring.repository.query.BeanQuery;
import org.codefilarete.stalactite.spring.repository.query.StalactiteRepositoryContextConfigurationBase;
import org.codefilarete.stalactite.spring.repository.query.bean.BeanQueriesRepository.NamesOnly;
import org.codefilarete.stalactite.spring.repository.query.bean.BeanQueriesRepository.NamesOnly.SimplePerson;
import org.codefilarete.stalactite.spring.repository.query.bean.BeanQueriesRepository.NamesOnlyWithValue;
import org.codefilarete.stalactite.spring.repository.query.bean.BeanQueriesTest.StalactiteRepositoryContextConfiguration;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.reflection.AccessorChain.fromMethodReferences;
import static org.codefilarete.stalactite.dsl.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.query.model.Operators.containsArgNamed;
import static org.codefilarete.stalactite.query.model.Operators.endsWithArgNamed;
import static org.codefilarete.stalactite.query.model.Operators.eq;
import static org.codefilarete.stalactite.query.model.Operators.equalsArgNamed;
import static org.codefilarete.tool.function.Functions.chain;

/**
 * Dedicated test class for orderBy and limit cases : both work only with a mapping that doesn't imply Collection property
 *
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		StalactiteRepositoryContextConfigurationBase.class,
		StalactiteRepositoryContextConfiguration.class
})
@Transactional
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.spring.repository.query",
		// because we have another repository in the same package, we filter them to keep only the appropriate one (it also checks that filtering works !)
		includeFilters = @Filter(
				type = FilterType.ASSIGNABLE_TYPE,
				classes = { BeanQueriesRepository.class, AnotherBeanQueriesRepository.class})
)
public class BeanQueriesTest {
	
	@Autowired
	private BeanQueriesRepository beanQueriesRepository;
	
	@Autowired
	private AnotherBeanQueriesRepository anotherBeanQueriesRepository;
	
	@Test
	void methodHasAMatchingBeanName_beanQueryIsExecuted() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		country1.setEuMember(true);
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		beanQueriesRepository.saveAll(Arrays.asList(country1, country2));

		Republic loadedCountry = anotherBeanQueriesRepository.findEuropeanMember("me");
		assertThat(loadedCountry).isEqualTo(country1);
	}
	
	@Test
	void methodHasAMatchingBeanQuery_beanQueryIsExecuted() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		country1.setEuMember(true);
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		beanQueriesRepository.saveAll(Arrays.asList(country1, country2));

		Set<Republic> loadedCountry = beanQueriesRepository.findEuropeanMemberWithPresidentName("me");
		assertThat(loadedCountry).containsExactly(country1);
	}
	
	@Test
	void methodHasAMatchingBeanQuery_beanQueryIsExecuted_andReturnsSlice() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		country1.setEuMember(true);
		Person president1 = new Person(666);
		president1.setName("Me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		country2.setEuMember(true);
		Person president2 = new Person(667);
		president2.setName("John Do");
		country2.setPresident(president2);
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		country3.setEuMember(true);
		Person president3 = new Person(668);
		president3.setName("Jane Do");
		country3.setPresident(president3);
		Republic country4 = new Republic(45);
		country4.setName("Tonton");
		country4.setEuMember(true);
		Person president4 = new Person(669);
		president4.setName("Saca Do");
		country4.setPresident(president4);
		beanQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		PageRequest pageable = PageRequest.ofSize(2);
		Slice<Republic> loadedCountry;
		loadedCountry = beanQueriesRepository.findEuropeanMemberWithPresidentName_withSlice("%Do", pageable);
		assertThat(loadedCountry)
				.containsExactly(country3, country2);
		
		loadedCountry = beanQueriesRepository.findEuropeanMemberWithPresidentName_withSlice("%Do", pageable.next());
		assertThat(loadedCountry)
				.containsExactly(country4);
	}
	
	@Test
	void methodHasAMatchingBeanQuery_beanQueryIsExecuted_andReturnsPage() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		country1.setEuMember(true);
		Person president1 = new Person(666);
		president1.setName("Me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		country2.setEuMember(true);
		Person president2 = new Person(667);
		president2.setName("John Do");
		country2.setPresident(president2);
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		country3.setEuMember(true);
		Person president3 = new Person(668);
		president3.setName("Jane Do");
		country3.setPresident(president3);
		Republic country4 = new Republic(45);
		country4.setName("Tonton");
		country4.setEuMember(true);
		Person president4 = new Person(669);
		president4.setName("Saca Do");
		country4.setPresident(president4);
		beanQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		PageRequest pageable = PageRequest.ofSize(2);
		Page<Republic> loadedCountry;
		loadedCountry = beanQueriesRepository.findEuropeanMemberWithPresidentName_withPage("%o", pageable);
		assertThat(loadedCountry)
				.containsExactly(country3, country2);
		
		loadedCountry = beanQueriesRepository.findEuropeanMemberWithPresidentName_withPage("%o", pageable.next());
		assertThat(loadedCountry)
				.containsExactly(country4);
	}
	
	@Test
	void methodHasAMatchingBeanQueryWithAnExplicitRepositoryClass_beanQueryIsExecuted() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		country1.setEuMember(true);
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		Person president2 = new Person(777);
		president2.setName("me");
		country2.setPresident(president2);
		beanQueriesRepository.saveAll(Arrays.asList(country1, country2));

		Set<Republic> loadedCountries;
		loadedCountries = beanQueriesRepository.findEuropeanCountryForPresident("me");
		assertThat(loadedCountries).containsExactly(country1);
		
		// overridden by anotherOverrideOfFindEuropeanMemberWithPresidentName => retrieves non-EU members
		loadedCountries = anotherBeanQueriesRepository.findEuropeanCountryForPresident("me");
		assertThat(loadedCountries).containsExactly(country2);
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
		beanQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Collection<NamesOnlyWithValue> loadedProjectionWithValueAnnotation = beanQueriesRepository.getByName("Toto", NamesOnlyWithValue.class);
		assertThat(loadedProjectionWithValueAnnotation).extracting(NamesOnly::getName)
				.containsExactlyInAnyOrder(country1.getName(), country2.getName());
		assertThat(loadedProjectionWithValueAnnotation).extracting(NamesOnlyWithValue::getPresidentName)
				.containsExactlyInAnyOrder(
						country1.getPresident().getName() + "-" + country1.getPresident().getId().getDelegate(),
						country2.getPresident().getName() + "-" + country2.getPresident().getId().getDelegate()
				);
		assertThat(loadedProjectionWithValueAnnotation).extracting(chain(NamesOnly::getPresident, SimplePerson::getName))
				.containsExactlyInAnyOrder(country1.getPresident().getName(), country2.getPresident().getName());
		
		Collection<NamesOnly> loadedProjectionWithoutValueAnnotation = beanQueriesRepository.getByName("Toto", NamesOnly.class);
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
		beanQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		Set<NamesOnly> loadedNamesOnly = beanQueriesRepository.getByNameLikeOrderByPresidentNameAsc("%o%");
		assertThat(loadedNamesOnly).extracting(NamesOnly::getName).containsExactly(country1.getName(), country4.getName());
	}
	
	public static class StalactiteRepositoryContextConfiguration {

		@BeanQuery
		public ExecutableEntityQuery<Republic, ?> findEuropeanMember(EntityPersister<Republic, Identifier<Long>> countryPersister) {
			return countryPersister.selectWhere(Republic::isEuMember, eq(true))
					.and(fromMethodReferences(Republic::getPresident, Person::getName), equalsArgNamed("presidentName", String.class));
		}
		
		@BeanQuery(method = "findEuropeanCountryForPresident")
		public ExecutableEntityQuery<Republic, ?> anOverrideOfFindEuropeanMemberWithPresidentName(EntityPersister<Republic, Identifier<Long>> countryPersister) {
			return countryPersister.selectWhere(Republic::isEuMember, eq(true))
					.and(fromMethodReferences(Republic::getPresident, Person::getName), equalsArgNamed("presidentName", String.class));
		}

		@BeanQuery(method = "findEuropeanCountryForPresident", repositoryClass = AnotherBeanQueriesRepository.class)
		public ExecutableEntityQuery<Republic, ?> anotherOverrideOfFindEuropeanMemberWithPresidentName(EntityPersister<Republic, Identifier<Long>> countryPersister) {
			// this one retrieves non-EU members to help checking it is really invoked
			return countryPersister.selectWhere(Republic::isEuMember, eq(false))
					.and(fromMethodReferences(Republic::getPresident, Person::getName), equalsArgNamed("presidentName", String.class));
		}

		@BeanQuery(method = "findEuropeanMemberWithPresidentName")
		public ExecutableEntityQuery<Republic, ?> aMethodThatDoesntMatchAnyRepositoryMethodName(EntityPersister<Republic, Identifier<Long>> countryPersister) {
			return countryPersister.selectWhere(Republic::isEuMember, eq(true))
					.and(fromMethodReferences(Republic::getPresident, Person::getName), equalsArgNamed("presidentName", String.class));
		}
		
		@BeanQuery(method = "findEuropeanMemberWithPresidentName_withSlice")
		public ExecutableEntityQuery<Republic, ?> aMethodThatDoesntMatchAnyRepositoryMethodName_withSlice(EntityPersister<Republic, Identifier<Long>> countryPersister) {
			return countryPersister.selectWhere(Republic::isEuMember, eq(true))
					.and(fromMethodReferences(Republic::getPresident, Person::getName), endsWithArgNamed("presidentName", String.class))
					.orderBy(fromMethodReferences(Republic::getPresident, Person::getName));
		}
		
		@BeanQuery(
				method = "findEuropeanMemberWithPresidentName_withPage",
				counterBean = "aMethodThatDoesntMatchAnyRepositoryMethodName_count"
		)
		public ExecutableEntityQuery<Republic, ?> aMethodThatDoesntMatchAnyRepositoryMethodName_withPage(EntityPersister<Republic, Identifier<Long>> countryPersister) {
			return countryPersister.selectWhere(Republic::isEuMember, eq(true))
					.and(fromMethodReferences(Republic::getPresident, Person::getName), endsWithArgNamed("presidentName", String.class))
					.orderBy(fromMethodReferences(Republic::getPresident, Person::getName));
		}
		
		@Bean
		public ExecutableProjectionQuery<Republic, ?> aMethodThatDoesntMatchAnyRepositoryMethodName_count(EntityPersister<Republic, Identifier<Long>> countryPersister) {
			return countryPersister.selectProjectionWhere(select -> {
						select.clear();
						select.add(Operators.count(new SimpleSelectable<>("*", long.class)), "count");
					}, Republic::isEuMember, eq(true))
					.and(fromMethodReferences(Republic::getPresident, Person::getName), endsWithArgNamed("presidentName", String.class));
		}
		
		@BeanQuery
		public ExecutableEntityQuery<Republic, ?> getByNameLikeOrderByPresidentNameAsc(EntityPersister<Republic, Identifier<Long>> countryPersister) {
			return countryPersister.selectWhere(Republic::getName, containsArgNamed("name", String.class))
					.orderBy(fromMethodReferences(Republic::getPresident, Person::getName));
		}
		
		@BeanQuery
		public ExecutableEntityQuery<Republic, ?> getByName(EntityPersister<Republic, Identifier<Long>> countryPersister) {
			return countryPersister.selectWhere(Republic::getName, equalsArgNamed("name", String.class));
		}
		
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
							.mapOneToOne(Person::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
									.mapKey(Vehicle::getId, IdentifierPolicy.<Vehicle, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
									.map(Vehicle::getColor))
					)
					.mapOneToMany(Republic::getStates, entityBuilder(State.class, LONG_TYPE)
							.mapKey(State::getId, IdentifierPolicy.<State, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.map(State::getName))
					.mapManyToMany(Republic::getLanguages, entityBuilder(Language.class, LONG_TYPE)
							.mapKey(Language::getId, IdentifierPolicy.<Language, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.usingConstructor(Language::new, "id", "code")
							.map(Language::getCode).setByConstructor())
					.build(persistenceContext);
		}
	}
}

