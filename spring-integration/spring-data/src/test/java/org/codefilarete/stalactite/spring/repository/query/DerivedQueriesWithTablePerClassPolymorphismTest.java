package org.codefilarete.stalactite.spring.repository.query;


import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.King;
import org.codefilarete.stalactite.engine.model.Language;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Realm;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.State;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;

/**
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		StalactiteRepositoryContextConfigurationBase.class,
		DerivedQueriesWithTablePerClassPolymorphismTest.StalactiteRepositoryContextConfiguration.class
})
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.spring.repository.query",
		// because we have another repository in the same package, we filter them to keep only the appropriate one (it also checks that filtering works !)
		includeFilters = @Filter(
				type = FilterType.ASSIGNABLE_TYPE,
				classes = { DerivedQueriesRepository.class, CountryDerivedQueriesRepository.class })
)
class DerivedQueriesWithTablePerClassPolymorphismTest extends AbstractDerivedQueriesTest {
	
	@Autowired
	private CountryDerivedQueriesRepository countryDerivedQueriesRepository;
	
	@Test
	void crud() {
		Realm realm = new Realm(42);
		realm.setName("Toto");
		Person president = new Person(666);
		president.setName("me");
		realm.setPresident(president);
		King king = new King(999);
		king.setName("still me");
		realm.setKing(king);
		Republic republic = new Republic(43);
		republic.setName("Tata");
		countryDerivedQueriesRepository.saveAll(Arrays.asList(realm, republic));
		
		Set<Country> foundCountries = countryDerivedQueriesRepository.findByNameIn("Toto", "Tata");
		
		Map<String, Country> countryPerName = Iterables.map(foundCountries, Country::getName);
		Country loadedCountry1 = countryPerName.get("Toto");
		assertThat(loadedCountry1).isExactlyInstanceOf(Realm.class);
		assertThat(loadedCountry1.getName()).isEqualTo(realm.getName());
		assertThat(loadedCountry1.getPresident().getName()).isEqualTo(president.getName());
		assertThat(((Realm) loadedCountry1).getKing()).isExactlyInstanceOf(King.class);
		assertThat(((Realm) loadedCountry1).getKing().getName()).isEqualTo(king.getName());
		
		Country loadedCountry2 = countryPerName.get("Tata");
		assertThat(loadedCountry2).isExactlyInstanceOf(Republic.class);
		assertThat(loadedCountry2.getName()).isEqualTo(republic.getName());
		assertThat(loadedCountry2.getPresident()).isNull();
	}
	
	public static class StalactiteRepositoryContextConfiguration {
		
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
					
					.mapPolymorphism(PolymorphismPolicy.<Country>tablePerClass()
							.addSubClass(subentityBuilder(Republic.class)
									.map(Republic::getDeputeCount))
							.addSubClass(subentityBuilder(Realm.class)
									.mapOneToOne(Realm::getKing, entityBuilder(King.class, LONG_TYPE)
											.mapKey(King::getId, IdentifierPolicy.<King, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
											.map(King::getName))))
					.build(persistenceContext);
		}
	}
}