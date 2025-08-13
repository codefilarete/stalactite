package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.Set;

import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.King;
import org.codefilarete.stalactite.engine.model.Language;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Realm;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.spring.repository.query.StalactiteRepositoryContextConfigurationBase;
import org.codefilarete.stalactite.spring.repository.query.nativ.JoinTableNativeQueriesTest.StalactiteRepositoryContextConfiguration;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;

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
				classes = JoinTableNativeQueriesRepository.class)
)
class JoinTableNativeQueriesTest {
	
	@Autowired
	private JoinTableNativeQueriesRepository derivedQueriesRepository;
	
	@Test
	void in() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Realm country2 = new Realm(43);
		country2.setName("Tata");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
		
		Set<Country> loadedCountries;
		loadedCountries = derivedQueriesRepository.loadByIdIn(new PersistedIdentifier<>(42L), new PersistedIdentifier<>(43L), new PersistedIdentifier<>(44L));
		assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
	}
	
	public static class StalactiteRepositoryContextConfiguration {
		
		@Bean
		public EntityPersister<Country, Identifier<Long>> countryPersister(PersistenceContext persistenceContext) {
			// Because this test class inherits from an abstract one that instantiates Republic entities (because it is shared with other
			// polymorphic test classes), we map Republic instead of Country, else, we get some exception because a persister can only persist
			// instance of its defined type
			// Note that, comparing to other test classes, the aggregate is simplified because it's quite boring to write and maintain the native SQL
			// of the whole graph in the repository !
			return entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, IdentifierPolicy.<Country, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
					.map(Country::getName)
					.map(Country::isEuMember)
					.mapOneToOne(Country::getPresident, entityBuilder(Person.class, LONG_TYPE)
							.mapKey(Person::getId, IdentifierPolicy.<Person, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.map(Person::getName)
							.mapOneToOne(Person::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
									.mapKey(Vehicle::getId, IdentifierPolicy.<Vehicle, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
									.map(Vehicle::getColor)))
					.mapManyToMany(Country::getLanguages, entityBuilder(Language.class, LONG_TYPE)
							.mapKey(Language::getId, IdentifierPolicy.<Language, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.usingConstructor(Language::new, "id", "code")
							.map(Language::getCode).setByConstructor()
					)
					.mapPolymorphism(PolymorphismPolicy.joinTable(Country.class)
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