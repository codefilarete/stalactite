package org.codefilarete.stalactite.spring.repository.query;

import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.dsl.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.spring.repository.query.DerivedQueriesWithoutMappedCollectionTest.StalactiteRepositoryContextConfigurationWithoutCollection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;

import static org.codefilarete.stalactite.dsl.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;

/**
 * Dedicated test class for orderBy and limit cases : both work only with a mapping that doesn't imply Collection property
 * 
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		StalactiteRepositoryContextConfigurationBase.class,
		StalactiteRepositoryContextConfigurationWithoutCollection.class
})
@Transactional
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.spring.repository.query",
		// because we have another repository in the same package, we filter them to keep only the appropriate one (it also checks that filtering works !)
		includeFilters = @Filter(
				type = FilterType.ASSIGNABLE_TYPE,
				classes = DerivedQueriesWithoutMappedCollectionRepository.class)
)
public class DerivedQueriesWithoutMappedCollectionTest extends AbstractDerivedQueriesWithoutMappedCollectionTest {
	
	public static class StalactiteRepositoryContextConfigurationWithoutCollection {
		
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
							.mapOneToOne(Person::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
									.mapKey(Vehicle::getId, IdentifierPolicy.<Vehicle, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
									.map(Vehicle::getColor)))
					.build(persistenceContext);
		}
	}
}
