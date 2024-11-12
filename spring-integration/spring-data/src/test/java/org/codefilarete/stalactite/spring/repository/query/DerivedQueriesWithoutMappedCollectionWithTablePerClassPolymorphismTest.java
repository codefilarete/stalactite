package org.codefilarete.stalactite.spring.repository.query;


import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;

/**
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		DerivedQueriesWithoutMappedCollectionWithTablePerClassPolymorphismTest.StalactiteRepositoryContextConfiguration.class
})
class DerivedQueriesWithoutMappedCollectionWithTablePerClassPolymorphismTest extends AbstractDerivedQueriesWithoutMappedCollectionTest {
	
	public static class StalactiteRepositoryContextConfiguration extends StalactiteRepositoryContextConfigurationWithoutCollection {
		
		@Bean
		public EntityPersister<Republic, Identifier<Long>> countryPersister(PersistenceContext persistenceContext) {
			return entityBuilder(Republic.class, LONG_TYPE)
					.mapKey(Republic::getId, IdentifierPolicy.<Country, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
					.map(Republic::getName)
					.map(Republic::getDescription)
					.map(Republic::isEuMember)
					.map(Republic::getDeputeCount)
					.embed(Republic::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapOneToOne(Republic::getPresident, entityBuilder(Person.class, LONG_TYPE)
							.mapKey(Person::getId, IdentifierPolicy.<Person, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.map(Person::getName)
							.mapCollection(Person::getNicknames, String.class)
							.mapOneToOne(Person::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
									.mapKey(Vehicle::getId, IdentifierPolicy.<Vehicle, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
									.map(Vehicle::getColor)))
					.build(persistenceContext);
		}
	}
}