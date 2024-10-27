package org.codefilarete.stalactite.spring.repository.query;


import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;

/**
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		DerivedQueriesWithoutMappedCollectionWithSingleTablePolymorphismTest.StalactiteRepositoryContextConfiguration.class
})
class DerivedQueriesWithoutMappedCollectionWithSingleTablePolymorphismTest extends AbstractDerivedQueriesWithoutMappedCollectionTest {
	
	public static class StalactiteRepositoryContextConfiguration extends StalactiteRepositoryContextConfigurationWithoutCollection {
		
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
					
					.mapPolymorphism(PolymorphismPolicy.<Country>singleTable()
							.addSubClass(subentityBuilder(Republic.class)
									.map(Republic::getDeputeCount), "Republic"))
					.build(persistenceContext);
		}
	}
}