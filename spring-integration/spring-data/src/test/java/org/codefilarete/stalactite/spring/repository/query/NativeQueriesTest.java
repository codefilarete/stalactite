package org.codefilarete.stalactite.spring.repository.query;

import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Language;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.State;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.query.NativeQueriesTest.StalactiteRepositoryContextConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
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
public class NativeQueriesTest extends AbstractNativeQueriesTest {
	
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
