package org.codefilarete.stalactite.spring.repository.query;


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
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

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
class DerivedQueriesWithTablePerClassPolymorphismTest extends AbstractDerivedQueriesWithPolymorphismTest {
	
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