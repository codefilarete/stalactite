package org.codefilarete.stalactite.engine.configurer.dslresolver;

import java.util.Set;

import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.model.AncestorJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.MappingJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.State;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.tool.collection.Iterables.first;
import static org.mockito.Mockito.mock;

/**
 * Tests the non-recursive, stack-based tree traversal in {@link RelationsMetadataResolver}.
 * The key property verified is that relations are resolved at every depth of the entity graph
 * (not just the first level), for both one-to-one and one-to-many relation types.
 */
class RelationsMetadataResolverTest {

	private final AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));

	/**
	 * Verifies that a three-level deep one-to-one chain (Country -> City -> State) has its
	 * relations resolved at every level, not just at the root.
	 *
	 * Country --[1:1]--> City --[1:1]--> State
	 */
	@Test
	void resolve_oneToOneChain_relationsAreResolvedAtEveryDepth() {
		FluentEntityMappingBuilder<State, Identifier<Long>> stateBuilder =
				entityBuilder(State.class, Identifier.LONG_TYPE)
						.mapKey(State::getId, ALREADY_ASSIGNED)
						.map(State::getName);

		FluentEntityMappingBuilder<City, Identifier<Long>> cityBuilder =
				entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, ALREADY_ASSIGNED)
						.map(City::getName)
						.mapOneToOne(City::getState, stateBuilder);

		FluentEntityMappingBuilder<Country, Identifier<Long>> countryBuilder =
				entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.mapOneToOne(Country::getCapital, cityBuilder);

		// --- when ---
		Entity<Country, Identifier<Long>, ?> countryEntity =
				testInstance.resolve(countryBuilder.getConfiguration());

		// --- then ---
		// Depth 1: Country should have its relation to City resolved
		Set<MappingJoin<?, ?, ?>> countryRelations = countryEntity.getRelations();
		assertThat(countryRelations).hasSize(1);
		MappingJoin<?, ?, ?> firstCountryRelation = first(countryRelations);
		assertThat(firstCountryRelation).isInstanceOf(ResolvedOneToOneRelation.class);

		// Depth 2: the target City entity should itself have its relation to State resolved
		ResolvedOneToOneRelation<?, ?, ?, ?, ?> countryToCityRelation = (ResolvedOneToOneRelation<?, ?, ?, ?, ?>) firstCountryRelation;
		Entity<?, ?, ?> cityEntity = countryToCityRelation.getTargetEntity();
		assertThat(cityEntity.getEntityType()).isEqualTo(City.class);
		
		Set<MappingJoin<?, ?, ?>> cityRelations = cityEntity.getRelations();
		assertThat(cityRelations).hasSize(1);
		MappingJoin<?, ?, ?> firstCityRelation = first(cityRelations);
		assertThat(firstCityRelation).isInstanceOf(ResolvedOneToOneRelation.class);

		// Depth 3: the target State entity has no further relations
		ResolvedOneToOneRelation<?, ?, ?, ?, ?> cityToStateRelation = (ResolvedOneToOneRelation<?, ?, ?, ?, ?>) firstCityRelation;
		assertThat(cityToStateRelation.getTargetEntity().getEntityType()).isEqualTo(State.class);
		Entity<?, ?, ?> stateEntity = cityToStateRelation.getTargetEntity();
		assertThat(stateEntity.getRelations()).isEmpty();
	}

	/**
	 * Verifies that a three-level deep one-to-many chain (Country -> City -> Person) has its
	 * relations resolved at every level, not just at the root.
	 *
	 * Country --[1:N]--> City --[1:N]--> Person
	 */
	@Test
	void resolve_oneToManyChain_relationsAreResolvedAtEveryDepth() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder =
				entityBuilder(Person.class, Identifier.LONG_TYPE)
						.mapKey(Person::getId, ALREADY_ASSIGNED)
						.map(Person::getName);

		FluentEntityMappingBuilder<City, Identifier<Long>> cityBuilder =
				entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, ALREADY_ASSIGNED)
						.map(City::getName)
						.mapOneToMany(City::getPersons, personBuilder);

		FluentEntityMappingBuilder<Country, Identifier<Long>> countryBuilder =
				entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.mapOneToMany(Country::getCities, cityBuilder);

		// --- when ---
		Entity<Country, Identifier<Long>, ?> countryEntity =
				testInstance.resolve(countryBuilder.getConfiguration());

		// --- then ---
		// Depth 1: Country should have its relation to City resolved
		Set<MappingJoin<?, ?, ?>> countryRelations = countryEntity.getRelations();
		assertThat(countryRelations).hasSize(1);
		MappingJoin<?, ?, ?> firstCountryRelation = first(countryRelations);
		assertThat(firstCountryRelation).isInstanceOf(ResolvedOneToManyRelation.class);

		// Depth 2: the target City entity should itself have its relation to Person resolved
		ResolvedOneToManyRelation<?, ?, ?, ?, ?, ?, ?> countryToCityRelation =
				(ResolvedOneToManyRelation<?, ?, ?, ?, ?, ?, ?>) firstCountryRelation;
		Entity<?, ?, ?> cityEntity = countryToCityRelation.getTargetEntity();
		assertThat(cityEntity.getEntityType()).isEqualTo(City.class);
		
		Set<MappingJoin<?, ?, ?>> cityRelations = cityEntity.getRelations();
		assertThat(cityRelations).hasSize(1);
		MappingJoin<?, ?, ?> firstCityRelation = first(cityRelations);
		assertThat(firstCityRelation).isInstanceOf(ResolvedOneToManyRelation.class);

		// Depth 3: Person has no further relations
		ResolvedOneToManyRelation<?, ?, ?, ?, ?, ?, ?> cityToPersonRelation = (ResolvedOneToManyRelation<?, ?, ?, ?, ?, ?, ?>) firstCityRelation;
		Entity<?, ?, ?> personEntity = cityToPersonRelation.getTargetEntity();
		assertThat(personEntity.getEntityType()).isEqualTo(Person.class);
		assertThat(personEntity.getRelations()).isEmpty();
	}

	/**
	 * Verifies that a mix of one-to-one and one-to-many relations at the same depth level
	 * are both resolved.
	 *
	 * Country --[1:1]--> Person (president)
	 *         --[1:N]--> City (cities)
	 */
	@Test
	void resolve_mixedRelationsAtSameLevel_bothAreResolved() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder =
				entityBuilder(Person.class, Identifier.LONG_TYPE)
						.mapKey(Person::getId, ALREADY_ASSIGNED)
						.map(Person::getName);

		FluentEntityMappingBuilder<City, Identifier<Long>> cityBuilder =
				entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, ALREADY_ASSIGNED)
						.map(City::getName);

		FluentEntityMappingBuilder<Country, Identifier<Long>> countryBuilder =
				entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.mapOneToOne(Country::getPresident, personBuilder)
						.mapOneToMany(Country::getCities, cityBuilder);

		// --- when ---
		Entity<Country, Identifier<Long>, ?> countryEntity =
				testInstance.resolve(countryBuilder.getConfiguration());

		// --- then ---
		Set<MappingJoin<?, ?, ?>> countryRelations = countryEntity.getRelations();
		assertThat(countryRelations).hasSize(2);
		MappingJoin<?, ?, ?> countryRelation1 = Iterables.asList(countryRelations).get(0);
		MappingJoin<?, ?, ?> countryRelation2 = Iterables.asList(countryRelations).get(1);
		assertThat(countryRelation1).isInstanceOf(ResolvedOneToOneRelation.class);
		assertThat(countryRelation2).isInstanceOf(ResolvedOneToManyRelation.class);
		assertThat(((ResolvedOneToOneRelation) countryRelation1).getTargetEntity().getEntityType()).isEqualTo(Person.class);
		assertThat(((ResolvedOneToManyRelation) countryRelation2).getTargetEntity().getEntityType()).isEqualTo(City.class);
	}

	/**
	 * Verifies that a mixed deep graph (one-to-many at depth 1, one-to-one at depth 2) is
	 * fully traversed: the algorithm must not stop after the first level.
	 *
	 * Country --[1:N]--> City --[1:1]--> State
	 */
	@Test
	void resolve_oneToManyThenOneToOne_deepRelationIsResolved() {
		FluentEntityMappingBuilder<State, Identifier<Long>> stateBuilder =
				entityBuilder(State.class, Identifier.LONG_TYPE)
						.mapKey(State::getId, ALREADY_ASSIGNED)
						.map(State::getName);

		FluentEntityMappingBuilder<City, Identifier<Long>> cityBuilder =
				entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, ALREADY_ASSIGNED)
						.map(City::getName)
						.mapOneToOne(City::getState, stateBuilder);

		FluentEntityMappingBuilder<Country, Identifier<Long>> countryBuilder =
				entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.mapOneToMany(Country::getCities, cityBuilder);

		// --- when ---
		Entity<Country, Identifier<Long>, ?> countryEntity =
				testInstance.resolve(countryBuilder.getConfiguration());

		// --- then ---
		// Depth 1: Country -> cities (one-to-many)
		Set<MappingJoin<?, ?, ?>> countryRelations = countryEntity.getRelations();
		assertThat(countryRelations).hasSize(1);
		MappingJoin<?, ?, ?> firstCountryRelation = countryRelations.iterator().next();
		assertThat(firstCountryRelation).isInstanceOf(ResolvedOneToManyRelation.class);

		// Depth 2: City -> state (one-to-one) — this is the critical assertion:
		// a naive (non-traversing) algorithm would miss this level
		ResolvedOneToManyRelation<?, ?, ?, ?, ?, ?, ?> countryToCityRelation =
				(ResolvedOneToManyRelation<?, ?, ?, ?, ?, ?, ?>) firstCountryRelation;
		Entity<?, ?, ?> cityEntity = countryToCityRelation.getTargetEntity();
		assertThat(cityEntity.getEntityType()).isEqualTo(City.class);
		Set<MappingJoin<?, ?, ?>> cityRelations = cityEntity.getRelations();
		assertThat(cityRelations).hasSize(1);
		MappingJoin<?, ?, ?> firstCityRelation = cityRelations.iterator().next();
		assertThat(firstCityRelation).isInstanceOf(ResolvedOneToOneRelation.class);
		assertThat(((ResolvedOneToOneRelation) firstCityRelation).getTargetEntity().getEntityType()).isEqualTo(State.class);
	}
	
	/**
	 * Verifies that relations defined on a superclass (ancestor entity, via joined-tables inheritance)
	 * are resolved by the traversal, even though they live on the ancestor entity and not on
	 * the child entity itself.
	 *
	 * Republic (child) --[inherits]--> Country (ancestor) --[1:1]--> Person (president)
	 *                                                      --[1:N]--> City  (cities)
	 */
	@Test
	void resolve_withAncestor_relationsDefinedOnAncestorAreResolved() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder =
				entityBuilder(Person.class, Identifier.LONG_TYPE)
						.mapKey(Person::getId, ALREADY_ASSIGNED)
						.map(Person::getName);

		FluentEntityMappingBuilder<City, Identifier<Long>> cityBuilder =
				entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, ALREADY_ASSIGNED)
						.map(City::getName);

		// Country superclass configuration carries both relations
		FluentEntityMappingBuilder<Country, Identifier<Long>> countryBuilder =
				entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.mapOneToOne(Country::getPresident, personBuilder)
						.mapOneToMany(Country::getCities, cityBuilder);

		// Republic extends Country via joined-tables: Country becomes an ancestor Entity
		FluentEntityMappingBuilder<Republic, Identifier<Long>> republicBuilder =
				entityBuilder(Republic.class, Identifier.LONG_TYPE)
						.mapSuperClass(countryBuilder)
						.joiningTables();

		// --- when ---
		Entity<Republic, Identifier<Long>, ?> republicEntity = testInstance.resolve(republicBuilder.getConfiguration());

		// --- then ---
		// The child entity (Republic) itself has no direct relations
		assertThat(republicEntity.getRelations()).isEmpty();

		// The ancestor entity (Country) is reachable through getParent()
		AncestorJoin<? super Republic, ?, ?, Identifier<Long>> parentJoin = republicEntity.getParent();
		Entity<? super Republic, Identifier<Long>, ?> ancestorCountryEntity = parentJoin.getAncestor();

		// The ancestor's relations (president + cities) must have been resolved by the traversal
		Set<MappingJoin<?, ?, ?>> ancestorRelations = ancestorCountryEntity.getRelations();
		assertThat(ancestorRelations).hasSize(2);
		MappingJoin<?, ?, ?> countryRelation1 = Iterables.asList(ancestorRelations).get(0);
		MappingJoin<?, ?, ?> countryRelation2 = Iterables.asList(ancestorRelations).get(1);
		assertThat(countryRelation1).isInstanceOf(ResolvedOneToOneRelation.class);
		assertThat(countryRelation2).isInstanceOf(ResolvedOneToManyRelation.class);
		assertThat(((ResolvedOneToOneRelation) countryRelation1).getTargetEntity().getEntityType()).isEqualTo(Person.class);
		assertThat(((ResolvedOneToManyRelation) countryRelation2).getTargetEntity().getEntityType()).isEqualTo(City.class);
	}
	
	/**
	 * Verifies that relations on the target entity of an ancestor's relation are also resolved.
	 * This exercises the combination of ancestor traversal and depth-first stacking:
	 * the City EntitySource produced by resolving Country's one-to-many must itself be
	 * pushed onto the stack so its own relations (state, persons) get resolved too.
	 *
	 * Republic --[inherits]--> Country (ancestor) --[1:N]--> City --[1:1]--> State
	 *                                                               --[1:N]--> Person
	 */
	@Test
	void resolve_withAncestor_relationsOnTargetOfAncestorRelationAreAlsoResolved() {
		FluentEntityMappingBuilder<State, Identifier<Long>> stateBuilder =
				entityBuilder(State.class, Identifier.LONG_TYPE)
						.mapKey(State::getId, ALREADY_ASSIGNED)
						.map(State::getName);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder =
				entityBuilder(Person.class, Identifier.LONG_TYPE)
						.mapKey(Person::getId, ALREADY_ASSIGNED)
						.map(Person::getName);
		
		// City carries two relations of its own
		FluentEntityMappingBuilder<City, Identifier<Long>> cityBuilder =
				entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, ALREADY_ASSIGNED)
						.map(City::getName)
						.mapOneToOne(City::getState, stateBuilder)
						.mapOneToMany(City::getPersons, personBuilder);
		
		// Country (future ancestor) has a one-to-many to City
		FluentEntityMappingBuilder<Country, Identifier<Long>> countryBuilder =
				entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.mapOneToMany(Country::getCities, cityBuilder);
		
		// Republic extends Country via joined-tables: Country becomes an ancestor Entity
		FluentEntityMappingBuilder<Republic, Identifier<Long>> republicBuilder =
				entityBuilder(Republic.class, Identifier.LONG_TYPE)
						.mapSuperClass(countryBuilder)
						.joiningTables();
		
		// --- when ---
		Entity<Republic, Identifier<Long>, ?> republicEntity =
				testInstance.resolve(republicBuilder.getConfiguration());
		
		// --- then ---
		// Republic itself has no direct relations
		assertThat(republicEntity.getRelations()).isEmpty();
		
		// Reach the ancestor Country entity
		AncestorJoin<? super Republic, ?, ?, Identifier<Long>> parentJoin =
				republicEntity.getParent();
		Entity<? super Republic, Identifier<Long>, ?> ancestorCountryEntity = parentJoin.getAncestor();
		
		// The ancestor's relation to City must be resolved
		Set<MappingJoin<?, ?, ?>> ancestorRelations = ancestorCountryEntity.getRelations();
		assertThat(ancestorRelations).hasSize(1);
		MappingJoin<?, ?, ?> firstAncestorRelation = first(ancestorRelations);
		assertThat(firstAncestorRelation).isInstanceOf(ResolvedOneToManyRelation.class);
		
		// Navigate to the City entity produced by resolving Country's one-to-many
		ResolvedOneToManyRelation<?, ?, ?, ?, ?, ?, ?> countryToCityRelation =
				(ResolvedOneToManyRelation<?, ?, ?, ?, ?, ?, ?>) firstAncestorRelation;
		Entity<?, ?, ?> cityEntity = countryToCityRelation.getTargetEntity();
		
		// City's own relations (state + persons) must also have been resolved by the traversal:
		// this is the key assertion — a naive algorithm that does not re-enqueue children
		// produced from ancestor sources would leave cityEntity.getRelations() empty.
		Set<MappingJoin<?, ?, ?>> cityRelations = cityEntity.getRelations();
		assertThat(cityRelations).hasSize(2);
		MappingJoin<?, ?, ?> cityRelation1 = Iterables.asList(cityRelations).get(0);
		MappingJoin<?, ?, ?> cityRelation2 = Iterables.asList(cityRelations).get(1);
		assertThat(cityRelation1).isInstanceOf(ResolvedOneToOneRelation.class);
		assertThat(cityRelation2).isInstanceOf(ResolvedOneToManyRelation.class);
		assertThat(((ResolvedOneToOneRelation) cityRelation1).getTargetEntity().getEntityType()).isEqualTo(State.class);
		assertThat(((ResolvedOneToManyRelation) cityRelation2).getTargetEntity().getEntityType()).isEqualTo(Person.class);
	}
	
	/**
	 * Verifies that a relation defined inside an inset (embedded object) on an ancestor entity
	 * is resolved by the traversal.
	 * The inset is a Person embedded in Country (ancestor of Republic). The Person embeddable
	 * carries a one-to-one relation to Vehicle, which must appear as a resolved relation on the
	 * ancestor Country entity.
	 *
	 * Republic --[inherits]--> Country (ancestor) --[embed Person]--> Person::getVehicle --[1:1]--> Vehicle
	 */
	@Test
	void resolve_withAncestor_relationInsideInsetOnAncestorIsResolved() {
		
		FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleBuilder = entityBuilder(Vehicle.class, Identifier.LONG_TYPE)
				.mapKey(Vehicle::getId, ALREADY_ASSIGNED);
		
		// Person embeddable has a one-to-one to City
		FluentEmbeddableMappingBuilder<Person> personBuilder = embeddableBuilder(Person.class)
				.map(Person::getName)
				.mapOneToOne(Person::getVehicle, vehicleBuilder);
		
		// Country (future ancestor) embeds Person
		FluentEntityMappingBuilder<Country, Identifier<Long>> countryBuilder =
				entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.embed(Country::getPresident, personBuilder);
		
		// Republic extends Country via joined-tables: Country becomes an ancestor Entity
		FluentEntityMappingBuilder<Republic, Identifier<Long>> republicBuilder =
				entityBuilder(Republic.class, Identifier.LONG_TYPE)
						.mapSuperClass(countryBuilder)
						.joiningTables();
		
		// --- when ---
		Entity<Republic, Identifier<Long>, ?> republicEntity =
				testInstance.resolve(republicBuilder.getConfiguration());
		
		// --- then ---
		// Republic itself has no direct relations
		assertThat(republicEntity.getRelations()).isEmpty();
		
		// Reach the ancestor Country entity
		AncestorJoin<? super Republic, ?, ?, Identifier<Long>> parentJoin = republicEntity.getParent();
		Entity<? super Republic, Identifier<Long>, ?> ancestorCountryEntity = parentJoin.getAncestor();
		
		// The relation defined inside the embedded Person inset must have been resolved on the ancestor:
		// this is the critical assertion — it exercises the inset-walking code path inside the
		// ancestor's EntitySource processing
		Set<MappingJoin<?, ?, ?>> ancestorRelations = ancestorCountryEntity.getRelations();
		assertThat(ancestorRelations).hasSize(1);
		MappingJoin<?, ?, ?> firstAncestorRelation = ancestorRelations.iterator().next();
		assertThat(firstAncestorRelation).isInstanceOf(ResolvedOneToOneRelation.class);
		
		assertThat(((ResolvedOneToOneRelation) firstAncestorRelation).getTargetEntity().getEntityType()).isEqualTo(Vehicle.class);
	}
}
