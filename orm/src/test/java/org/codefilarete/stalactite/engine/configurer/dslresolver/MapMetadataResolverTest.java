package org.codefilarete.stalactite.engine.configurer.dslresolver;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.MappingJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation.CompositeMemberMapping;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.compositekey.House;
import org.codefilarete.stalactite.engine.model.compositekey.House.HouseId;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.compositeKeyBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.tool.collection.Iterables.first;
import static org.mockito.Mockito.mock;

class MapMetadataResolverTest {

	@Test
	void resolve_defaultTableAndColumns() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> mappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapMap(Person::getPhoneNumbers, String.class, String.class);

		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Person, Identifier<Long>, ?> personEntity = testInstance.resolve(mappingBuilder.getConfiguration());

		assertThat(personEntity.getRelations()).hasSize(1);
		MappingJoin<?, ?, ?> relation = first(personEntity.getRelations());
		assertThat(relation).isInstanceOf(ResolvedMapRelation.class);

		ResolvedMapRelation<Person, ?, String, ?, String, ?, ?, ?, ?, ?, ?> mapRelation =
				(ResolvedMapRelation<Person, ?, String, ?, String, ?, ?, ?, ?, ?, ?>) relation;
		Table<?> mapTable = mapRelation.getJoin().getRightKey().getTable();
		assertThat(mapTable.getName()).isEqualTo("Person_phoneNumbers");
		assertThat(mapTable.getColumn("id")).isNotNull();
		assertThat(mapTable.getColumn("key")).isNotNull();
		assertThat(mapTable.getColumn("value")).isNotNull();
	}

	@Test
	void resolve_customTableAndColumnNames() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> mappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
						.onTable("Toto")
						.reverseJoinColumn("person_fk")
						.keyColumn("entry_key")
						.valueColumn("entry_value");

		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Person, Identifier<Long>, ?> personEntity = testInstance.resolve(mappingBuilder.getConfiguration());

		ResolvedMapRelation<Person, ?, String, ?, String, ?, ?, ?, ?, ?, ?> relation =
				(ResolvedMapRelation<Person, ?, String, ?, String, ?, ?, ?, ?, ?, ?>) first(personEntity.getRelations());
		Table<?> mapTable = relation.getJoin().getRightKey().getTable();
		assertThat(mapTable.getName()).isEqualTo("Toto");
		assertThat(mapTable.getColumn("person_fk")).isNotNull();
		assertThat(mapTable.getColumn("entry_key")).isNotNull();
		assertThat(mapTable.getColumn("entry_value")).isNotNull();
	}

	@Test
	void resolve_relationFixerInitializesAndPopulatesMap() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> mappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapMap(Person::getPhoneNumbers, String.class, String.class);

		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Person, Identifier<Long>, ?> personEntity = testInstance.resolve(mappingBuilder.getConfiguration());
		ResolvedMapRelation<Person, Identifier<Long>, String, ?, String, ?, ?, ?, ?, ?, ?> relation =
				(ResolvedMapRelation<Person, Identifier<Long>, String, ?, String, ?, ?, ?, ?, ?, ?>) first(personEntity.getRelations());

		Person person = new Person(new PersistableIdentifier<>(1L));
		KeyValueRecord<String, String, Identifier<Long>> firstRecord = new KeyValueRecord<>(person.getId(), "home", "01 11 11 11 11");
		KeyValueRecord<String, String, Identifier<Long>> secondRecord = new KeyValueRecord<>(person.getId(), "work", "02 22 22 22 22");
		relation.getRelationFixer().apply(person, firstRecord);
		relation.getRelationFixer().apply(person, secondRecord);

		assertThat(person.getPhoneNumbers()).containsEntry("home", "01 11 11 11 11");
		assertThat(person.getPhoneNumbers()).containsEntry("work", "02 22 22 22 22");
	}

	@Test
	void resolve_fetchSeparatelyFlagIsTransferred() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> mappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
						.fetchSeparately();

		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Person, Identifier<Long>, ?> personEntity = testInstance.resolve(mappingBuilder.getConfiguration());
		ResolvedMapRelation<Person, ?, String, ?, String, ?, ?, ?, ?, ?, ?> relation =
				(ResolvedMapRelation<Person, ?, String, ?, String, ?, ?, ?, ?, ?, ?>) first(personEntity.getRelations());

		assertThat(relation.isFetchSeparately()).isTrue();
	}

	@Test
	void resolve_entityAsKey_addsEntityForeignKey() {
		FluentEntityMappingBuilder<Country, Identifier<Long>> countryBuilder = entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, ALREADY_ASSIGNED)
				.map(Country::getName);

		FluentEntityMappingBuilder<Person, Identifier<Long>> mappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapMap(Person::getMapPropertyMadeOfEntityAsKey, Country.class, String.class)
				.withKeyMapping(countryBuilder);

		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Person, Identifier<Long>, ?> personEntity = testInstance.resolve(mappingBuilder.getConfiguration());
		ResolvedMapRelation<Person, ?, Country, ?, String, ?, ?, ?, ?, ?, ?> relation =
				(ResolvedMapRelation<Person, ?, Country, ?, String, ?, ?, ?, ?, ?, ?>) first(personEntity.getRelations());

		Table<?> mapTable = relation.getJoin().getRightKey().getTable();
		assertThat(mapTable.getColumn("keyId")).isNotNull();
		assertThat(mapTable.getForeignKeys())
				.extracting(foreignKey -> foreignKey.getTargetTable().getName())
				.contains("Person", "Country");
	}

	@Test
	void resolve_entityAsValue_addsEntityForeignKey() {
		FluentEntityMappingBuilder<Country, Identifier<Long>> countryBuilder = entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, ALREADY_ASSIGNED)
				.map(Country::getName);

		FluentEntityMappingBuilder<Person, Identifier<Long>> mappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapMap(Person::getMapPropertyMadeOfEntityAsValue, String.class, Country.class)
				.withValueMapping(countryBuilder);

		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Person, Identifier<Long>, ?> personEntity = testInstance.resolve(mappingBuilder.getConfiguration());
		ResolvedMapRelation<Person, ?, String, ?, Country, ?, ?, ?, ?, ?, ?> relation =
				(ResolvedMapRelation<Person, ?, String, ?, Country, ?, ?, ?, ?, ?, ?>) first(personEntity.getRelations());

		Table<?> mapTable = relation.getJoin().getRightKey().getTable();
		assertThat(mapTable.getColumn("valueId")).isNotNull();
		assertThat(mapTable.getForeignKeys())
				.extracting(foreignKey -> foreignKey.getTargetTable().getName())
				.contains("Person", "Country");
	}

	@Test
	void resolve_entityAsCompositeKey_addsCompositeForeignKeyColumns() {
		FluentEntityMappingBuilder<House, HouseId> houseBuilder = entityBuilder(House.class, HouseId.class)
				.mapKey(House::getHouseId, compositeKeyBuilder(HouseId.class)
						.map(HouseId::getNumber)
						.map(HouseId::getStreet)
						.map(HouseId::getZipCode)
						.map(HouseId::getCity),
					h -> {},
					h -> true);

		FluentEntityMappingBuilder<Person, Identifier<Long>> mappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapMap(Person::getMapPropertyMadeOfCompositeIdEntityAsKey, House.class, String.class)
				.withKeyMapping(houseBuilder);

		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Person, Identifier<Long>, ?> personEntity = testInstance.resolve(mappingBuilder.getConfiguration());
		ResolvedMapRelation<Person, ?, House, ?, String, ?, ?, ?, ?, ?, ?> relation =
				(ResolvedMapRelation<Person, ?, House, ?, String, ?, ?, ?, ?, ?, ?>) first(personEntity.getRelations());

		Table<?> mapTable = relation.getJoin().getRightKey().getTable();
		assertThat(mapTable.getColumn("number")).isNotNull();
		assertThat(mapTable.getColumn("street")).isNotNull();
		assertThat(mapTable.getColumn("zipCode")).isNotNull();
		assertThat(mapTable.getColumn("city")).isNotNull();
		assertThat(relation.getKeyEntityDefinition().getForeignKey().getColumns()).extracting(Column::getName)
				.containsExactly("number", "street", "zipCode", "city");
		CompositeMemberMapping<HouseId, ?> keyEntityIdentifierMapping = (CompositeMemberMapping) relation.<HouseId>getKeyMapping();
		assertThat(keyEntityIdentifierMapping.getMapping().keySet())
				.extracting(AccessorDefinition::giveDefinition)
				.extracting(AccessorDefinition::getName)
				.containsExactlyInAnyOrder("number", "street", "zipCode", "city");
		assertThat(mapTable.getPrimaryKey().getColumns()).extracting(Column::getName)
				.contains("id", "number", "street", "zipCode", "city");
		assertThat(mapTable.getForeignKeys()).hasSize(2);
		assertThat(mapTable.getForeignKeys())
				.anySatisfy(foreignKey -> {
					assertThat(foreignKey.getTargetTable().getName()).isEqualTo("Person");
					assertThat(foreignKey.getColumns()).extracting(Column::getName).containsExactly("id");
				})
				.anySatisfy(foreignKey -> {
					assertThat(foreignKey.getTargetTable().getName()).isEqualTo("House");
					assertThat(foreignKey.getColumns()).extracting(Column::getName)
							.containsExactly("number", "street", "zipCode", "city");
				});
	}

	@Test
	void resolve_entityAsCompositeValue_addsCompositeForeignKeyColumns() {
		FluentEntityMappingBuilder<House, HouseId> houseBuilder = entityBuilder(House.class, HouseId.class)
				.mapKey(House::getHouseId, compositeKeyBuilder(HouseId.class)
						.map(HouseId::getNumber)
						.map(HouseId::getStreet)
						.map(HouseId::getZipCode)
						.map(HouseId::getCity),
					h -> {
					},
					h -> true);

		FluentEntityMappingBuilder<Person, Identifier<Long>> mappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapMap(Person::getMapPropertyMadeOfCompositeIdEntityAsValue, String.class, House.class)
				.withValueMapping(houseBuilder);

		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Person, Identifier<Long>, ?> personEntity = testInstance.resolve(mappingBuilder.getConfiguration());
		ResolvedMapRelation<Person, ?, String, ?, House, ?, ?, ?, ?, ?, ?> relation =
				(ResolvedMapRelation<Person, ?, String, ?, House, ?, ?, ?, ?, ?, ?>) first(personEntity.getRelations());

		Table<?> mapTable = relation.getJoin().getRightKey().getTable();
		assertThat(mapTable.getColumn("number")).isNotNull();
		assertThat(mapTable.getColumn("street")).isNotNull();
		assertThat(mapTable.getColumn("zipCode")).isNotNull();
		assertThat(mapTable.getColumn("city")).isNotNull();
		assertThat(relation.getValueEntityDefinition().getForeignKey().getColumns()).extracting(Column::getName)
				.contains("number", "street", "zipCode", "city");
		CompositeMemberMapping<HouseId, ?> valueEntityIdentifierMapping = (CompositeMemberMapping) relation.<HouseId>getValueMapping();
		assertThat(valueEntityIdentifierMapping.getMapping().keySet())
				.extracting(AccessorDefinition::giveDefinition)
				.extracting(AccessorDefinition::getName)
				.containsExactlyInAnyOrder("number", "street", "zipCode", "city");
		assertThat(mapTable.getPrimaryKey().getColumns()).extracting(Column::getName)
				.contains("id", "key");
		assertThat(mapTable.getForeignKeys()).hasSize(2);
		assertThat(mapTable.getForeignKeys())
				.anySatisfy(foreignKey -> {
					assertThat(foreignKey.getTargetTable().getName()).isEqualTo("Person");
					assertThat(foreignKey.getColumns()).extracting(Column::getName).containsExactly("id");
				})
				.anySatisfy(foreignKey -> {
					assertThat(foreignKey.getTargetTable().getName()).isEqualTo("House");
					assertThat(foreignKey.getColumns()).extracting(Column::getName)
							.containsExactly("number", "street", "zipCode", "city");
				});
	}
}
