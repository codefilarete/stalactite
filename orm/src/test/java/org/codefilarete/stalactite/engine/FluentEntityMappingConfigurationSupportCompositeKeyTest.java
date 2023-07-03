package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.model.compositekey.House;
import org.codefilarete.stalactite.engine.model.compositekey.House.HouseId;
import org.codefilarete.stalactite.engine.model.compositekey.Person;
import org.codefilarete.stalactite.engine.model.compositekey.Person.PersonId;
import org.codefilarete.stalactite.engine.model.compositekey.Pet;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Iterables;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.MappingEase.subentityBuilder;

public class FluentEntityMappingConfigurationSupportCompositeKeyTest {
	
	private final HSQLDBDialect dialect = new HSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() throws SQLException {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
	}
	
	@Nested
	class ForeignKeyCreation {
		
		@Test
		void oneToOne_compositeToSingleKey_relationOwnedBySource() throws SQLException {
			MappingEase.entityBuilder(Person.class, PersonId.class)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress))
					.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Long.class)
							.mapKey(House::getId, IdentifierPolicy.afterInsert()))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(House.class)).getMapping().getTargetTable().getName().toUpperCase());
			Map<String, JdbcForeignKey> foreignKeyPerName = giveForeignKeys(exportedKeysForPersonTable);
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_PERSON_HOUSEID_HOUSE_ID", "PERSON", "HOUSEID", "HOUSE", "ID");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}

		@Test
		void oneToOne_compositeToSingleKey_relationOwnedByTarget() throws SQLException {
			MappingEase.entityBuilder(Person.class, PersonId.class)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress))
					.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Long.class)
							.mapKey(House::getId, IdentifierPolicy.afterInsert()))
					.mappedBy(House::getOwner)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(Person.class)).getMapping().getTargetTable().getName().toUpperCase());
			Map<String, JdbcForeignKey> foreignKeyPerName = giveForeignKeys(exportedKeysForPersonTable);
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_D2936B99", "HOUSE", "OWNERFIRSTNAME, OWNERLASTNAME, OWNERADDRESS", "PERSON", "FIRSTNAME, LASTNAME, ADDRESS");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void oneToOne_compositeToCompositeKey_relationOwnedBySource() throws SQLException {
			MappingEase.entityBuilder(Person.class, PersonId.class)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress))
					.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, HouseId.class)
							.mapCompositeKey(House::getHouseId, MappingEase.compositeKeyBuilder(HouseId.class)
									.map(HouseId::getNumber)
									.map(HouseId::getStreet)
									.map(HouseId::getZipCode)
									.map(HouseId::getCity)))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(House.class)).getMapping().getTargetTable().getName().toUpperCase());
			Map<String, JdbcForeignKey> foreignKeyPerName = giveForeignKeys(exportedKeysForPersonTable);
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_3D70E04A", "PERSON", "HOUSENUMBER, HOUSESTREET, HOUSEZIPCODE, HOUSECITY", "HOUSE", "NUMBER, STREET, ZIPCODE, CITY");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void oneToOne_compositeToCompositeKey_relationOwnedByTarget() throws SQLException {
			MappingEase.entityBuilder(Person.class, PersonId.class)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress))
					.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, HouseId.class)
							.mapCompositeKey(House::getHouseId, MappingEase.compositeKeyBuilder(HouseId.class)
									.map(HouseId::getNumber)
									.map(HouseId::getStreet)
									.map(HouseId::getZipCode)
									.map(HouseId::getCity)))
					.mappedBy(House::getOwner)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(Person.class)).getMapping().getTargetTable().getName().toUpperCase());
			Map<String, JdbcForeignKey> foreignKeyPerName = giveForeignKeys(exportedKeysForPersonTable);
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_D2936B99", "HOUSE", "OWNERFIRSTNAME, OWNERLASTNAME, OWNERADDRESS", "PERSON", "FIRSTNAME, LASTNAME, ADDRESS");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void oneToMany_compositeToCompositeKey_withAssociationTable() throws SQLException {
			EntityPersister<Person, PersonId> personPersister =  MappingEase.entityBuilder(Person.class, PersonId.class)
					.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress))
					.map(Person::getAge)
					.mapOneToManySet(Person::getPets, MappingEase.entityBuilder(Pet.class, Pet.PetId.class)
							.mapCompositeKey(Pet::getId, MappingEase.compositeKeyBuilder(Pet.PetId.class)
									.map(Pet.PetId::getName)
									.map(Pet.PetId::getRace)
									.map(Pet.PetId::getAge)))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(Person.class)).getMapping().getTargetTable().getName().toUpperCase());
			Map<String, JdbcForeignKey> foreignKeyPerName =  giveForeignKeys(exportedKeysForPersonTable);
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_D2936B99", "PERSON_PETS", "PERSON_FIRSTNAME, PERSON_LASTNAME, PERSON_ADDRESS", "PERSON", "FIRSTNAME, LASTNAME, ADDRESS");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
			
			ResultSet exportedKeysForPetTable = currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(Pet.class)).getMapping().getTargetTable().getName().toUpperCase());
			foreignKeyPerName =  giveForeignKeys(exportedKeysForPetTable);
			foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			expectedForeignKey = new JdbcForeignKey("FK_BF8CB44", "PERSON_PETS", "PETS_NAME, PETS_RACE, PETS_AGE", "PET", "NAME, RACE, AGE");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		private Map<String, JdbcForeignKey> giveForeignKeys(ResultSet exportedKeysForPersonTable) {
			Map<String, JdbcForeignKey> result = new HashMap<>();
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(exportedKeysForPersonTable) {
				@Override
				public JdbcForeignKey convert(ResultSet rs) throws SQLException {
					String fkName = rs.getString("FK_NAME");
					String fktableName = rs.getString("FKTABLE_NAME");
					String fkcolumnName = rs.getString("FKCOLUMN_NAME");
					String pktableName = rs.getString("PKTABLE_NAME");
					String pkcolumnName = rs.getString("PKCOLUMN_NAME");
					return new JdbcForeignKey(
							fkName,
							fktableName, fkcolumnName,
							pktableName, pkcolumnName);
				}
			};
			fkPersonIterator.forEachRemaining(jdbcForeignKey -> {
				String fkName = jdbcForeignKey.getName();
				String fktableName = jdbcForeignKey.getSrcTableName();
				String fkcolumnName = jdbcForeignKey.getSrcColumnName();
				String pktableName = jdbcForeignKey.getTargetTableName();
				String pkcolumnName = jdbcForeignKey.getTargetColumnName();
				result.compute(fkName, (k, fk) -> fk == null
						? new JdbcForeignKey(fkName, fktableName, fkcolumnName, pktableName, pkcolumnName)
						: new JdbcForeignKey(fkName, fktableName, fk.getSrcColumnName() + ", " + fkcolumnName, pktableName, fk.getTargetColumnName() + ", " + pkcolumnName));
			});
			return result;
		}
	}
	
	@Test
	void crud_oneToOne_compositeToSingleKey_ownedBySource() {
		EntityPersister<Person, PersonId> personPersister =  MappingEase.entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress))
				.map(Person::getAge)
				.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Long.class)
						.mapKey(House::getId, IdentifierPolicy.afterInsert()))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.setHouse(new House());
		dummyPerson.setAge(35);
		
		personPersister.insert(dummyPerson);
		
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		assertThat(loadedPerson.getHouse().getId()).isEqualTo(1);
		
		dummyPerson.setAge(36);
		personPersister.update(dummyPerson, loadedPerson, true);
		Table personTable = new Table("Person");
		Column<Table, Integer> age = personTable.addColumn("age", int.class);
		Set<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).isEmpty();
	}

	@Test
	void crud_oneToOne_compositeToSingleKey_ownedByTarget() {
		EntityPersister<Person, PersonId> personPersister =  MappingEase.entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress))
				.map(Person::getAge)
				.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, Long.class)
						.mapKey(House::getId, IdentifierPolicy.afterInsert()))
				.mappedBy(House::getOwner)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.setHouse(new House());
		dummyPerson.setAge(35);
		
		personPersister.insert(dummyPerson);
		
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		assertThat(loadedPerson.getHouse().getId()).isEqualTo(1);
		
		dummyPerson.setAge(36);
		personPersister.update(dummyPerson, loadedPerson, true);
		Table personTable = new Table("Person");
		Column<Table, Integer> age = personTable.addColumn("age", int.class);
		Set<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).isEmpty();
	}
	
	@Test
	void crud_oneToOne_compositeToCompositeKey_ownedBySource() {
		EntityPersister<Person, PersonId> personPersister =  MappingEase.entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress))
				.map(Person::getAge)
				.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, HouseId.class)
						.mapCompositeKey(House::getHouseId, MappingEase.compositeKeyBuilder(HouseId.class)
								.map(HouseId::getNumber)
								.map(HouseId::getStreet)
								.map(HouseId::getZipCode)
								.map(HouseId::getCity))
						)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.setHouse(new House(new HouseId(42, "Stalactite street", "888", "CodeFilarete City")));
		dummyPerson.setAge(35);
		
		personPersister.insert(dummyPerson);
		
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		assertThat(loadedPerson.getHouse().getHouseId())
				.usingRecursiveComparison()
				.isEqualTo(new HouseId(42, "Stalactite street", "888", "CodeFilarete City"));
		
		dummyPerson.setAge(36);
		personPersister.update(dummyPerson, loadedPerson, true);
		Table personTable = new Table("Person");
		Column<Table, Integer> age = personTable.addColumn("age", int.class);
		Set<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).isEmpty();
	}
	
	@Test
	void crud_oneToOne_compositeToCompositeKey_ownedByTarget() {
		EntityPersister<Person, PersonId> personPersister =  MappingEase.entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress))
				.map(Person::getAge)
				.mapOneToOne(Person::getHouse, MappingEase.entityBuilder(House.class, HouseId.class)
						.mapCompositeKey(House::getHouseId, MappingEase.compositeKeyBuilder(HouseId.class)
								.map(HouseId::getNumber)
								.map(HouseId::getStreet)
								.map(HouseId::getZipCode)
								.map(HouseId::getCity))
				)
				.mappedBy(House::getOwner)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.setHouse(new House(new HouseId(42, "Stalactite street", "888", "CodeFilarete City")));
		dummyPerson.setAge(35);
		
		personPersister.insert(dummyPerson);
		
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		assertThat(loadedPerson.getHouse().getHouseId())
				.usingRecursiveComparison()
				.isEqualTo(new HouseId(42, "Stalactite street", "888", "CodeFilarete City"));
		
		dummyPerson.setAge(36);
		personPersister.update(dummyPerson, loadedPerson, true);
		Table personTable = new Table("Person");
		Column<Table, Integer> age = personTable.addColumn("age", int.class);
		Set<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).isEmpty();
	}
	
	@Test
	void crud_oneToMany_ownedByTarget() {
		EntityPersister<Person, PersonId> personPersister =  MappingEase.entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress))
				.map(Person::getAge)
				.mapOneToManySet(Person::getPets, MappingEase.entityBuilder(Pet.class, Pet.PetId.class)
						.mapCompositeKey(Pet::getId, MappingEase.compositeKeyBuilder(Pet.PetId.class)
								.map(Pet.PetId::getName)
								.map(Pet.PetId::getRace)
								.map(Pet.PetId::getAge)))
				.mappedBy(Pet::getOwner)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.addPet(new Pet(new Pet.PetId("Pluto", "Dog", 4)));
		dummyPerson.addPet(new Pet(new Pet.PetId("Rantanplan", "Dog", 5)));
		dummyPerson.setAge(35);
		
		personPersister.insert(dummyPerson);
		
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		assertThat(loadedPerson.getPets())
				.usingRecursiveFieldByFieldElementComparatorIgnoringFields("owner")
				.containsExactlyInAnyOrderElementsOf(dummyPerson.getPets());
		Set<PersonId> personIds = Iterables.collect(loadedPerson.getPets(), pet -> pet.getOwner().getId(), HashSet::new);
		assertThat(personIds).containsExactly(dummyPerson.getId());
		
		dummyPerson.setAge(36);
		personPersister.update(dummyPerson, loadedPerson, true);
		Table personTable = new Table("Person");
		Column<Table, Integer> age = personTable.addColumn("age", int.class);
		Set<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		Column<Table, String> firstNameColumn = personTable.addColumn("firstName", String.class);
		Column<Table, String> lastNameColumn = personTable.addColumn("lastName", String.class);
		Column<Table, String> addressColumn = personTable.addColumn("address", String.class);
		Set<PersonId> persons = persistenceContext.select(PersonId::new, firstNameColumn, lastNameColumn, addressColumn);
		assertThat(persons).isEmpty();
	}
	
	@Test
	void crud_oneToMany_withAssociationTable() {
		EntityPersister<Person, PersonId> personPersister =  MappingEase.entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress))
				.map(Person::getAge)
				.mapOneToManySet(Person::getPets, MappingEase.entityBuilder(Pet.class, Pet.PetId.class)
						.mapCompositeKey(Pet::getId, MappingEase.compositeKeyBuilder(Pet.PetId.class)
								.map(Pet.PetId::getName)
								.map(Pet.PetId::getRace)
								.map(Pet.PetId::getAge)))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.addPet(new Pet(new Pet.PetId("Pluto", "Dog", 4)));
		dummyPerson.addPet(new Pet(new Pet.PetId("Rantanplan", "Dog", 5)));
		dummyPerson.setAge(35);
		
		personPersister.insert(dummyPerson);
		
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		assertThat(loadedPerson.getPets())
				.usingRecursiveFieldByFieldElementComparatorIgnoringFields("owner")
				.containsExactlyInAnyOrderElementsOf(dummyPerson.getPets());
		
		dummyPerson.setAge(36);
		personPersister.update(dummyPerson, loadedPerson, true);
		Table personTable = new Table("Person");
		Column<Table, Integer> age = personTable.addColumn("age", int.class);
		Set<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		Column<Table, String> firstNameColumn = personTable.addColumn("firstName", String.class);
		Column<Table, String> lastNameColumn = personTable.addColumn("lastName", String.class);
		Column<Table, String> addressColumn = personTable.addColumn("address", String.class);
		Set<PersonId> persons = persistenceContext.select(PersonId::new, firstNameColumn, lastNameColumn, addressColumn);
		assertThat(persons).isEmpty();
	}
	
	@Test
	void persist_oneToMany_ownedByTarget() {
		EntityPersister<Person, PersonId> personPersister =  MappingEase.entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress))
				.map(Person::getAge)
				.mapOneToManySet(Person::getPets, MappingEase.entityBuilder(Pet.class, Pet.PetId.class)
						.mapCompositeKey(Pet::getId, MappingEase.compositeKeyBuilder(Pet.PetId.class)
								.map(Pet.PetId::getName)
								.map(Pet.PetId::getRace)
								.map(Pet.PetId::getAge)))
				.mappedBy(Pet::getOwner)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.addPet(new Pet(new Pet.PetId("Pluto", "Dog", 4)));
		dummyPerson.addPet(new Pet(new Pet.PetId("Rantanplan", "Dog", 5)));
		dummyPerson.setAge(35);
		
		personPersister.persist(dummyPerson);
		
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		assertThat(loadedPerson.getPets())
				// we don't take "owner" into account because Person doesn't implement equals/hashcode
				.usingRecursiveFieldByFieldElementComparatorIgnoringFields("owner")
				.containsExactlyInAnyOrderElementsOf(dummyPerson.getPets());
		Set<PersonId> personIds = Iterables.collect(loadedPerson.getPets(), pet -> pet.getOwner().getId(), HashSet::new);
		assertThat(personIds).containsExactly(dummyPerson.getId());
		
		// changing value to check for database update
		loadedPerson.setAge(36);
		loadedPerson.addPet(new Pet(new Pet.PetId("Schrodinger", "Cat", -42)));
		loadedPerson.removePet(new Pet.PetId("Rantanplan", "Dog", 5));
		personPersister.persist(loadedPerson);
		
		loadedPerson = personPersister.select(dummyPerson.getId());
		assertThat(loadedPerson.getAge()).isEqualTo(36);
		assertThat(loadedPerson.getPets())
				// we don't take "owner" into account because Person doesn't implement equals/hashcode
				.usingRecursiveFieldByFieldElementComparatorIgnoringFields("owner")
				.containsExactlyInAnyOrder(
				new Pet(new Pet.PetId("Pluto", "Dog", 4)),
				new Pet(new Pet.PetId("Schrodinger", "Cat", -42)));
	}
	
	@Test
	void crud_manyToMany() {
		EntityPersister<Person, PersonId> personPersister =  MappingEase.entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress))
				.map(Person::getAge)
				.mapOneToManySet(Person::getPets, MappingEase.entityBuilder(Pet.class, Pet.PetId.class)
						.mapCompositeKey(Pet::getId, MappingEase.compositeKeyBuilder(Pet.PetId.class)
								.map(Pet.PetId::getName)
								.map(Pet.PetId::getRace)
								.map(Pet.PetId::getAge)))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.addPet(new Pet(new Pet.PetId("Pluto", "Dog", 4)));
		dummyPerson.addPet(new Pet(new Pet.PetId("Rantanplan", "Dog", 5)));
		dummyPerson.setAge(35);
		
		personPersister.insert(dummyPerson);
		
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		assertThat(loadedPerson.getPets())
				.usingRecursiveFieldByFieldElementComparatorIgnoringFields()
				.containsExactlyInAnyOrderElementsOf(dummyPerson.getPets());
		
		dummyPerson.setAge(36);
		personPersister.update(dummyPerson, loadedPerson, true);
		Table personTable = new Table("Person");
		Column<Table, Integer> age = personTable.addColumn("age", int.class);
		Set<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		Column<Table, String> firstNameColumn = personTable.addColumn("firstName", String.class);
		Column<Table, String> lastNameColumn = personTable.addColumn("lastName", String.class);
		Column<Table, String> addressColumn = personTable.addColumn("address", String.class);
		Set<PersonId> persons = persistenceContext.select(PersonId::new, firstNameColumn, lastNameColumn, addressColumn);
		assertThat(persons).isEmpty();
	}
	
	@Test
	void persist_manyToMany() {
		EntityPersister<Person, PersonId> personPersister =  MappingEase.entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, MappingEase.compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress))
				.map(Person::getAge)
				.mapManyToManySet(Person::getPets, MappingEase.entityBuilder(Pet.class, Pet.PetId.class)
						.mapCompositeKey(Pet::getId, MappingEase.compositeKeyBuilder(Pet.PetId.class)
								.map(Pet.PetId::getName)
								.map(Pet.PetId::getRace)
								.map(Pet.PetId::getAge)))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.addPet(new Pet(new Pet.PetId("Pluto", "Dog", 4)));
		dummyPerson.addPet(new Pet(new Pet.PetId("Rantanplan", "Dog", 5)));
		dummyPerson.setAge(35);
		
		personPersister.persist(dummyPerson);
		
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		assertThat(loadedPerson.getPets())
				.usingRecursiveFieldByFieldElementComparator()
				.containsExactlyInAnyOrderElementsOf(dummyPerson.getPets());
		
		// changing value to check for database update
		loadedPerson.setAge(36);
		loadedPerson.addPet(new Pet(new Pet.PetId("Schrodinger", "Cat", -42)));
		loadedPerson.removePet(new Pet.PetId("Rantanplan", "Dog", 5));
		personPersister.persist(loadedPerson);
		
		loadedPerson = personPersister.select(dummyPerson.getId());
		assertThat(loadedPerson.getAge()).isEqualTo(36);
		assertThat(loadedPerson.getPets())
				// we don't take "owner" into account because Person doesn't implement equals/hashcode
				.usingRecursiveFieldByFieldElementComparatorIgnoringFields("owner")
				.containsExactlyInAnyOrder(
				new Pet(new Pet.PetId("Pluto", "Dog", 4)),
				new Pet(new Pet.PetId("Schrodinger", "Cat", -42)));
	}
	
	@Test
	void crud_inheritance_joinedTables() throws SQLException {
		EntityPersister<Pet, Pet.PetId> petPersister = MappingEase.entityBuilder(Pet.class, Pet.PetId.class)
				.mapCompositeKey(Pet::getId, MappingEase.compositeKeyBuilder(Pet.PetId.class)
						.map(Pet.PetId::getName)
						.map(Pet.PetId::getRace)
						.map(Pet.PetId::getAge))
				.mapPolymorphism(PolymorphismPolicy.joinTable(Pet.class)
						.addSubClass(subentityBuilder(Pet.Cat.class)
								.mapEnum(Pet.Cat::getCatBreed))
				)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Pet.Cat cat = new Pet.Cat(new Pet.PetId("Pluto", "Dog", 4));
		cat.setCatBreed(Pet.CatBreed.Persian);
		petPersister.insert(cat);
		
		Pet loadedPet = petPersister.select(cat.getId());
		assertThat(loadedPet)
				.usingRecursiveComparison()
				.isEqualTo(cat);
		
		cat.setCatBreed(Pet.CatBreed.Persian);
		petPersister.update(cat, loadedPet, true);
		loadedPet = petPersister.select(cat.getId());
		assertThat(loadedPet)
				.usingRecursiveComparison()
				.isEqualTo(cat);
		
		
		persistenceContext.getConnectionProvider().giveConnection().commit();
		petPersister.delete(cat);
		String petName = persistenceContext.newQuery("select name from Pet", String.class).mapKey("name", String.class).singleResult().execute();
		assertThat(petName).isNull();
		String catBreed = persistenceContext.newQuery("select catBreed from Cat", String.class).mapKey("name", String.class).singleResult().execute();
		assertThat(catBreed).isNull();
		persistenceContext.getConnectionProvider().giveConnection().rollback();
		
		// we check deleteById to ensure that it takes composite key into account
		petPersister.deleteById(cat);
		petName = persistenceContext.newQuery("select name from Pet", String.class).mapKey("name", String.class).singleResult().execute();
		assertThat(petName).isNull();
		catBreed = persistenceContext.newQuery("select catBreed from Cat", String.class).mapKey("name", String.class).singleResult().execute();
		assertThat(catBreed).isNull();
	}
}
