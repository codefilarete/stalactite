package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.model.compositekey.House;
import org.codefilarete.stalactite.engine.model.compositekey.House.HouseId;
import org.codefilarete.stalactite.engine.model.compositekey.Person;
import org.codefilarete.stalactite.engine.model.compositekey.Person.PersonId;
import org.codefilarete.stalactite.engine.model.compositekey.Pet;
import org.codefilarete.stalactite.engine.model.compositekey.Pet.Cat;
import org.codefilarete.stalactite.engine.model.compositekey.Pet.CatBreed;
import org.codefilarete.stalactite.engine.model.compositekey.Pet.Dog;
import org.codefilarete.stalactite.engine.model.compositekey.Pet.PetId;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Iterables;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.MappingEase.compositeKeyBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.subentityBuilder;

public class FluentEntityMappingConfigurationSupportCompositeKeyTest {
	
	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	
	private final Set<PersonId> persistedPersons = new HashSet<>();
	private final Set<PetId> persistedPets = new HashSet<>();
	private final Set<HouseId> persistedHouses = new HashSet<>();
	
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() throws SQLException {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
		clearPersistedStatuses();
	}
	
	public void clearPersistedStatuses() {
		persistedPersons.clear();
		persistedPets.clear();
		persistedHouses.clear();
	}
	
	@Nested
	class ForeignKeyCreation {
		
		@Test
		void oneToOne_compositeToSingleKey_relationOwnedBySource() throws SQLException {
			entityBuilder(Person.class, PersonId.class)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
					.mapOneToOne(Person::getHouse, entityBuilder(House.class, Long.class)
							.mapKey(House::getId, IdentifierPolicy.databaseAutoIncrement()))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null, "HOUSE");
			Map<String, JdbcForeignKey> foreignKeyPerName = giveForeignKeys(exportedKeysForPersonTable);
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_PERSON_HOUSEID_HOUSE_ID", "PERSON", "HOUSEID", "HOUSE", "ID");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}

		@Test
		void oneToOne_compositeToSingleKey_relationOwnedByTarget() throws SQLException {
			entityBuilder(Person.class, PersonId.class)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
					.mapOneToOne(Person::getHouse, entityBuilder(House.class, Long.class)
							.mapKey(House::getId, IdentifierPolicy.databaseAutoIncrement()))
					.mappedBy(House::getOwner)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(Person.class)).getMapping().getTargetTable().getName().toUpperCase());
			Map<String, JdbcForeignKey> foreignKeyPerName = giveForeignKeys(exportedKeysForPersonTable);
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_C7E4C03A", "HOUSE", "OWNERFIRSTNAME, OWNERLASTNAME, OWNERADDRESS", "PERSON", "FIRSTNAME, LASTNAME, ADDRESS");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void oneToOne_compositeToCompositeKey_relationOwnedBySource() throws SQLException {
			entityBuilder(Person.class, PersonId.class)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
					.mapOneToOne(Person::getHouse, entityBuilder(House.class, HouseId.class)
							.mapCompositeKey(House::getHouseId, compositeKeyBuilder(HouseId.class)
									.map(HouseId::getNumber)
									.map(HouseId::getStreet)
									.map(HouseId::getZipCode)
									.map(HouseId::getCity), h -> persistedHouses.add(h.getHouseId()), h -> persistedHouses.contains(h.getHouseId())))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null, "HOUSE");
			Map<String, JdbcForeignKey> foreignKeyPerName = giveForeignKeys(exportedKeysForPersonTable);
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_DC808DBE", "PERSON", "HOUSENUMBER, HOUSESTREET, HOUSEZIPCODE, HOUSECITY", "HOUSE", "NUMBER, STREET, ZIPCODE, CITY");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void oneToOne_compositeToCompositeKey_relationOwnedByTarget() throws SQLException {
			entityBuilder(Person.class, PersonId.class)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
					.mapOneToOne(Person::getHouse, entityBuilder(House.class, HouseId.class)
							.mapCompositeKey(House::getHouseId, compositeKeyBuilder(HouseId.class)
									.map(HouseId::getNumber)
									.map(HouseId::getStreet)
									.map(HouseId::getZipCode)
									.map(HouseId::getCity), h -> persistedHouses.add(h.getHouseId()), h -> persistedHouses.contains(h.getHouseId())))
					.mappedBy(House::getOwner)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(Person.class)).getMapping().getTargetTable().getName().toUpperCase());
			Map<String, JdbcForeignKey> foreignKeyPerName = giveForeignKeys(exportedKeysForPersonTable);
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_C7E4C03A", "HOUSE", "OWNERFIRSTNAME, OWNERLASTNAME, OWNERADDRESS", "PERSON", "FIRSTNAME, LASTNAME, ADDRESS");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void oneToMany_compositeToCompositeKey_withAssociationTable() throws SQLException {
			EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
					.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
					.map(Person::getAge)
					.mapOneToMany(Person::getPets, entityBuilder(Pet.class, PetId.class)
							.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
									.map(PetId::getName)
									.map(PetId::getRace)
									.map(PetId::getAge), p -> persistedPets.add(p.getId()), p -> persistedPets.contains(p.getId())))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(Person.class)).getMapping().getTargetTable().getName().toUpperCase());
			Map<String, JdbcForeignKey> foreignKeyPerName =  giveForeignKeys(exportedKeysForPersonTable);
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_D6E530BC", "PERSON_PETS", "PERSON_FIRSTNAME, PERSON_LASTNAME, PERSON_ADDRESS", "PERSON", "FIRSTNAME, LASTNAME, ADDRESS");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
			
			ResultSet exportedKeysForPetTable = currentConnection.getMetaData().getExportedKeys(null, null, "PET");
			foreignKeyPerName =  giveForeignKeys(exportedKeysForPetTable);
			foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			expectedForeignKey = new JdbcForeignKey("FK_104A9067", "PERSON_PETS", "PETS_NAME, PETS_RACE, PETS_AGE", "PET", "NAME, RACE, AGE");
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
	void crud() {
		EntityPersister<Person, PersonId> personPersister = entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::setId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.map(Person::getAge)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.setAge(35);
		
		personPersister.insert(dummyPerson);
		
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		
		dummyPerson.setAge(36);
		personPersister.update(dummyPerson, loadedPerson, true);
		Table personTable = new Table("Person");
		Column<Table, Integer> age = personTable.addColumn("age", int.class);
		List<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).isEmpty();
	}
	
	@Test
	void crud_columnNameOverridden_columnNameIsUsed() {
		EntityPersister<Person, PersonId> personPersister = entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName).columnName("familyName")
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.map(Person::getAge)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.setAge(35);
		
		personPersister.insert(dummyPerson);
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		
		assertThat(loadedPerson.getId().getLastName()).isEqualTo("Do");
	}
	
	@Test
	void crud_fieldNameOverridden_fieldNameIsUsed() throws SQLException {
		EntityPersister<Person, PersonId> personPersister = entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getFamilyName).fieldName("lastName")
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.map(Person::getAge)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.setAge(35);
		
		personPersister.insert(dummyPerson);
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		
		assertThat(loadedPerson.getId().getLastName()).isEqualTo("Do");
		
		ResultSet tableColumns = persistenceContext.getConnectionProvider().giveConnection().getMetaData().getColumns(null, null,
				((ConfiguredPersister) persistenceContext.getPersister(Person.class)).getMapping().getTargetTable().getName().toUpperCase(), null);
		ResultSetIterator<String> columnNameReader = new ResultSetIterator<String>(tableColumns) {
			@Override
			public String convert(ResultSet resultSet) throws SQLException {
				return resultSet.getString("COLUMN_NAME");
			}
		};
		assertThat(columnNameReader.convert()).containsExactlyInAnyOrder("AGE", "FIRSTNAME", "LASTNAME", "ADDRESS");
	}
	
	@Test
	void crud_fieldNameOverriddenAndColumnNameOverridden_columnNameIsUsed() throws SQLException {
		EntityPersister<Person, PersonId> personPersister = entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getFamilyName).fieldName("lastName").columnName("familyName")
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.map(Person::getAge)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.setAge(35);
		
		personPersister.insert(dummyPerson);
		Person loadedPerson = personPersister.select(dummyPerson.getId());
		
		assertThat(loadedPerson.getId().getLastName()).isEqualTo("Do");
		
		ResultSet tableColumns = persistenceContext.getConnectionProvider().giveConnection().getMetaData().getColumns(null, null,
				((ConfiguredPersister) persistenceContext.getPersister(Person.class)).getMapping().getTargetTable().getName().toUpperCase(), null);
		ResultSetIterator<String> columnNameReader = new ResultSetIterator<String>(tableColumns) {
			@Override
			public String convert(ResultSet resultSet) throws SQLException {
				return resultSet.getString("COLUMN_NAME");
			}
		};
		assertThat(columnNameReader.convert()).containsExactlyInAnyOrder("AGE", "FIRSTNAME", "FAMILYNAME", "ADDRESS");
	}
	
	@Test
	void crud_oneToOne_compositeToSingleKey_ownedBySource() {
		EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.map(Person::getAge)
				.mapOneToOne(Person::getHouse, entityBuilder(House.class, Long.class)
						.mapKey(House::getId, IdentifierPolicy.databaseAutoIncrement()))
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
		List<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).isEmpty();
	}

	@Test
	void crud_oneToOne_compositeToSingleKey_ownedByTarget() {
		EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.map(Person::getAge)
				.mapOneToOne(Person::getHouse, entityBuilder(House.class, Long.class)
						.mapKey(House::getId, IdentifierPolicy.databaseAutoIncrement()))
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
		List<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).isEmpty();
	}
	
	@Test
	void crud_oneToOne_compositeToCompositeKey_ownedBySource() {
		EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.map(Person::getAge)
				.mapOneToOne(Person::getHouse, entityBuilder(House.class, HouseId.class)
						.mapCompositeKey(House::getHouseId, compositeKeyBuilder(HouseId.class)
								.map(HouseId::getNumber)
								.map(HouseId::getStreet)
								.map(HouseId::getZipCode)
								.map(HouseId::getCity), h -> persistedHouses.add(h.getHouseId()), h -> persistedHouses.contains(h.getHouseId()))
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
		List<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).isEmpty();
	}
	
	@Test
	void crud_oneToOne_compositeToCompositeKey_ownedByTarget() {
		EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.map(Person::getAge)
				.mapOneToOne(Person::getHouse, entityBuilder(House.class, HouseId.class)
						.mapCompositeKey(House::getHouseId, compositeKeyBuilder(HouseId.class)
								.map(HouseId::getNumber)
								.map(HouseId::getStreet)
								.map(HouseId::getZipCode)
								.map(HouseId::getCity), h -> persistedHouses.add(h.getHouseId()), h -> persistedHouses.contains(h.getHouseId()))
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
		List<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).isEmpty();
	}
	
	@Test
	void crud_oneToMany_ownedByTarget() {
		EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.map(Person::getAge)
				.mapOneToMany(Person::getPets, entityBuilder(Pet.class, PetId.class)
						.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
								.map(PetId::getName)
								.map(PetId::getRace)
								.map(PetId::getAge), p -> persistedPets.add(p.getId()), p -> persistedPets.contains(p.getId())))
				.mappedBy(Pet::getOwner)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.addPet(new Pet(new PetId("Pluto", "Dog", 4)));
		dummyPerson.addPet(new Pet(new PetId("Rantanplan", "Dog", 5)));
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
		List<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		Column<Table, String> firstNameColumn = personTable.addColumn("firstName", String.class);
		Column<Table, String> lastNameColumn = personTable.addColumn("lastName", String.class);
		Column<Table, String> addressColumn = personTable.addColumn("address", String.class);
		List<PersonId> persons = persistenceContext.select(PersonId::new, firstNameColumn, lastNameColumn, addressColumn);
		assertThat(persons).isEmpty();
	}
	
	@Test
	void crud_oneToMany_withAssociationTable() {
		EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.map(Person::getAge)
				.mapOneToMany(Person::getPets, entityBuilder(Pet.class, PetId.class)
						.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
								.map(PetId::getName)
								.map(PetId::getRace)
								.map(PetId::getAge), p -> persistedPets.add(p.getId()), p -> persistedPets.contains(p.getId())))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.addPet(new Pet(new PetId("Pluto", "Dog", 4)));
		dummyPerson.addPet(new Pet(new PetId("Rantanplan", "Dog", 5)));
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
		List<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		Column<Table, String> firstNameColumn = personTable.addColumn("firstName", String.class);
		Column<Table, String> lastNameColumn = personTable.addColumn("lastName", String.class);
		Column<Table, String> addressColumn = personTable.addColumn("address", String.class);
		List<PersonId> persons = persistenceContext.select(PersonId::new, firstNameColumn, lastNameColumn, addressColumn);
		assertThat(persons).isEmpty();
	}
	
	@Test
	void crud_manyToMany() {
		EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
				.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.map(Person::getAge)
				.mapOneToMany(Person::getPets, entityBuilder(Pet.class, PetId.class)
						.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
								.map(PetId::getName)
								.map(PetId::getRace)
								.map(PetId::getAge), p -> persistedPets.add(p.getId()), p -> persistedPets.contains(p.getId())))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
		dummyPerson.addPet(new Pet(new PetId("Pluto", "Dog", 4)));
		dummyPerson.addPet(new Pet(new PetId("Rantanplan", "Dog", 5)));
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
		List<Integer> ages = persistenceContext.select(SerializableFunction.identity(), age);
		assertThat(ages).containsExactly(36);
		
		personPersister.delete(dummyPerson);
		Column<Table, String> firstNameColumn = personTable.addColumn("firstName", String.class);
		Column<Table, String> lastNameColumn = personTable.addColumn("lastName", String.class);
		Column<Table, String> addressColumn = personTable.addColumn("address", String.class);
		List<PersonId> persons = persistenceContext.select(PersonId::new, firstNameColumn, lastNameColumn, addressColumn);
		assertThat(persons).isEmpty();
	}
	
	@Nested
	class Persist {
		
		@Test
		void oneToMany() {
			EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
					.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
					.map(Person::getAge)
					.mapOneToMany(Person::getPets, entityBuilder(Pet.class, PetId.class)
							.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
									.map(PetId::getName)
									.map(PetId::getRace)
									.map(PetId::getAge), p -> persistedPets.add(p.getId()), p -> persistedPets.contains(p.getId())))
					.mappedBy(Pet::getOwner)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
			dummyPerson.addPet(new Pet(new PetId("Pluto", "Dog", 4)));
			dummyPerson.addPet(new Pet(new PetId("Rantanplan", "Dog", 5)));
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
			loadedPerson.addPet(new Pet(new PetId("Schrodinger", "Cat", -42)));
			loadedPerson.removePet(new PetId("Rantanplan", "Dog", 5));
			personPersister.persist(loadedPerson);
			
			loadedPerson = personPersister.select(dummyPerson.getId());
			assertThat(loadedPerson.getAge()).isEqualTo(36);
			assertThat(loadedPerson.getPets())
					// we don't take "owner" into account because Person doesn't implement equals/hashcode
					.usingRecursiveFieldByFieldElementComparatorIgnoringFields("owner")
					.containsExactlyInAnyOrder(
							new Pet(new PetId("Pluto", "Dog", 4)),
							new Pet(new PetId("Schrodinger", "Cat", -42)));
		}
		
		@Test
		void manyToMany() {
			EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
					.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
					.map(Person::getAge)
					.mapManyToMany(Person::getPets, entityBuilder(Pet.class, PetId.class)
							.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
									.map(PetId::getName)
									.map(PetId::getRace)
									.map(PetId::getAge), p -> persistedPets.add(p.getId()), p -> persistedPets.contains(p.getId())))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
			dummyPerson.addPet(new Pet(new PetId("Pluto", "Dog", 4)));
			dummyPerson.addPet(new Pet(new PetId("Rantanplan", "Dog", 5)));
			dummyPerson.setAge(35);
			
			personPersister.persist(dummyPerson);
			
			Person loadedPerson = personPersister.select(dummyPerson.getId());
			assertThat(loadedPerson.getPets())
					.usingRecursiveFieldByFieldElementComparator()
					.containsExactlyInAnyOrderElementsOf(dummyPerson.getPets());
			
			// changing value to check for database update
			loadedPerson.setAge(36);
			loadedPerson.addPet(new Pet(new PetId("Schrodinger", "Cat", -42)));
			loadedPerson.removePet(new PetId("Rantanplan", "Dog", 5));
			personPersister.persist(loadedPerson);
			
			loadedPerson = personPersister.select(dummyPerson.getId());
			assertThat(loadedPerson.getAge()).isEqualTo(36);
			assertThat(loadedPerson.getPets())
					// we don't take "owner" into account because Person doesn't implement equals/hashcode
					.usingRecursiveFieldByFieldElementComparatorIgnoringFields("owner")
					.containsExactlyInAnyOrder(
							new Pet(new PetId("Pluto", "Dog", 4)),
							new Pet(new PetId("Schrodinger", "Cat", -42)));
		}
		
		@Test
		void oneToMany_manyTypeIsCompositeKeyAndPolymorphic() {
			EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
					.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
					.map(Person::getAge)
					.mapOneToMany(Person::getPets, entityBuilder(Pet.class, PetId.class)
							.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
									.map(PetId::getName)
									.map(PetId::getRace)
									.map(PetId::getAge), p -> persistedPets.add(p.getId()), p -> persistedPets.contains(p.getId()))
							.mapPolymorphism(PolymorphismPolicy.<Pet>joinTable().addSubClass(
									subentityBuilder(Cat.class, PetId.class)))
					)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
			dummyPerson.getPets().add(new Cat(new PetId("Fluffy", "Cat", 3)));
			dummyPerson.getPets().add(new Cat(new PetId("Whiskers", "Cat", 2)));
			personPersister.persist(dummyPerson);
			
			Person loadedPerson = personPersister.select(dummyPerson.getId());
			assertThat(loadedPerson.getPets())
					// we don't take "owner" into account because Person doesn't implement equals/hashcode
					.usingRecursiveFieldByFieldElementComparatorIgnoringFields("owner")
					.containsExactlyInAnyOrderElementsOf(dummyPerson.getPets());
			
			// changing value to check for database update
			loadedPerson.setAge(36);
			loadedPerson.addPet(new Cat(new PetId("Schrodinger", "Cat", -42)));
			loadedPerson.removePet(new PetId("Whiskers", "Cat", 2));
			personPersister.persist(loadedPerson);
			
			loadedPerson = personPersister.select(dummyPerson.getId());
			assertThat(loadedPerson.getAge()).isEqualTo(36);
			assertThat(loadedPerson.getPets())
					// we don't take "owner" into account because Person doesn't implement equals/hashcode
					.usingRecursiveFieldByFieldElementComparatorIgnoringFields("owner")
					.containsExactlyInAnyOrder(
							new Cat(new PetId("Fluffy", "Cat", 3)),
							new Cat(new PetId("Schrodinger", "Cat", -42)));
		}
		
		@Test
		<T extends Table<T>> void oneToMany_manyTypeIsCompositeKeyAndPolymorphic_realLife() {
			EntityPersister<Person, PersonId> personPersister =  entityBuilder(Person.class, PersonId.class)
					.mapCompositeKey(Person::getId, compositeKeyBuilder(PersonId.class)
							.map(PersonId::getFirstName)
							.map(PersonId::getLastName)
							.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
					.map(Person::getAge)
					.mapOneToMany(Person::getPets, entityBuilder(Pet.class, PetId.class)
							.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
									.map(PetId::getName)
									.map(PetId::getRace)
									.map(PetId::getAge), p -> persistedPets.add(p.getId()), p -> persistedPets.contains(p.getId()))
							.mapPolymorphism(PolymorphismPolicy.<Pet>joinTable().addSubClass(
									subentityBuilder(Cat.class, PetId.class)))
					)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person dummyPerson = new Person(new PersonId("John", "Do", "nowhere"));
			dummyPerson.getPets().add(new Cat(new PetId("Fluffy", "Cat", 3)));
			dummyPerson.getPets().add(new Cat(new PetId("Whiskers", "Cat", 2)));
			personPersister.persist(dummyPerson);
			
			Person loadedPerson = personPersister.select(dummyPerson.getId());
			assertThat(loadedPerson.getPets())
					// we don't take "owner" into account because Person doesn't implement equals/hashcode
					.usingRecursiveFieldByFieldElementComparatorIgnoringFields("owner")
					.containsExactlyInAnyOrderElementsOf(dummyPerson.getPets());
			
			// changing value to check for database update
			loadedPerson.setAge(36);
			loadedPerson.addPet(new Cat(new PetId("Schrodinger", "Cat", -42)));
			loadedPerson.removePet(new PetId("Whiskers", "Cat", 2));
			personPersister.persist(loadedPerson);
			
			loadedPerson = personPersister.select(dummyPerson.getId());
			assertThat(loadedPerson.getAge()).isEqualTo(36);
			assertThat(loadedPerson.getPets())
					// we don't take "owner" into account because Person doesn't implement equals/hashcode
					.usingRecursiveFieldByFieldElementComparatorIgnoringFields("owner")
					.containsExactlyInAnyOrder(
							new Cat(new PetId("Fluffy", "Cat", 3)),
							new Cat(new PetId("Schrodinger", "Cat", -42)));
		}
	}
	
	static Object[][] persist_polymorphic_data() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		PersistenceContext persistenceContext1 = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		PersistenceContext persistenceContext2 = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		PersistenceContext persistenceContext3 = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		Object[][] result = new Object[][] {
				{	"single table",
						entityBuilder(Pet.class, PetId.class)
								.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
										.map(PetId::getName)
										.map(PetId::getRace)
										.map(PetId::getAge), p -> {}, p -> false)
								.mapPolymorphism(PolymorphismPolicy.singleTable(Pet.class)
										.addSubClass(subentityBuilder(Cat.class)
												.mapEnum(Cat::getCatBreed), "Pet")
										.addSubClass(subentityBuilder(Dog.class)
												.mapEnum(Dog::getDogBreed), "Dog")
								).build(persistenceContext1)
				},
				{	"joined tables",
						entityBuilder(Pet.class, PetId.class)
								.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
										.map(PetId::getName)
										.map(PetId::getRace)
										.map(PetId::getAge), p -> {}, p -> false)
								.mapPolymorphism(PolymorphismPolicy.joinTable(Pet.class)
										.addSubClass(subentityBuilder(Cat.class)
												.mapEnum(Cat::getCatBreed))
										.addSubClass(subentityBuilder(Dog.class)
												.mapEnum(Dog::getDogBreed))
								).build(persistenceContext2)
				},
				{	"table per class",
						entityBuilder(Pet.class, PetId.class)
								.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
										.map(PetId::getName)
										.map(PetId::getRace)
										.map(PetId::getAge), p -> {}, p -> false)
								.mapPolymorphism(PolymorphismPolicy.tablePerClass(Pet.class)
										.addSubClass(subentityBuilder(Cat.class)
												.mapEnum(Cat::getCatBreed))
										.addSubClass(subentityBuilder(Dog.class)
												.mapEnum(Dog::getDogBreed))
								).build(persistenceContext3)
				},
		};
		new DDLDeployer(persistenceContext1).deployDDL();
		new DDLDeployer(persistenceContext2).deployDDL();
		new DDLDeployer(persistenceContext3).deployDDL();
		return result;
	}
	
	@ParameterizedTest
	@MethodSource("persist_polymorphic_data")
	void persist_polymorphic(String testDisplayName, EntityPersister<Pet, PetId> petPersister) {
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Cat cat = new Cat(new PetId("Pluto", "Dog", 4));
		cat.setCatBreed(CatBreed.Persian);
		petPersister.persist(cat);
		
		Pet loadedPet = petPersister.select(cat.getId());
		assertThat(loadedPet)
				.usingRecursiveComparison()
				.isEqualTo(cat);
		
		cat.setCatBreed(CatBreed.Persian);
		petPersister.persist(cat);
		loadedPet = petPersister.select(cat.getId());
		assertThat(loadedPet)
				.usingRecursiveComparison()
				.isEqualTo(cat);
	}
	
	@Nested
	class CRUD_Polymorphism {
		
		@Test
		void joinedTables() throws SQLException {
			EntityPersister<Pet, PetId> petPersister = entityBuilder(Pet.class, PetId.class)
					.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
							.map(PetId::getName)
							.map(PetId::getRace)
							.map(PetId::getAge), p -> persistedPets.add(p.getId()), p -> persistedPets.contains(p.getId()))
					.mapPolymorphism(PolymorphismPolicy.joinTable(Pet.class)
							.addSubClass(subentityBuilder(Cat.class)
									.mapEnum(Cat::getCatBreed))
							.addSubClass(subentityBuilder(Dog.class)
									.mapEnum(Dog::getDogBreed))
					)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Cat cat = new Cat(new PetId("Pluto", "Dog", 4));
			cat.setCatBreed(CatBreed.Persian);
			petPersister.insert(cat);
			
			Pet loadedPet = petPersister.select(cat.getId());
			assertThat(loadedPet)
					.usingRecursiveComparison()
					.isEqualTo(cat);
			
			cat.setCatBreed(CatBreed.Persian);
			petPersister.update(cat, loadedPet, true);
			loadedPet = petPersister.select(cat.getId());
			assertThat(loadedPet)
					.usingRecursiveComparison()
					.isEqualTo(cat);
			
			
			persistenceContext.getConnectionProvider().giveConnection().commit();
			petPersister.delete(cat);
			String petName = persistenceContext.newQuery("select name from Pet", String.class).mapKey("name", String.class).execute(Accumulators.getFirst());
			assertThat(petName).isNull();
			String catBreed = persistenceContext.newQuery("select catBreed from Cat", String.class).mapKey("name", String.class).execute(Accumulators.getFirst());
			assertThat(catBreed).isNull();
			persistenceContext.getConnectionProvider().giveConnection().rollback();
			
			// we check deleteById to ensure that it takes composite key into account
			petPersister.deleteById(cat);
			petName = persistenceContext.newQuery("select name from Pet", String.class).mapKey("name", String.class).execute(Accumulators.getFirst());
			assertThat(petName).isNull();
			catBreed = persistenceContext.newQuery("select catBreed from Cat", String.class).mapKey("name", String.class).execute(Accumulators.getFirst());
			assertThat(catBreed).isNull();
		}
		
		@Test
		void singleTable() throws SQLException {
			EntityPersister<Pet, PetId> petPersister = entityBuilder(Pet.class, PetId.class)
					.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
							.map(PetId::getName)
							.map(PetId::getRace)
							.map(PetId::getAge), p -> persistedPets.add(p.getId()), p -> persistedPets.contains(p.getId()))
					.mapPolymorphism(PolymorphismPolicy.singleTable(Pet.class)
							.addSubClass(subentityBuilder(Cat.class)
									.mapEnum(Cat::getCatBreed), "Pet")
							.addSubClass(subentityBuilder(Dog.class)
									.mapEnum(Dog::getDogBreed), "Dog")
					)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Cat cat = new Cat(new PetId("Pluto", "Dog", 4));
			cat.setCatBreed(CatBreed.Persian);
			petPersister.insert(cat);
			
			Pet loadedPet = petPersister.select(cat.getId());
			assertThat(loadedPet)
					.usingRecursiveComparison()
					.isEqualTo(cat);
			
			cat.setCatBreed(CatBreed.Persian);
			petPersister.update(cat, loadedPet, true);
			loadedPet = petPersister.select(cat.getId());
			assertThat(loadedPet)
					.usingRecursiveComparison()
					.isEqualTo(cat);
			
			
			persistenceContext.getConnectionProvider().giveConnection().commit();
			petPersister.delete(cat);
			String petName = persistenceContext.newQuery("select name from Pet", String.class).mapKey("name", String.class).execute(Accumulators.getFirst());
			assertThat(petName).isNull();
			String catBreed = persistenceContext.newQuery("select catBreed from Pet", String.class).mapKey("name", String.class).execute(Accumulators.getFirst());
			assertThat(catBreed).isNull();
			persistenceContext.getConnectionProvider().giveConnection().rollback();
			
			// we check deleteById to ensure that it takes composite key into account
			petPersister.deleteById(cat);
			petName = persistenceContext.newQuery("select name from Pet", String.class).mapKey("name", String.class).execute(Accumulators.getFirst());
			assertThat(petName).isNull();
			catBreed = persistenceContext.newQuery("select catBreed from Pet", String.class).mapKey("name", String.class).execute(Accumulators.getFirst());
			assertThat(catBreed).isNull();
		}
		
		@Test
		void tablePerClass() throws SQLException {
			EntityPersister<Pet, PetId> petPersister = entityBuilder(Pet.class, PetId.class)
					.mapCompositeKey(Pet::getId, compositeKeyBuilder(PetId.class)
							.map(PetId::getName)
							.map(PetId::getRace)
							.map(PetId::getAge), p -> persistedPets.add(p.getId()), p -> persistedPets.contains(p.getId()))
					.mapPolymorphism(PolymorphismPolicy.tablePerClass(Pet.class)
							.addSubClass(subentityBuilder(Cat.class)
									.mapEnum(Cat::getCatBreed))
							.addSubClass(subentityBuilder(Dog.class)
									.mapEnum(Dog::getDogBreed))
					)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Cat cat = new Cat(new PetId("Pluto", "Dog", 4));
			cat.setCatBreed(CatBreed.Persian);
			petPersister.insert(cat);
			
			Pet loadedPet = petPersister.select(cat.getId());
			assertThat(loadedPet)
					.usingRecursiveComparison()
					.isEqualTo(cat);
			
			cat.setCatBreed(CatBreed.Persian);
			petPersister.update(cat, loadedPet, true);
			loadedPet = petPersister.select(cat.getId());
			assertThat(loadedPet)
					.usingRecursiveComparison()
					.isEqualTo(cat);
			
			
			persistenceContext.getConnectionProvider().giveConnection().commit();
			petPersister.delete(cat);
			String catBreed = persistenceContext.newQuery("select catBreed from Cat", String.class).mapKey("name", String.class).execute(Accumulators.getFirst());
			assertThat(catBreed).isNull();
			persistenceContext.getConnectionProvider().giveConnection().rollback();
			
			// we check deleteById to ensure that it takes composite key into account
			petPersister.deleteById(cat);
			catBreed = persistenceContext.newQuery("select catBreed from Cat", String.class).mapKey("name", String.class).execute(Accumulators.getFirst());
			assertThat(catBreed).isNull();
		}
	
	}
}
