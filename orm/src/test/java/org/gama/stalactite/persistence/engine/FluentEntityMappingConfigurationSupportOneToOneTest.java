package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.test.Assertions;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.result.ResultSetIterator;
import org.gama.sql.result.RowIterator;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderOneToOneOptions;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderPropertyOptions;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ALL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ALL_ORPHAN_REMOVAL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ASSOCIATION_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportOneToOneTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
	private EntityMappingConfiguration<Person, Identifier<Long>> personConfiguration;
	private EntityMappingConfiguration<City, Identifier<Long>> cityConfiguration;
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
	}
	
	@BeforeEach
	public void initTest() {
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
		
		IFluentMappingBuilderPropertyOptions<Person, Identifier<Long>> personMappingBuilder = FluentEntityMappingConfigurationSupport.from(Person.class, Identifier.LONG_TYPE)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName);
		personConfiguration = personMappingBuilder.getConfiguration();
		
		IFluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityMappingBuilder = FluentEntityMappingConfigurationSupport.from(City.class, Identifier.LONG_TYPE)
				.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName);
		cityConfiguration = cityMappingBuilder.getConfiguration();
	}
	
	@Test
	public void cascade_associationOnly_throwsException() {
		IFluentMappingBuilderOneToOneOptions<Country, Identifier<Long>, ?> mappingBuilder = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				// no cascade
				.addOneToOne(Country::getPresident, personConfiguration).cascading(ASSOCIATION_ONLY);
		
		Assertions.assertThrows(() -> mappingBuilder.build(persistenceContext), Assertions.hasExceptionInCauses(MappingConfigurationException.class)
				.andProjection(Assertions.hasMessage(RelationshipMode.ASSOCIATION_ONLY + " is only relevent for one-to-many association")));
	}
	
	@Test
	public void cascade_none_defaultIsReadOnly_getter() throws SQLException {
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				// no cascade
				.addOneToOne(Country::getPresident, personConfiguration)
				.build(persistenceContext);
		
		assert_cascade_noCascade_defaultIsReadOnly(countryPersister);
	}
	
	@Test
	public void cascade_none_defaultIsReadOnly_setter() throws SQLException {
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				// no cascade
				.addOneToOne(Country::setPresident, personConfiguration)
				.build(persistenceContext);
		
		assert_cascade_noCascade_defaultIsReadOnly(countryPersister);
	}
	
	private void assert_cascade_noCascade_defaultIsReadOnly(Persister<Country, Identifier<Long>, ?> countryPersister) throws SQLException {
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Country dummyCountry = new Country(new PersistableIdentifier<>(42L));
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setName("French president");
		dummyCountry.setPresident(person);
		
		// insert throws integrity constraint because it doesn't save target entity
		Assertions.assertThrows(() -> countryPersister.insert(dummyCountry), Assertions.hasExceptionInCauses(BatchUpdateException.class)
				.andProjection(Assertions.hasMessage("integrity constraint violation: foreign key no parent; FK_COUNTRY_PRESIDENTID_PERSON_ID table: COUNTRY")));
		
		persistenceContext.getCurrentConnection().prepareStatement("insert into Person(id, name) values (1, 'French president')").execute();
		persistenceContext.getCurrentConnection().prepareStatement("insert into Country(id, name, presidentId) values (42, 'France', 1)").execute();
		
		// select selects entity and relationship
		Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
		assertEquals("France", loadedCountry.getName());
		assertEquals("French president", loadedCountry.getPresident().getName());
		
		loadedCountry.setName("touched France");
		loadedCountry.getPresident().setName("touched french president");
		countryPersister.update(loadedCountry, dummyCountry, false);
		
		// president is left untouched because association is read only
		assertEquals("French president", persistenceContext.newQuery("select name from Person where id = 1", String.class)
				.mapKey(String::new, "name", String.class)
				.execute(persistenceContext.getConnectionProvider())
				.get(0));
		
		// deletion has no action on target
		countryPersister.delete(loadedCountry);
		assertTrue(persistenceContext.newQuery("select name from Country", String.class)
				.mapKey(String::new, "name", String.class)
				.execute(persistenceContext.getConnectionProvider())
				.isEmpty());
		assertEquals("French president", persistenceContext.newQuery("select name from Person where id = 1", String.class)
				.mapKey(String::new, "name", String.class)
				.execute(persistenceContext.getConnectionProvider())
				.get(0));
	}
	
	/**
	 * Thanks to the registering of Identified instances into the ColumnBinderRegistry (cf test preparation) it's possible to have a light relation
	 * between 2 mappings: kind of OneToOne without any cascade, just column of the relation is inserted/updated.
	 * Not really an expected feature since it looks like a OneToOne with insert+update cascade (on insert, already persisted instance are not inserted again)
	 * 
	 * @throws SQLException
	 */
	@Test
	public void lightOneToOne_relationIsPersisted() throws SQLException {
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.add(Country::getPresident, "presidentId")	// this is not a true relation, it's only for presidentId insert/update
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider personIdProvider = new LongProvider(123);
		Person person = new Person(personIdProvider.giveNewIdentifier());
		person.setName("France president");
		
		LongProvider countryIdProvider = new LongProvider(456);
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setPresident(person);
		countryPersister.insert(dummyCountry);
		
		// Checking that the country has the right president in the database
		ResultSet resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery(
				"select count(*) as countryCount from Country where presidentId = " + person.getId().getSurrogate());
		RowIterator resultSetIterator = new RowIterator(resultSet, Maps.asMap("countryCount", DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER));
		resultSetIterator.hasNext();
		assertEquals(1, resultSetIterator.next().get("countryCount"));
		
		
		Country selectedCountry = countryPersister.select(dummyCountry.getId());
		// update test
		Person person2 = new Person(personIdProvider.giveNewIdentifier());
		person2.setName("French president");
		
		dummyCountry.setPresident(person2);
		countryPersister.update(dummyCountry, selectedCountry, false);
		
		// Checking that the country has changed from president in the database
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery(
				"select count(*) as countryCount from Country where presidentId = " + person2.getId().getSurrogate());
		resultSetIterator = new RowIterator(resultSet, Maps.asMap("countryCount", DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER));
		resultSetIterator.hasNext();
		assertEquals(1, resultSetIterator.next().get("countryCount"));
	}
	
	@Test
	public void cascade_insert() {
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		countryPersister.insert(dummyCountry);
		
		// Checking that the country and the president are persisted all together since we asked for an insert cascade
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("Smelly cheese !", persistedCountry.getDescription());
		assertEquals("French president", persistedCountry.getPresident().getName());
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
		
		// Creating a new country with the same president (!): the president shouldn't be resaved
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		countryPersister.insert(dummyCountry2);
		
		// Checking that the country is persisted but not the president since it has been previously
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertEquals(new PersistedIdentifier<>(1L), persistedCountry2.getId());
		assertEquals("French president", persistedCountry2.getPresident().getName());
		assertEquals(persistedCountry.getPresident().getId().getSurrogate(), persistedCountry2.getPresident().getId().getSurrogate());
		// President is cloned since we did nothing during select to reuse the existing one
		assertNotSame(persistedCountry.getPresident(), persistedCountry2.getPresident());
	}
	
	@Test
	public void cascade_insert_mandatory() {
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personConfiguration).cascading(ALL).mandatory()
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		assertThrows(RuntimeMappingException.class, () -> countryPersister.insert(dummyCountry),
				"Non null value expected for relation o.g.s.p.e.m.Person o.g.s.p.e.m.Country.getPresident() on object org.gama.stalactite.persistence.engine.model.Country@0");
	}
	
	@Test
	void foreignKeyIsCreated() throws SQLException {
		FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.foreignKeyNamingStrategy(ForeignKeyNamingStrategy.DEFAULT)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getCapital, cityConfiguration)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Connection currentConnection = persistenceContext.getCurrentConnection();
		ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
				persistenceContext.getPersister(City.class).getMainTable().getName().toUpperCase())) {
			@Override
			public JdbcForeignKey convert(ResultSet rs) throws SQLException {
				return new JdbcForeignKey(
						rs.getString("FK_NAME"),
						rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
						rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
				);
			}
		};
		JdbcForeignKey foundForeignKey = Iterables.first(fkPersonIterator);
		JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_COUNTRY_CAPITALID_CITY_ID", "COUNTRY", "CAPITALID", "CITY", "ID");
		assertEquals(expectedForeignKey.getSignature(), foundForeignKey.getSignature());
	}
	
	@Test
	void foreignKeyIsCreated_relationOwnedByTargetSide() throws SQLException {
		Persister<Country, Identifier<Long>, Table> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.foreignKeyNamingStrategy(ForeignKeyNamingStrategy.DEFAULT)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getCapital, cityConfiguration).mappedBy(City::getCountry)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Connection currentConnection = persistenceContext.getCurrentConnection();
		ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
				countryPersister.getMainTable().getName().toUpperCase())) {
			@Override
			public JdbcForeignKey convert(ResultSet rs) throws SQLException {
				return new JdbcForeignKey(
						rs.getString("FK_NAME"),
						rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
						rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
				);
			}
		};
		JdbcForeignKey foundForeignKey = Iterables.first(fkPersonIterator);
		JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_CITY_COUNTRYID_COUNTRY_ID", "CITY", "COUNTRYID", "COUNTRY", "ID");
		assertEquals(expectedForeignKey.getSignature(), foundForeignKey.getSignature());
	}
	
	@Test
	void foreignKeyIsCreated_relationIsDefinedByColumnOnTargetSide() throws SQLException {
		IFluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityMappingBuilder = FluentEntityMappingConfigurationSupport.from(City.class, Identifier.LONG_TYPE)
				.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName);
		Table<?> cityTable = new Table("city");
		Column<Table, Country> stateColumn = (Column<Table, Country>) cityTable.addColumn("state", Country.class);
		
		Persister<Country, Identifier<Long>, Table> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.foreignKeyNamingStrategy(ForeignKeyNamingStrategy.DEFAULT)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getCapital, cityMappingBuilder.getConfiguration()).mappedBy(stateColumn)
				.build(persistenceContext);
		
		// ensuring that the foreign key is present on table, hence testing that cityTable was used, not a clone created by build(..) 
		JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_city_state_Country_id", "city", "state", "Country", "id");
		Assertions.assertAllEquals(Arrays.asHashSet(expectedForeignKey), Iterables.collect(cityTable.getForeignKeys(), JdbcForeignKey::new, HashSet::new), JdbcForeignKey::getSignature);
		
		// ensuring that the foreign key is also deployed
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Connection currentConnection = persistenceContext.getCurrentConnection();
		ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
				countryPersister.getMainTable().getName().toUpperCase())) {
			@Override
			public JdbcForeignKey convert(ResultSet rs) throws SQLException {
				return new JdbcForeignKey(
						rs.getString("FK_NAME"),
						rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
						rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
				);
			}
		};
		JdbcForeignKey foundForeignKey = Iterables.first(fkPersonIterator);
		Assertions.assertEquals(expectedForeignKey.getSignature(), foundForeignKey.getSignature(), String.CASE_INSENSITIVE_ORDER);
	}
	
	@Test
	void foreignKeyIsCreated_relationIsDefinedByColumnOnTargetSideAndReverseAccessorIsUsed_columnOverrideIsUsed() throws SQLException {
		IFluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityMappingBuilder = FluentEntityMappingConfigurationSupport.from(City.class, Identifier.LONG_TYPE)
				.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName);
		Table cityTable = new Table("city");
		Column<Table, Country> stateColumn = cityTable.addColumn("state", Country.class);
		
		Persister<Country, Identifier<Long>, Table> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.foreignKeyNamingStrategy(ForeignKeyNamingStrategy.DEFAULT)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getCapital, cityMappingBuilder.getConfiguration()).mappedBy(stateColumn).mappedBy(City::getCountry)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Connection currentConnection = persistenceContext.getCurrentConnection();
		ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
				countryPersister.getMainTable().getName().toUpperCase())) {
			@Override
			public JdbcForeignKey convert(ResultSet rs) throws SQLException {
				return new JdbcForeignKey(
						rs.getString("FK_NAME"),
						rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
						rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
				);
			}
		};
		JdbcForeignKey foundForeignKey = Iterables.first(fkPersonIterator);
		JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_CITY_STATE_COUNTRY_ID", "CITY", "STATE", "COUNTRY", "ID");
		assertEquals(expectedForeignKey.getSignature(), foundForeignKey.getSignature());
	}
	
	@Test
	void foreignKeyIsCreated_relationIsDefinedByColumnOnTargetSideAndReverseMutatorIsUsed_columnOverrideIsUsed() throws SQLException {
		IFluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityMappingBuilder = FluentEntityMappingConfigurationSupport.from(City.class, Identifier.LONG_TYPE)
				.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName);
		Table cityTable = new Table("city");
		Column<Table, Country> stateColumn = cityTable.addColumn("state", Country.class);
		
		Persister<Country, Identifier<Long>, Table> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.foreignKeyNamingStrategy(ForeignKeyNamingStrategy.DEFAULT)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getCapital, cityMappingBuilder.getConfiguration()).mappedBy(stateColumn).mappedBy(City::setCountry)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Connection currentConnection = persistenceContext.getCurrentConnection();
		ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
				countryPersister.getMainTable().getName().toUpperCase())) {
			@Override
			public JdbcForeignKey convert(ResultSet rs) throws SQLException {
				return new JdbcForeignKey(
						rs.getString("FK_NAME"),
						rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
						rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
				);
			}
		};
		JdbcForeignKey foundForeignKey = Iterables.first(fkPersonIterator);
		JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_CITY_STATE_COUNTRY_ID", "CITY", "STATE", "COUNTRY", "ID");
		assertEquals(expectedForeignKey.getSignature(), foundForeignKey.getSignature());
	}
	
	@Nested
	class CascadeUpdate {
		
		@Test
		void relationChanged() {
			Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			LongProvider countryIdProvider = new LongProvider();
			LongProvider personIdProvider = new LongProvider();
			Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
			dummyCountry.setName("France");
			dummyCountry.setDescription("Smelly cheese !");
			Person originalPresident = new Person(personIdProvider.giveNewIdentifier());
			originalPresident.setName("French president");
			dummyCountry.setPresident(originalPresident);
			countryPersister.insert(dummyCountry);
			
			// Changing president's name to see what happens when we save it to the database
			Country persistedCountry = countryPersister.select(dummyCountry.getId());
			persistedCountry.getPresident().setName("French president renamed");
			countryPersister.update(persistedCountry, dummyCountry, true);
			// Checking that changing president's name is pushed to the database when we save the country
			Country countryFromDB = countryPersister.select(dummyCountry.getId());
			assertEquals("French president renamed", countryFromDB.getPresident().getName());
			assertTrue(persistedCountry.getPresident().getId().isPersisted());
			
			// Changing president
			Person newPresident = new Person(personIdProvider.giveNewIdentifier());
			newPresident.setName("new French president");
			persistedCountry.setPresident(newPresident);
			countryPersister.update(persistedCountry, countryFromDB, true);
			// Checking that president has changed
			countryFromDB = countryPersister.select(dummyCountry.getId());
			assertEquals("new French president", countryFromDB.getPresident().getName());
			assertEquals(newPresident.getId(), countryFromDB.getPresident().getId());
			// and original one was left untouched
			Persister<Person, Identifier<Long>, ?> personPersister = persistenceContext.getPersister(Person.class);
			assertEquals("French president renamed", personPersister.select(originalPresident.getId()).getName());
		}
	
		@Test
		void relationNullified() {
			Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			LongProvider countryIdProvider = new LongProvider();
			LongProvider personIdProvider = new LongProvider();
			Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
			dummyCountry.setName("France");
			dummyCountry.setDescription("Smelly cheese !");
			Person president = new Person(personIdProvider.giveNewIdentifier());
			president.setName("French president");
			dummyCountry.setPresident(president);
			countryPersister.insert(dummyCountry);
			
			// Removing president
			Country persistedCountry = countryPersister.select(dummyCountry.getId());
			persistedCountry.setPresident(null);
			countryPersister.update(persistedCountry, dummyCountry, true);
			// Checking that president is no more related to country
			Country countryFromDB = countryPersister.select(dummyCountry.getId());
			assertNull(countryFromDB.getPresident());
			// President shouldn't be deleted because orphan removal wasn't asked
			Persister<Person, Identifier<Long>, ?> personPersister = persistenceContext.getPersister(Person.class);
			Person previousPresident = personPersister.select(president.getId());
			assertNotNull(previousPresident);
			// properties shouldn't have been nullified
			assertNotNull(previousPresident.getName());
		}
		
		@Test
		void relationNullifiedWithOrphanRemoval() {
			Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToOne(Country::getPresident, personConfiguration).cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			LongProvider countryIdProvider = new LongProvider();
			LongProvider personIdProvider = new LongProvider();
			Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
			dummyCountry.setName("France");
			dummyCountry.setDescription("Smelly cheese !");
			Person president = new Person(personIdProvider.giveNewIdentifier());
			president.setName("French president");
			dummyCountry.setPresident(president);
			countryPersister.insert(dummyCountry);
			
			// Removing president
			Country persistedCountry = countryPersister.select(dummyCountry.getId());
			persistedCountry.setPresident(null);
			countryPersister.update(persistedCountry, dummyCountry, true);
			// Checking that president has changed
			Country countryFromDB = countryPersister.select(dummyCountry.getId());
			assertNull(countryFromDB.getPresident());
			// previous president has been deleted
			Persister<Person, Identifier<Long>, ?> personPersister = persistenceContext.getPersister(Person.class);
			Person previousPresident = personPersister.select(president.getId());
			assertNull(previousPresident);
		}
	
	}
	
	@Test
	void cascade_update_mandatory() {
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personConfiguration).cascading(ALL).mandatory()
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		countryPersister.insert(dummyCountry);
		
		// Changing president's name to see what happens when we save it to the database
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		persistedCountry.setPresident(null);
		assertThrows(RuntimeMappingException.class, () -> countryPersister.update(persistedCountry, dummyCountry, true),
				"Non null value expected for relation o.g.s.p.e.m.Person o.g.s.p.e.m.Country.getPresident() on object org.gama.stalactite.persistence.engine.model.Country@0");
	}
	
	@Test
	void cascade_delete() throws SQLException {
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Person(id) values (42), (666)");
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Country(id, presidentId) values (100, 42), (200, 666)");
		
		Country persistedCountry = countryPersister.select(new PersistedIdentifier<>(100L));
		countryPersister.delete(persistedCountry);
		ResultSet resultSet;
		// Checking that we deleted what we wanted
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 100");
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Person where id = 42");
		assertTrue(resultSet.next());
		// but we didn't delete everything !
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 200");
		assertTrue(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Person where id = 666");
		assertTrue(resultSet.next());
	}
	
	@Test
	void cascade_deleteWithOrphanRemoval() throws SQLException {
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personConfiguration).cascading(ALL_ORPHAN_REMOVAL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Person(id) values (42), (666)");
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Country(id, presidentId) values (100, 42), (200, 666)");
		
		Country persistedCountry = countryPersister.select(new PersistedIdentifier<>(100L));
		countryPersister.delete(persistedCountry);
		ResultSet resultSet;
		// Checking that we deleted what we wanted
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 100");
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Person where id = 42");
		assertFalse(resultSet.next());
		// but we did'nt delete everything !
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 200");
		assertTrue(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Person where id = 666");
		assertTrue(resultSet.next());
	}
	
	@Test
	void multiple_oneToOne() throws SQLException {
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
				.addOneToOne(Country::getCapital, cityConfiguration).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		
		City capital = new City(new LongProvider().giveNewIdentifier());
		capital.setName("Paris");
		dummyCountry.setCapital(capital);
		
		// testing insert cascade
		countryPersister.insert(dummyCountry);
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("French president", persistedCountry.getPresident().getName());
		assertEquals("Paris", persistedCountry.getCapital().getName());
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
		assertTrue(persistedCountry.getCapital().getId().isPersisted());
		
		// testing insert cascade with another Country reusing OneToOne entities
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		dummyCountry2.setCapital(capital);
		countryPersister.insert(dummyCountry2);
		// database must be up to date
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertEquals(new PersistedIdentifier<>(1L), persistedCountry2.getId());
		assertEquals("French president", persistedCountry2.getPresident().getName());
		assertEquals(persistedCountry.getPresident().getId().getSurrogate(), persistedCountry2.getPresident().getId().getSurrogate());
		assertEquals(persistedCountry.getCapital().getId().getSurrogate(), persistedCountry2.getCapital().getId().getSurrogate());
		assertNotSame(persistedCountry.getPresident(), persistedCountry2.getPresident());
		assertNotSame(persistedCountry.getCapital(), persistedCountry2.getCapital());
		
		// testing update cascade
		persistedCountry2.getPresident().setName("French president renamed");
		persistedCountry2.getCapital().setName("Paris renamed");
		countryPersister.update(persistedCountry2, dummyCountry2, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select name from Person");
		resultSet.next();
		assertEquals("French president renamed", resultSet.getString("name"));
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select name from City");
		resultSet.next();
		assertEquals("Paris renamed", resultSet.getString("name"));
		assertFalse(resultSet.next());
		
		// testing delete cascade
		// but we have to remove first the other country that points to the same president, else will get a constraint violation
		assertEquals(1, persistenceContext.getCurrentConnection().createStatement().executeUpdate(
				"update Country set presidentId = null, capitalId = null where id = " + dummyCountry2.getId().getSurrogate()));
		countryPersister.delete(persistedCountry);
		// database must be up to date
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = " + persistedCountry.getId().getSurrogate());
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Person where id = " + persistedCountry.getPresident().getId().getSurrogate());
		assertTrue(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from City where id = " + persistedCountry.getCapital().getId().getSurrogate());
		assertTrue(resultSet.next());
	}
	
	@Test
	void multiple_oneToOne_partialOrphanRemoval() throws SQLException {
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personConfiguration).cascading(ALL_ORPHAN_REMOVAL)
				.addOneToOne(Country::getCapital, cityConfiguration).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		
		City capital = new City(new LongProvider().giveNewIdentifier());
		capital.setName("Paris");
		dummyCountry.setCapital(capital);
		
		// testing insert cascade
		countryPersister.insert(dummyCountry);
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("French president", persistedCountry.getPresident().getName());
		assertEquals("Paris", persistedCountry.getCapital().getName());
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
		assertTrue(persistedCountry.getCapital().getId().isPersisted());
		
		// testing insert cascade with another Country reusing OneToOne entities
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		dummyCountry2.setCapital(capital);
		countryPersister.insert(dummyCountry2);
		// database must be up to date
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertEquals(new PersistedIdentifier<>(1L), persistedCountry2.getId());
		assertEquals("French president", persistedCountry2.getPresident().getName());
		assertEquals(persistedCountry.getPresident().getId().getSurrogate(), persistedCountry2.getPresident().getId().getSurrogate());
		assertEquals(persistedCountry.getCapital().getId().getSurrogate(), persistedCountry2.getCapital().getId().getSurrogate());
		assertNotSame(persistedCountry.getPresident(), persistedCountry2.getPresident());
		assertNotSame(persistedCountry.getCapital(), persistedCountry2.getCapital());
		
		// testing update cascade
		// but we have to remove first the other country that points to the same president, else will get a constraint violation
		assertEquals(1, persistenceContext.getCurrentConnection().createStatement().executeUpdate(
				"update Country set presidentId = null, capitalId = null where id = " + dummyCountry.getId().getSurrogate()));
		persistedCountry2.setPresident(null);
		persistedCountry2.getCapital().setName("Paris renamed");
		countryPersister.update(persistedCountry2, dummyCountry2, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select name from Person");
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select name from City");
		resultSet.next();
		assertEquals("Paris renamed", resultSet.getString("name"));
		assertFalse(resultSet.next());
		
		// testing delete cascade
		countryPersister.delete(persistedCountry2);
		// database must be up to date
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = " + persistedCountry2.getId().getSurrogate());
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Person where id = " + dummyCountry2.getPresident().getId().getSurrogate());
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from City where id = " + persistedCountry2.getCapital().getId().getSurrogate());
		assertTrue(resultSet.next());
	}
	
	
	@Nested
	class CascadeAll {
		
		@Test
		void ownedBySourceSide() {
			Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getDescription)
					.addOneToOne(Country::getCapital, cityConfiguration).cascading(ALL).mappedBy(City::getCountry)
					.build(persistenceContext);
			
			testCascadeAll(countryPersister);
		}
		
		@Test
		void ownedByReverseSideGetter() {
			Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getDescription)
					.addOneToOne(Country::getCapital, cityConfiguration).cascading(ALL).mappedBy(City::getCountry)
					.build(persistenceContext);
			
			testCascadeAll(countryPersister);
		}
		
		@Test
		void ownedByReverseSideSetter() {
			Persister<Country, Identifier<Long>, ?> countryPersister = FluentEntityMappingConfigurationSupport.from(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getDescription)
					.addOneToOne(Country::getCapital, cityConfiguration).cascading(ALL).mappedBy(City::setCountry)
					.build(persistenceContext);
			
			testCascadeAll(countryPersister);
		}
		
		/**
		 * Common tests of cascade-all with different owner definition.
		 * Should have been done with a @ParameterizedTest but can't be done in such a way due to database commit between tests and cityPersister
		 * dependency
		 */
		private void testCascadeAll(Persister<Country, Identifier<Long>, ?> countryPersister) {
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			LongProvider countryIdProvider = new LongProvider();
			Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
			dummyCountry.setName("France");
			dummyCountry.setDescription("Smelly cheese !");
			
			LongProvider cityIdProvider = new LongProvider();
			City paris = new City(cityIdProvider.giveNewIdentifier());
			paris.setName("Paris");
			dummyCountry.setCapital(paris);
			
			// insert cascade test
			countryPersister.insert(dummyCountry);
			Country persistedCountry = countryPersister.select(dummyCountry.getId());
			assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
			assertEquals("Smelly cheese !", persistedCountry.getDescription());
			assertEquals("Paris", persistedCountry.getCapital().getName());
			assertTrue(persistedCountry.getCapital().getId().isPersisted());
			
			// choosing better names for next tests
			Country modifiedCountry = persistedCountry;
			Country referentCountry = dummyCountry;
			
			Persister<City, Object, ?> cityPersister = persistenceContext.getPersister(City.class);
			// nullifiying relation test
			modifiedCountry.setCapital(null);
			countryPersister.update(modifiedCountry, referentCountry, false);
			modifiedCountry = countryPersister.select(referentCountry.getId());
			assertNull(modifiedCountry.getCapital());
			// ensuring that capital was not deleted nor updated (we didn't asked for orphan removal)
			City loadedParis = cityPersister.select(paris.getId());
			assertEquals("Paris", loadedParis.getName());
			// but relation is cut on both sides (because setCapital(..) calls setCountry(..))
			assertNull(loadedParis.getCountry());
			
			// from null to a (new) object
			referentCountry = countryPersister.select(referentCountry.getId());
			City lyon = new City(cityIdProvider.giveNewIdentifier());
			lyon.setName("Lyon");
			modifiedCountry.setCapital(lyon);
			countryPersister.update(modifiedCountry, referentCountry, false);
			modifiedCountry = countryPersister.select(referentCountry.getId());
			assertEquals(lyon, modifiedCountry.getCapital());
			// ensuring that capital was not deleted nor updated
			assertEquals("Lyon", cityPersister.select(lyon.getId()).getName());
			
			// testing update cascade
			modifiedCountry.getCapital().setName("Lyon renamed");
			countryPersister.update(modifiedCountry, referentCountry, false);
			modifiedCountry = countryPersister.select(referentCountry.getId());
			// ensuring that capital was not deleted nor updated
			assertEquals("Lyon renamed", cityPersister.select(lyon.getId()).getName());
			
			// testing delete cascade
			countryPersister.delete(modifiedCountry);
			// ensuring that capital was not deleted nor updated
			City loadedLyon = cityPersister.select(lyon.getId());
			assertEquals("Lyon renamed", loadedLyon.getName());
			assertNull(loadedLyon.getCountry());
		}
	}
}
