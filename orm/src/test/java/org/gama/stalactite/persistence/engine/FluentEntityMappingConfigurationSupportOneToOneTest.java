package org.codefilarete.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.stalactite.persistence.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.persistence.engine.FluentEntityMappingBuilder.FluentMappingBuilderOneToOneOptions;
import org.codefilarete.stalactite.persistence.engine.FluentEntityMappingBuilder.FluentMappingBuilderPropertyOptions;
import org.codefilarete.stalactite.persistence.engine.PersistenceContext.ExecutableBeanPropertyQueryMapper;
import org.codefilarete.stalactite.persistence.engine.model.City;
import org.codefilarete.stalactite.persistence.engine.model.Country;
import org.codefilarete.stalactite.persistence.engine.model.Person;
import org.codefilarete.stalactite.persistence.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.persistence.id.Identified;
import org.codefilarete.stalactite.persistence.id.Identifier;
import org.codefilarete.stalactite.persistence.id.PersistableIdentifier;
import org.codefilarete.stalactite.persistence.id.PersistedIdentifier;
import org.codefilarete.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.persistence.id.provider.LongProvider;
import org.codefilarete.stalactite.persistence.sql.HSQLDBDialect;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.ForeignKey;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.sql.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.persistence.engine.CascadeOptions.RelationMode.ALL;
import static org.codefilarete.stalactite.persistence.engine.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.codefilarete.stalactite.persistence.engine.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.codefilarete.stalactite.persistence.engine.CascadeOptions.RelationMode.READ_ONLY;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportOneToOneTest {
	
	private final HSQLDBDialect dialect = new HSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private FluentMappingBuilderPropertyOptions<Person, Identifier<Long>> personConfiguration;
	private FluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityConfiguration;
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
		
		FluentMappingBuilderPropertyOptions<Person, Identifier<Long>> personMappingBuilder = MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName);
		personConfiguration = personMappingBuilder;
		
		FluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityMappingBuilder = MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
				.mapKey(City::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(City::getName);
		cityConfiguration = cityMappingBuilder;
	}
	
	@Nested
	class CascadeDeclaration {
		
		@Test
		void associationOnly_throwsException() {
			FluentMappingBuilderOneToOneOptions<Country, Identifier<Long>, ?> mappingBuilder = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					// no cascade
					.mapOneToOne(Country::getPresident, personConfiguration).cascading(ASSOCIATION_ONLY);
			
			assertThatThrownBy(() -> mappingBuilder.build(persistenceContext))
					.extracting(t -> Exceptions.findExceptionInCauses(t, MappingConfigurationException.class), InstanceOfAssertFactories.THROWABLE)
					.hasMessage(RelationMode.ASSOCIATION_ONLY + " is only relevent for one-to-many association");
		}
		
		@Test
		void notDefined_defaultIsAll_getter() {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					// no cascade
					.mapOneToOne(Country::getPresident, personConfiguration)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Country country = new Country(new PersistableIdentifier<>(42L));
			country.setPresident(new Person(new PersistableIdentifier<>(666L)));
			countryPersister.insert(country);
			
			Country selectedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertThat(selectedCountry.getId().getSurrogate()).isEqualTo(42L);
			assertThat(selectedCountry.getPresident().getId().getSurrogate()).isEqualTo(666L);
			
			countryPersister.delete(selectedCountry);
			
			assertThat(countryPersister.select(new PersistedIdentifier<>(42L))).isEqualTo(null);
			// orphan was'nt removed because cascade is ALL, not ALL_ORPHAN_REMOVAL
			assertThat(persistenceContext.getPersister(Person.class).select(new PersistedIdentifier<>(666L)).getId().getSurrogate()).isEqualTo(666L);
		}
		
		@Test
		void readOnly_getter() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					// no cascade
					.mapOneToOne(Country::getPresident, personConfiguration).cascading(READ_ONLY)
					.build(persistenceContext);
			
			assert_cascade_readOnly(countryPersister);
		}
		
		@Test
		void readOnly_setter() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					// no cascade
					.mapOneToOne(Country::setPresident, personConfiguration).cascading(READ_ONLY)
					.build(persistenceContext);
			
			assert_cascade_readOnly(countryPersister);
		}
		
		private void assert_cascade_readOnly(EntityPersister<Country, Identifier<Long>> countryPersister) throws SQLException {
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Country dummyCountry = new Country(new PersistableIdentifier<>(42L));
			dummyCountry.setName("France");
			dummyCountry.setDescription("Smelly cheese !");
			Person person = new Person(new PersistableIdentifier<>(1L));
			person.setName("French president");
			dummyCountry.setPresident(person);
			
			// insert throws integrity constraint because it doesn't save target entity
			assertThatThrownBy(() -> countryPersister.insert(dummyCountry))
					.extracting(t -> Exceptions.findExceptionInCauses(t, BatchUpdateException.class), InstanceOfAssertFactories.THROWABLE)
					.hasMessage("integrity constraint violation: foreign key no parent; FK_COUNTRY_PRESIDENTID_PERSON_ID table: COUNTRY");
			
			persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Person(id, name) values (1, 'French president')").execute();
			persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Country(id, name, presidentId) values (42, 'France', 1)").execute();
			
			// select selects entity and relation
			Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedCountry.getName()).isEqualTo("France");
			assertThat(loadedCountry.getPresident().getName()).isEqualTo("French president");
			
			loadedCountry.setName("touched France");
			loadedCountry.getPresident().setName("touched french president");
			countryPersister.update(loadedCountry, dummyCountry, false);
			
			// president is left untouched because association is read only
			assertThat(persistenceContext.newQuery("select name from Person where id = 1", String.class)
					.mapKey("name", String.class)
					.execute()
					.get(0)).isEqualTo("French president");
			
			// deletion has no action on target
			countryPersister.delete(loadedCountry);
			assertThat(persistenceContext.newQuery("select name from Country", String.class)
					.mapKey("name", String.class)
					.execute()
					.isEmpty()).isTrue();
			assertThat(persistenceContext.newQuery("select name from Person where id = 1", String.class)
					.mapKey("name", String.class)
					.execute()
					.get(0)).isEqualTo("French president");
		}
		
		@Test
		void readOnly_getter_ownedByTarget() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					// no cascade
					.mapOneToOne(Country::getPresident, personConfiguration).cascading(READ_ONLY).mappedBy(Person::getCountry)
					.build(persistenceContext);
			
			assert_cascade_readOnly_ownByTarget(countryPersister);
		}
		
		private void assert_cascade_readOnly_ownByTarget(EntityPersister<Country, Identifier<Long>> countryPersister) throws SQLException {
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Country dummyCountry = new Country(new PersistableIdentifier<>(42L));
			dummyCountry.setName("France");
			dummyCountry.setDescription("Smelly cheese !");
			Person person = new Person(new PersistableIdentifier<>(1L));
			person.setName("French president");
			dummyCountry.setPresident(person);
			
			// person must be persisted before usage because cascade is marked as READ_ONLY
			persistenceContext.getPersister(Person.class).insert(person);
			
			// insert doesn't throw integrity constraint and will update foreign key in Person table making relation available on load
			countryPersister.insert(dummyCountry);
			
			// select selects entity and relation
			Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedCountry.getName()).isEqualTo("France");
			assertThat(loadedCountry.getPresident().getName()).isEqualTo("French president");
			
			loadedCountry.setName("touched France");
			loadedCountry.getPresident().setName("touched french president");
			countryPersister.update(loadedCountry, dummyCountry, false);
			
			// president is left untouched because association is read only
			assertThat(persistenceContext.newQuery("select name from Person where id = 1", String.class)
					.mapKey("name", String.class)
					.execute()
					.get(0)).isEqualTo("French president");
			
			// Changing country persident to check foreign key modification
			Person newPerson = new Person(new PersistableIdentifier<>(2L));
			newPerson.setName("New French president");
			// person must be persisted before usage because cascade is marked as READ_ONLY
			persistenceContext.getPersister(Person.class).insert(newPerson);
			
			dummyCountry.setPresident(newPerson);
			countryPersister.update(dummyCountry, loadedCountry, true);
			
			
			assertThat(countryPersister.select(new PersistedIdentifier<>(42L)).getPresident().getName()).isEqualTo("New French president");
			
			// deletion doesn't throws integrity constraint and nullify foreign key
			countryPersister.delete(dummyCountry);
			
			assertThat(persistenceContext.newQuery("select name from Country", String.class)
					.mapKey("name", String.class)
					.execute()).isEmpty();
			assertThat(persistenceContext.newQuery("select name from Person", String.class)
					.mapKey("name", String.class)
					.execute())
					.containsExactly("French president", "New French president");
			assertThat(persistenceContext.newQuery("select name from Person where id = 2", String.class)
					.mapKey("name", String.class)
					.execute())
					.first().isEqualTo("New French president");
		}
		
		@Test
		void cascade_deleteWithOrphanRemoval() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getDescription)
					.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Person(id) values (42), (666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country(id, presidentId) values (100, 42), (200, 666)");
			
			Country persistedCountry = countryPersister.select(new PersistedIdentifier<>(100L));
			countryPersister.delete(persistedCountry);
			ResultSet resultSet;
			// Checking that we deleted what we wanted
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 100");
			assertThat(resultSet.next()).isFalse();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Person where id = 42");
			assertThat(resultSet.next()).isFalse();
			// but we did'nt delete everything !
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 200");
			assertThat(resultSet.next()).isTrue();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Person where id = 666");
			assertThat(resultSet.next()).isTrue();
		}
	}
	
	/**
	 * Thanks to the registering of Identified instances into the ColumnBinderRegistry it's possible to have a light relation
	 * between 2 mappings: kind of OneToOne without any cascade, just column of the relation is inserted/updated.
	 * Not really an expected feature since it looks like a OneToOne with insert+update cascade (on insert, already persisted instance are not inserted again)
	 */
	@Test
	public void lightOneToOne_relationIsPersisted() throws SQLException {
		// we redifine th eDialcet to avoid polluting the instance one with some more mapping that is only the purpose of this test (avoid side effect)
		HSQLDBDialect dialect = new HSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		dialect.getColumnBinderRegistry().register((Class) Person.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Person.class, "int");
		
		PersistenceContext persistenceContext = new PersistenceContext(dataSource, dialect);
		
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription)
				.map(Country::getPresident, "presidentId")	// this is not a true relation, it's only for presidentId insert/update
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
		ResultSet resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery(
				"select count(*) as countryCount from Country where presidentId = " + person.getId().getSurrogate());
		RowIterator resultSetIterator = new RowIterator(resultSet, Maps.asMap("countryCount", DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER));
		resultSetIterator.hasNext();
		assertThat(resultSetIterator.next().get("countryCount")).isEqualTo(1);
		
		
		Country selectedCountry = countryPersister.select(dummyCountry.getId());
		// update test
		Person person2 = new Person(personIdProvider.giveNewIdentifier());
		person2.setName("French president");
		
		dummyCountry.setPresident(person2);
		countryPersister.update(dummyCountry, selectedCountry, false);
		
		// Checking that the country has changed from president in the database
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery(
				"select count(*) as countryCount from Country where presidentId = " + person2.getId().getSurrogate());
		resultSetIterator = new RowIterator(resultSet, Maps.asMap("countryCount", DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER));
		resultSetIterator.hasNext();
		assertThat(resultSetIterator.next().get("countryCount")).isEqualTo(1);
	}
	
	@Nested
	class ForeignKeyCreation {
		
		@Test
		void relationOwnedBySource() throws SQLException {
			MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityConfiguration)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(City.class)).getMappingStrategy().getTargetTable().getName().toUpperCase())) {
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
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void relationOwnedByTargetSide() throws SQLException {
			ConfiguredPersister<Country, Identifier<Long>> countryPersister =
					(ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityConfiguration).mappedBy(City::getCountry)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					countryPersister.getMappingStrategy().getTargetTable().getName().toUpperCase())) {
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
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void relationIsDefinedByColumnOnTargetSide() throws SQLException {
			FluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityMappingBuilder = MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
					.mapKey(City::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(City::getName);
			Table cityTable = new Table("city");
			Column<Table, Identifier<Long>> stateColumn = cityTable.addColumn("state", Identifier.LONG_TYPE);
			
			ConfiguredPersister<Country, Identifier<Long>> countryPersister =
					(ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityMappingBuilder).mappedBy(stateColumn)
					.build(persistenceContext);
			
			// ensuring that the foreign key is present on table, hence testing that cityTable was used, not a clone created by build(..) 
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_city_state_Country_id", "city", "state", "Country", "id");
			Comparator<JdbcForeignKey> comparing = Comparator.comparing(JdbcForeignKey::getSignature, Comparator.naturalOrder());
			assertThat((Set<ForeignKey<? extends Table, ?>>) cityTable.getForeignKeys()).extracting(JdbcForeignKey::new)
					.usingElementComparator(comparing)
					.containsExactlyInAnyOrder(expectedForeignKey);
			
			// ensuring that the foreign key is also deployed
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					countryPersister.getMappingStrategy().getTargetTable().getName().toUpperCase())) {
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
			assertThat(foundForeignKey.getSignature()).isEqualToIgnoringCase(expectedForeignKey.getSignature());
		}
		
		@Test
		void relationIsDefinedByColumnOnTargetSideAndReverseAccessorIsUsed_columnOverrideIsUsed() throws SQLException {
			FluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityMappingBuilder = MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
					.mapKey(City::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(City::getName);
			Table cityTable = new Table("city");
			Column<Table, Identifier<Long>> stateColumn = cityTable.addColumn("state", Identifier.LONG_TYPE);
			
			ConfiguredPersister<Country, Identifier<Long>> countryPersister =
					(ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityMappingBuilder).mappedBy(stateColumn).mappedBy(City::getCountry)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					countryPersister.getMappingStrategy().getTargetTable().getName().toUpperCase())) {
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
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void relationIsDefinedByColumnOnTargetSideAndReverseMutatorIsUsed_columnOverrideIsUsed() throws SQLException {
			FluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityMappingBuilder = MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
					.mapKey(City::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(City::getName);
			Table cityTable = new Table("city");
			Column<Table, Identifier<Long>> stateColumn = cityTable.addColumn("state", Identifier.LONG_TYPE);
			
			ConfiguredPersister<Country, Identifier<Long>> countryPersister =
					(ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityMappingBuilder).mappedBy(stateColumn).mappedBy(City::setCountry)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					countryPersister.getMappingStrategy().getTargetTable().getName().toUpperCase())) {
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
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
	}
	
	
	@Test
	void multiple_oneToOne() throws SQLException {
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getDescription)
				.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
				.mapOneToOne(Country::getCapital, cityConfiguration).cascading(ALL)
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
		assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry.getCapital().getName()).isEqualTo("Paris");
		assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
		assertThat(persistedCountry.getCapital().getId().isPersisted()).isTrue();
		
		// testing insert cascade with another Country reusing OneToOne entities
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		dummyCountry2.setCapital(capital);
		countryPersister.insert(dummyCountry2);
		// database must be up to date
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertThat(persistedCountry2.getId()).isEqualTo(new PersistedIdentifier<>(1L));
		assertThat(persistedCountry2.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry2.getPresident().getId().getSurrogate()).isEqualTo(persistedCountry.getPresident().getId().getSurrogate());
		assertThat(persistedCountry2.getCapital().getId().getSurrogate()).isEqualTo(persistedCountry.getCapital().getId().getSurrogate());
		assertThat(persistedCountry2.getPresident()).isNotSameAs(persistedCountry.getPresident());
		assertThat(persistedCountry2.getCapital()).isNotSameAs(persistedCountry.getCapital());
		
		// testing update cascade
		persistedCountry2.getPresident().setName("French president renamed");
		persistedCountry2.getCapital().setName("Paris renamed");
		countryPersister.update(persistedCountry2, dummyCountry2, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from Person");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("French president renamed");
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from City");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("Paris renamed");
		assertThat(resultSet.next()).isFalse();
		
		// testing delete cascade
		// but we have to remove first the other country that points to the same president, else will get a constraint violation
		assertThat(persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate(
				"update Country set presidentId = null, capitalId = null where id = " + dummyCountry2.getId().getSurrogate())).isEqualTo(1);
		countryPersister.delete(persistedCountry);
		// database must be up to date
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Country where id = " + persistedCountry.getId().getSurrogate());
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Person where id = " + persistedCountry.getPresident().getId().getSurrogate());
		assertThat(resultSet.next()).isTrue();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from City where id = " + persistedCountry.getCapital().getId().getSurrogate());
		assertThat(resultSet.next()).isTrue();
	}
	
	@Test
	void multiple_oneToOne_partialOrphanRemoval() throws SQLException {
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getDescription)
				.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL_ORPHAN_REMOVAL)
				.mapOneToOne(Country::getCapital, cityConfiguration).cascading(ALL)
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
		assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry.getCapital().getName()).isEqualTo("Paris");
		assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
		assertThat(persistedCountry.getCapital().getId().isPersisted()).isTrue();
		
		// testing insert cascade with another Country reusing OneToOne entities
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		dummyCountry2.setCapital(capital);
		countryPersister.insert(dummyCountry2);
		// database must be up to date
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertThat(persistedCountry2.getId()).isEqualTo(new PersistedIdentifier<>(1L));
		assertThat(persistedCountry2.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry2.getPresident().getId().getSurrogate()).isEqualTo(persistedCountry.getPresident().getId().getSurrogate());
		assertThat(persistedCountry2.getCapital().getId().getSurrogate()).isEqualTo(persistedCountry.getCapital().getId().getSurrogate());
		assertThat(persistedCountry2.getPresident()).isNotSameAs(persistedCountry.getPresident());
		assertThat(persistedCountry2.getCapital()).isNotSameAs(persistedCountry.getCapital());
		
		// testing update cascade
		// but we have to remove first the other country that points to the same president, else will get a constraint violation
		assertThat(persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate(
				"update Country set presidentId = null, capitalId = null where id = " + dummyCountry.getId().getSurrogate())).isEqualTo(1);
		persistedCountry2.setPresident(null);
		persistedCountry2.getCapital().setName("Paris renamed");
		countryPersister.update(persistedCountry2, dummyCountry2, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from Person");
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from City");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("Paris renamed");
		assertThat(resultSet.next()).isFalse();
		
		// testing delete cascade
		countryPersister.delete(persistedCountry2);
		// database must be up to date
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Country where id = " + persistedCountry2.getId().getSurrogate());
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Person where id = " + dummyCountry2.getPresident().getId().getSurrogate());
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from City where id = " + persistedCountry2.getCapital().getId().getSurrogate());
		assertThat(resultSet.next()).isTrue();
	}
	
	
	@Nested
	class CascadeAll {
		
		@Test
		void ownedBySourceSide() {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityConfiguration).cascading(ALL).mappedBy(City::getCountry)
					.build(persistenceContext);
			
			checkCascadeAll(countryPersister);
		}
		
		@Test
		void ownedByReverseSideGetter() {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityConfiguration).cascading(ALL).mappedBy(City::getCountry)
					.build(persistenceContext);
			
			checkCascadeAll(countryPersister);
		}
		
		@Test
		void ownedByReverseSideSetter() {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityConfiguration).cascading(ALL).mappedBy(City::setCountry)
					.build(persistenceContext);
			
			checkCascadeAll(countryPersister);
		}
		
		@Test
		void ownedByReverseSideColumn() {
			Table cityTable = new Table("City");
			Column countryId = cityTable.addColumn("countryId", Identifier.LONG_TYPE);
			
			
			EntityMappingConfigurationProvider<City, Identifier<Long>> cityConfigurer = MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
					.mapKey(City::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(City::getName);
			
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityConfigurer).cascading(ALL).mappedBy(countryId)
					.build(persistenceContext);
			
			checkCascadeAll(countryPersister);
		}
		
		/**
		 * Common tests of cascade-all with different owner definition.
		 * Should have been done with a @ParameterizedTest but can't be done in such a way due to database commit between tests and cityPersister
		 * dependency
		 */
		private void checkCascadeAll(EntityPersister<Country, Identifier<Long>> countryPersister) {
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
			assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
			assertThat(persistedCountry.getDescription()).isEqualTo("Smelly cheese !");
			assertThat(persistedCountry.getCapital().getName()).isEqualTo("Paris");
			assertThat(persistedCountry.getCapital().getId().isPersisted()).isTrue();
			
			// choosing better names for next tests
			Country modifiedCountry = persistedCountry;
			Country referentCountry = dummyCountry;
			
			EntityPersister<City, Object> cityPersister = persistenceContext.getPersister(City.class);
			// nullifiying relation test
			modifiedCountry.setCapital(null);
			countryPersister.update(modifiedCountry, referentCountry, false);
			modifiedCountry = countryPersister.select(referentCountry.getId());
			assertThat(modifiedCountry.getCapital()).isNull();
			// ensuring that capital was not deleted nor updated (we didn't asked for orphan removal)
			City loadedParis = cityPersister.select(paris.getId());
			assertThat(loadedParis.getName()).isEqualTo("Paris");
			// but relation is cut on both sides (because setCapital(..) calls setCountry(..))
			assertThat(loadedParis.getCountry()).isNull();
			
			// from null to a (new) object
			referentCountry = countryPersister.select(referentCountry.getId());
			City lyon = new City(cityIdProvider.giveNewIdentifier());
			lyon.setName("Lyon");
			modifiedCountry.setCapital(lyon);
			countryPersister.update(modifiedCountry, referentCountry, false);
			modifiedCountry = countryPersister.select(referentCountry.getId());
			assertThat(modifiedCountry.getCapital()).isEqualTo(lyon);
			// ensuring that capital was not deleted nor updated
			assertThat(cityPersister.select(lyon.getId()).getName()).isEqualTo("Lyon");
			
			// testing update cascade
			referentCountry = countryPersister.select(referentCountry.getId());
			modifiedCountry.getCapital().setName("Lyon renamed");
			countryPersister.update(modifiedCountry, referentCountry, false);
			modifiedCountry = countryPersister.select(referentCountry.getId());
			// ensuring that capital was not deleted nor updated
			assertThat(cityPersister.select(lyon.getId()).getName()).isEqualTo("Lyon renamed");
			
			// testing delete cascade
			countryPersister.delete(modifiedCountry);
			// ensuring that capital was not deleted nor updated
			City loadedLyon = cityPersister.select(lyon.getId());
			assertThat(loadedLyon.getName()).isEqualTo("Lyon renamed");
			assertThat(loadedLyon.getCountry()).isNull();
		}
		
		@Nested
		class Insert {
			
			@Test
			void insertOnce_targetInstanceIsInserted() {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
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
				assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
				assertThat(persistedCountry.getDescription()).isEqualTo("Smelly cheese !");
				assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
				assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
			}
			
			@Test
			void insertTwice_targetInstanceIsInsertedOnce() {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
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
				
				// Creating a new country with the same president (!): the president shouldn't be resaved
				Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry2.setName("France 2");
				dummyCountry2.setPresident(person);
				countryPersister.insert(dummyCountry2);
				
				// Checking that the country is persisted but not the president since it has been previously
				Country persistedCountry = countryPersister.select(dummyCountry2.getId());
				assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(1L));
				assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
				assertThat(persistedCountry.getPresident().getId().getSurrogate()).isEqualTo(dummyCountry.getPresident().getId().getSurrogate());
				// President is cloned since we did nothing during select to reuse the existing one
				assertThat(persistedCountry.getPresident()).isNotSameAs(dummyCountry.getPresident());
			}
			
			@Test
			void mandatory_withNullTarget_throwsException() {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL).mandatory()
						.build(persistenceContext);
				
				DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
				ddlDeployer.deployDDL();
				
				LongProvider countryIdProvider = new LongProvider();
				Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry.setName("France");
				assertThatExceptionOfType(RuntimeMappingException.class).as("Non null value expected for relation o.g.s.p.e.m.Person o.g.s.p.e.m" 
						+ ".Country.getPresident() on object org.codefilarete.stalactite.persistence.engine.model.Country@0").isThrownBy(() -> countryPersister.insert(dummyCountry));
			}
			
			@Test
			void insert_targetInstanceIsUpdated() {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
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
				
				// Creating a new country with the same president (!) and changing president
				person.setName("Me !!");
				Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry2.setName("France 2");
				dummyCountry2.setPresident(person);
				countryPersister.insert(dummyCountry2);
				
				// Checking that president is modified
				Country persistedCountry = countryPersister.select(dummyCountry2.getId());
				assertThat(persistedCountry.getPresident().getName()).isEqualTo("Me !!");
				// ... and we still a 2 countries (no deletion was done)
				List<Long> countryCount = persistenceContext.newQuery("select count(*) as countryCount from Country", Long.class)
						.mapKey(Long::new, "countryCount", Long.class)
						.execute();
				assertThat(Iterables.first(countryCount)).isEqualTo(2);
			}
			
			@Test
			void insert_targetInstanceIsUpdated_ownedByReverseSide() {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL).mappedBy(Person::getCountry)
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
				person.setCountry(dummyCountry);
				countryPersister.insert(dummyCountry);
				
				// Creating a new country with the same president (!) and modifying president
				person.setName("Me !!");
				Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry2.setName("France 2");
				dummyCountry2.setPresident(person);
				person.setCountry(dummyCountry2);
				countryPersister.insert(dummyCountry2);
				
				// Checking that president is modified
				Country persistedCountry = countryPersister.select(dummyCountry2.getId());
				assertThat(persistedCountry.getPresident().getName()).isEqualTo("Me !!");
				// ... and we still have 2 countries (no deletion was done)
				List<Long> countryCount = persistenceContext.newQuery("select count(*) as countryCount from Country", Long.class)
						.mapKey(Long::new, "countryCount", Long.class)
						.execute();
				assertThat(Iterables.first(countryCount)).isEqualTo(2);
			}
		}
		
		@Nested
		class Update {
			
			@Test
			void relationChanged_relationIsOwnedBySource() {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
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
				assertThat(countryFromDB.getPresident().getName()).isEqualTo("French president renamed");
				assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
				
				// Changing president
				Person newPresident = new Person(personIdProvider.giveNewIdentifier());
				newPresident.setName("new French president");
				persistedCountry.setPresident(newPresident);
				countryPersister.update(persistedCountry, countryFromDB, true);
				// Checking that president has changed
				countryFromDB = countryPersister.select(dummyCountry.getId());
				assertThat(countryFromDB.getPresident().getName()).isEqualTo("new French president");
				assertThat(countryFromDB.getPresident().getId()).isEqualTo(newPresident.getId());
				// and original one was left untouched
				EntityPersister<Person, Identifier<Long>> personPersister = persistenceContext.getPersister(Person.class);
				assertThat(personPersister.select(originalPresident.getId()).getName()).isEqualTo("French president renamed");
			}
			
			@Test
			void relationChanged_relationIsOwnedByTarget() {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL).mappedBy(Person::getCountry)
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
				originalPresident.setCountry(dummyCountry);	// maintaining reverse relation in memory (as it must be), else column has not value which results in an NPE
				countryPersister.insert(dummyCountry);
				
				// Changing president's name to see what happens when we save it to the database
				Country persistedCountry = countryPersister.select(dummyCountry.getId());
				persistedCountry.getPresident().setName("French president renamed");
				countryPersister.update(persistedCountry, dummyCountry, true);
				// Checking that changing president's name is pushed to the database when we save the country
				Country countryFromDB = countryPersister.select(dummyCountry.getId());
				assertThat(countryFromDB.getPresident().getName()).isEqualTo("French president renamed");
				assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
				
				// Changing president
				Person newPresident = new Person(personIdProvider.giveNewIdentifier());
				newPresident.setName("new French president");
				persistedCountry.setPresident(newPresident);
				newPresident.setCountry(dummyCountry);	// maintaining reverse relation in memory (as it must be), else column has not value which results in an NPE
				countryPersister.update(persistedCountry, countryFromDB, true);
				// Checking that president has changed
				countryFromDB = countryPersister.select(dummyCountry.getId());
				assertThat(countryFromDB.getPresident().getName()).isEqualTo("new French president");
				assertThat(countryFromDB.getPresident().getId()).isEqualTo(newPresident.getId());
				// and original one was left untouched
				EntityPersister<Person, Identifier<Long>> personPersister = persistenceContext.getPersister(Person.class);
				assertThat(personPersister.select(originalPresident.getId()).getName()).isEqualTo("French president renamed");
				
				// checking reverse side column value ...
				// ... must be null for old president
				ExecutableBeanPropertyQueryMapper<Long> countryIdQuery = persistenceContext.newQuery("select countryId from Person where id = :personId", Long.class)
						.mapKey("countryId", Long.class);
				List<Long> originalPresidentCountryId = countryIdQuery
						.set("personId", originalPresident.getId())
						.execute();
				assertThat(originalPresidentCountryId.get(0)).isNull();
				// ... and not null for new president
				List<Long> newPresidentCountryId = countryIdQuery
						.set("personId", newPresident.getId())
						.execute();
				assertThat(dummyCountry.getId().getSurrogate()).isEqualTo(newPresidentCountryId.get(0));
			}
			
			@Test
			void relationNullified_relationIsOwnedBySource() {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
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
				assertThat(countryFromDB.getPresident()).isNull();
				// President shouldn't be deleted because orphan removal wasn't asked
				EntityPersister<Person, Identifier<Long>> personPersister = persistenceContext.getPersister(Person.class);
				Person previousPresident = personPersister.select(president.getId());
				assertThat(previousPresident).isNotNull();
				// properties shouldn't have been nullified
				assertThat(previousPresident.getName()).isNotNull();
			}
			
			@Test
			void relationNullifiedWithOrphanRemoval() {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL_ORPHAN_REMOVAL)
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
				assertThat(countryFromDB.getPresident()).isNull();
				// previous president has been deleted
				EntityPersister<Person, Identifier<Long>> personPersister = persistenceContext.getPersister(Person.class);
				Person previousPresident = personPersister.select(president.getId());
				assertThat(previousPresident).isNull();
			}
			
			@Test
			void relationChanged_relationIsOwnedBySource_withOrphanRemoval() {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL_ORPHAN_REMOVAL)
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
				Person newPresident = new Person(personIdProvider.giveNewIdentifier());
				newPresident.setName("New French president");
				persistedCountry.setPresident(newPresident);
				countryPersister.update(persistedCountry, dummyCountry, true);
				// Checking that president has changed
				Country countryFromDB = countryPersister.select(dummyCountry.getId());
				assertThat(countryFromDB.getPresident()).isEqualTo(newPresident);
				// previous president has been deleted
				EntityPersister<Person, Identifier<Long>> personPersister = persistenceContext.getPersister(Person.class);
				Person previousPresident = personPersister.select(president.getId());
				assertThat(previousPresident).isNull();
			}
			
			@Test
			void mandatory_withNullTarget_throwsException() {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL).mandatory()
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
				assertThatExceptionOfType(RuntimeMappingException.class).as("Non null value expected for relation o.g.s.p.e.m.Person o.g.s.p.e.m" 
						+ ".Country.getPresident() on object org.codefilarete.stalactite.persistence.engine.model.Country@0").isThrownBy(() -> countryPersister.update(persistedCountry, dummyCountry, true));
			}
			
		}
		
		@Nested
		class Delete {
			
			@Test
			void targetEntityIsDeleted() throws SQLException {
				EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
						.build(persistenceContext);
				
				DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
				ddlDeployer.deployDDL();
				
				persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Person(id) values (42), (666)");
				persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country(id, presidentId) values (100, 42), (200, 666)");
				
				Country persistedCountry = countryPersister.select(new PersistedIdentifier<>(100L));
				countryPersister.delete(persistedCountry);
				ResultSet resultSet;
				// Checking that we deleted what we wanted
				resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country" 
						+ " where id = 100");
				assertThat(resultSet.next()).isFalse();
				resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Person " 
						+ "where id = 42");
				assertThat(resultSet.next()).isTrue();
				// but we didn't delete everything !
				resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country" 
						+ " where id = 200");
				assertThat(resultSet.next()).isTrue();
				resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Person where id = 666");
				assertThat(resultSet.next()).isTrue();
			}
		}
	}
}
