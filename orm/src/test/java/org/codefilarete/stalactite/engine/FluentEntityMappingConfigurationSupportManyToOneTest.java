package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.PersistenceContext.ExecutableBeanPropertyQueryMapper;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.device.Address;
import org.codefilarete.stalactite.engine.model.device.Company;
import org.codefilarete.stalactite.engine.model.device.Device;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.ALL;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.codefilarete.stalactite.engine.CascadeOptions.RelationMode.READ_ONLY;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportManyToOneTest {
	
	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private FluentEntityMappingBuilder<Person, Identifier<Long>> personConfiguration;
	private FluentEntityMappingBuilder<Company, Identifier<Long>> companyConfiguration;
	private FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration;
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingBuilder = MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName);
		personConfiguration = personMappingBuilder;
		
		FluentEntityMappingBuilder<Company, Identifier<Long>> companyMappingBuilder = MappingEase.entityBuilder(Company.class, Identifier.LONG_TYPE)
				.mapKey(Company::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Company::getName);
		companyConfiguration = companyMappingBuilder;
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
				.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(City::getName);
		cityConfiguration = cityMappingBuilder;
	}
	
	@Nested
	class CascadeDeclaration {
		
		@Test
		void associationOnly_throwsException() {
			FluentEntityMappingBuilder<Device, Identifier<Long>> mappingBuilder = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					// no cascade
					.mapManyToOne(Device::getLocation, cityConfiguration).cascading(ASSOCIATION_ONLY);
			
			assertThatThrownBy(() -> mappingBuilder.build(persistenceContext))
					.extracting(t -> Exceptions.findExceptionInCauses(t, MappingConfigurationException.class), InstanceOfAssertFactories.THROWABLE)
					.hasMessage(RelationMode.ASSOCIATION_ONLY + " is only relevant for one-to-many association");
		}
		
		@Test
		void notDefined_defaultIsAll_getter() {
			EntityPersister<Device, Identifier<Long>> devicePersister = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					// no cascade definition
					.mapManyToOne(Device::getManufacturer, companyConfiguration)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Device device = new Device(new PersistableIdentifier<>(42L));
			device.setManufacturer(new Company(new PersistableIdentifier<>(666L)));
			devicePersister.insert(device);
			
			Device selectedDevice = devicePersister.select(new PersistedIdentifier<>(42L));
			assertThat(selectedDevice.getId().getDelegate()).isEqualTo(42L);
			assertThat(selectedDevice.getManufacturer().getId().getDelegate()).isEqualTo(666L);
			
			devicePersister.delete(selectedDevice);
			
			assertThat(devicePersister.select(new PersistedIdentifier<>(42L))).isEqualTo(null);
			// orphan was'nt removed because cascade is ALL, not ALL_ORPHAN_REMOVAL
			EntityPersister<Company, Identifier<Long>> personPersister = companyConfiguration.build(persistenceContext);
			assertThat(personPersister.select(new PersistedIdentifier<>(666L)).getId().getDelegate()).isEqualTo(666L);
		}
		
		@Test
		void readOnly_getter() throws SQLException {
			EntityPersister<Device, Identifier<Long>> devicePersister = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					// cascade read-only
					.mapManyToOne(Device::getManufacturer, companyConfiguration).cascading(READ_ONLY)
					.build(persistenceContext);
			
			assert_cascade_readOnly(devicePersister);
		}
		
		@Test
		void readOnly_setter() throws SQLException {
			EntityPersister<Device, Identifier<Long>> devicePersister = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					// cascade read-only
					.mapManyToOne(Device::setManufacturer, companyConfiguration).cascading(READ_ONLY)
					.build(persistenceContext);
			
			assert_cascade_readOnly(devicePersister);
		}
		
		private void assert_cascade_readOnly(EntityPersister<Device, Identifier<Long>> devicePersister) throws SQLException {
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Device dummyDevice = new Device(new PersistableIdentifier<>(42L));
			dummyDevice.setName("UPS");
			Company company = new Company(new PersistableIdentifier<>(1L));
			company.setName("World company");
			dummyDevice.setManufacturer(company);
			
			// insert throws integrity constraint because it doesn't save target entity
			assertThatThrownBy(() -> devicePersister.insert(dummyDevice))
					.extracting(t -> Exceptions.findExceptionInCauses(t, BatchUpdateException.class), InstanceOfAssertFactories.THROWABLE)
					.hasMessage("integrity constraint violation: foreign key no parent ; FK_DEVICE_MANUFACTURERID_COMPANY_ID table: DEVICE value: 1");
			
			persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Company(id, name) values (1, 'World company')").execute();
			persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Device(id, name, manufacturerId) values (42, 'UPS', 1)").execute();
			
			// select entity and relation
			Device loadedDevice = devicePersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedDevice.getName()).isEqualTo("UPS");
			assertThat(loadedDevice.getManufacturer().getName()).isEqualTo("World company");
			
			loadedDevice.setName("touched UPS");
			loadedDevice.getManufacturer().setName("touched World Company");
			devicePersister.update(loadedDevice, dummyDevice, false);
			
			// company is left untouched because association is read only
			assertThat(persistenceContext.newQuery("select name from Company where id = 1", String.class)
					.mapKey("name", String.class)
					.execute(Accumulators.getFirst()))
					.isEqualTo("World company");
			
			// deletion has no action on target
			devicePersister.delete(loadedDevice);
			ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select name from Device", String.class)
					.mapKey("name", String.class);
			assertThat(stringExecutableQuery.execute(Accumulators.toSet())
					.isEmpty()).isTrue();
			assertThat(persistenceContext.newQuery("select name from Company where id = 1", String.class)
					.mapKey("name", String.class)
					.execute(Accumulators.getFirst()))
					.isEqualTo("World company");
		}
		
		@Test
		void cascade_deleteWithOrphanRemoval() throws SQLException {
			EntityPersister<Device, Identifier<Long>> devicePersister = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					.mapManyToOne(Device::setManufacturer, companyConfiguration).cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Company(id) values (42), (666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Device(id, manufacturerId) values (100, 42), (200, 666)");
			
			Device persistedCountry = devicePersister.select(new PersistedIdentifier<>(100L));
			devicePersister.delete(persistedCountry);
			ResultSet resultSet;
			// Checking that we deleted what we wanted
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Device where id = 100");
			assertThat(resultSet.next()).isFalse();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Company where id = 42");
			assertThat(resultSet.next()).isFalse();
			// but we didn't delete everything !
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Device where id = 200");
			assertThat(resultSet.next()).isTrue();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Company where id = 666");
			assertThat(resultSet.next()).isTrue();
		}
	}
	
	@Nested
	class ForeignKeyCreation {
		
		@Test
		void defaultBehavior() throws SQLException {
			MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					.mapManyToOne(Device::getLocation, cityConfiguration)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null, "CITY")) {
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
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_DEVICE_LOCATIONID_CITY_ID", "DEVICE", "LOCATIONID", "CITY", "ID");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void defaultBehavior_2Relations() throws SQLException {
			MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					.mapManyToOne(Device::getLocation, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
							.mapKey(Address::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Address::getStreet))
					.mapManyToOne(Device::getManufacturer, MappingEase.entityBuilder(Company.class, Identifier.LONG_TYPE)
							.mapKey(Company::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Company::getName))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null, "ADDRESS")) {
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
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_DEVICE_LOCATIONID_ADDRESS_ID", "DEVICE", "LOCATIONID", "ADDRESS", "ID");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
			
			ResultSetIterator<JdbcForeignKey> fkLocationIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null, "COMPANY")) {
				@Override
				public JdbcForeignKey convert(ResultSet rs) throws SQLException {
					return new JdbcForeignKey(
							rs.getString("FK_NAME"),
							rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
							rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
					);
				}
			};
			foundForeignKey = Iterables.first(fkLocationIterator);
			expectedForeignKey = new JdbcForeignKey("FK_DEVICE_MANUFACTURERID_COMPANY_ID", "DEVICE", "MANUFACTURERID", "COMPANY", "ID");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void withTargetTable_targetTableIsUsed() throws SQLException {
			MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					.mapManyToOne(Device::getLocation, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(City::getName), new Table<>("Township"))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<Table> tableIterator = new ResultSetIterator<Table>(currentConnection.getMetaData().getTables(null, currentConnection.getSchema(),
					null, null)) {
				@Override
				public Table convert(ResultSet rs) throws SQLException {
					return new Table(
							rs.getString("TABLE_NAME")
					);
				}
			};
			Set<String> foundTables = Iterables.collect(() -> tableIterator, Table::getName, HashSet::new);
			assertThat(foundTables).containsExactlyInAnyOrder("DEVICE", "TOWNSHIP");
			
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					"TOWNSHIP")) {
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
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_DEVICE_LOCATIONID_TOWNSHIP_ID", "DEVICE", "LOCATIONID", "TOWNSHIP", "ID");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void withTargetTableSetByTargetEntity_tableSetByTargetEntityIsUSed() throws SQLException {
			MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					.mapManyToOne(Device::getLocation, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE, new Table<>("Town"))
							.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(City::getName))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<Table> tableIterator = new ResultSetIterator<Table>(currentConnection.getMetaData().getTables(null, currentConnection.getSchema(),
					null, null)) {
				@Override
				public Table convert(ResultSet rs) throws SQLException {
					return new Table(
							rs.getString("TABLE_NAME")
					);
				}
			};
			Set<String> foundTables = Iterables.collect(() -> tableIterator, Table::getName, HashSet::new);
			assertThat(foundTables).containsExactlyInAnyOrder("DEVICE", "TOWN");
			
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					"TOWN")) {
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
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_DEVICE_LOCATIONID_TOWN_ID", "DEVICE", "LOCATIONID", "TOWN", "ID");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void withTargetTableAndTableSetByTargetEntity_targetTableIsUsed() throws SQLException {
			MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					.mapManyToOne(Device::getLocation, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE, new Table<>("Town"))
							.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(City::getName), new Table<>("Township"))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<Table> tableIterator = new ResultSetIterator<Table>(currentConnection.getMetaData().getTables(null, currentConnection.getSchema(),
					null, null)) {
				@Override
				public Table convert(ResultSet rs) throws SQLException {
					return new Table(
							rs.getString("TABLE_NAME")
					);
				}
			};
			Set<String> foundTables = Iterables.collect(() -> tableIterator, Table::getName, HashSet::new);
			assertThat(foundTables).containsExactlyInAnyOrder("DEVICE", "TOWNSHIP");
			
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					"TOWNSHIP")) {
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
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_DEVICE_LOCATIONID_TOWNSHIP_ID", "DEVICE", "LOCATIONID", "TOWNSHIP", "ID");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
	}
}
