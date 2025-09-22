package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.PersistenceContext.ExecutableBeanPropertyQueryMapper;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.device.Address;
import org.codefilarete.stalactite.engine.model.device.Company;
import org.codefilarete.stalactite.engine.model.device.Device;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
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
	private FluentEntityMappingBuilder<Company, Identifier<Long>> companyConfiguration;
	private FluentEntityMappingBuilder<Address, Identifier<Long>> addressConfiguration;
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
		
		FluentEntityMappingBuilder<Company, Identifier<Long>> companyMappingBuilder = MappingEase.entityBuilder(Company.class, Identifier.LONG_TYPE)
				.mapKey(Company::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Company::getName);
		companyConfiguration = companyMappingBuilder;
		
		FluentEntityMappingBuilder<Address, Identifier<Long>> addressMappingBuilder = MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
				.mapKey(Address::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Address::getStreet);
		addressConfiguration = addressMappingBuilder;
	}
	
	@Nested
	class CascadeDeclaration {
		
		@Test
		void associationOnly_throwsException() {
			FluentEntityMappingBuilder<Device, Identifier<Long>> mappingBuilder = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					// no cascade
					.mapManyToOne(Device::getLocation, addressConfiguration).cascading(ASSOCIATION_ONLY);
			
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
			
			Device persistedDevice = devicePersister.select(new PersistedIdentifier<>(100L));
			devicePersister.delete(persistedDevice);
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
		
		@Test
		void all() {
			EntityPersister<Device, Identifier<Long>> devicePersister = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					.mapManyToOne(Device::setManufacturer, companyConfiguration).cascading(ALL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			LongProvider deviceIdProvider = new LongProvider(42);
			Device dummyDevice = new Device(deviceIdProvider.giveNewIdentifier());
			dummyDevice.setName("UPS");
			
			LongProvider companyIdProvider = new LongProvider();
			Company company = new Company(companyIdProvider.giveNewIdentifier());
			company.setName("World Company");
			dummyDevice.setManufacturer(company);
			
			// insert cascade test
			devicePersister.insert(dummyDevice);
			Device persistedDevice = devicePersister.select(dummyDevice.getId());
			assertThat(persistedDevice.getId()).isEqualTo(new PersistedIdentifier<>(42L));
			assertThat(persistedDevice.getManufacturer().getName()).isEqualTo("World Company");
			assertThat(persistedDevice.getManufacturer().getId().isPersisted()).isTrue();
			
			// choosing better names for next tests
			Device modifiedDevice = persistedDevice;
			Device referentDevice = dummyDevice;
			
			// nullifiying relation test
			modifiedDevice.setManufacturer(null);
			devicePersister.update(modifiedDevice, referentDevice, false);
			modifiedDevice = devicePersister.select(referentDevice.getId());
			assertThat(modifiedDevice.getManufacturer()).isNull();
			
			// ensuring that manufacturer was not deleted nor updated (we didn't ask for orphan removal)
			ExecutableBeanPropertyQueryMapper<LiteCompany> companySelector = persistenceContext.newQuery("select name from Company where id = :id", LiteCompany.class)
					.mapKey(LiteCompany::new, "name", String.class);
			LiteCompany liteCompany = companySelector
					.set("id", company.getId())
					.execute(Accumulators.getFirstUnique());
			// but relation is cut on both sides (because setCapital(..) calls setCountry(..))
			assertThat(liteCompany).usingRecursiveComparison().isEqualTo(new LiteCompany("World Company"));
			
			// from null to a (new) object
			referentDevice = devicePersister.select(referentDevice.getId());
			Company stalactiteCorp = new Company(companyIdProvider.giveNewIdentifier());
			stalactiteCorp.setName("Stalactite Corp");
			modifiedDevice.setManufacturer(stalactiteCorp);
			devicePersister.update(modifiedDevice, referentDevice, false);
			modifiedDevice = devicePersister.select(referentDevice.getId());
			assertThat(modifiedDevice.getManufacturer()).usingRecursiveComparison().isEqualTo(stalactiteCorp);
			// ensuring that capital was not deleted nor updated
			assertThat(companySelector
					.set("id", stalactiteCorp.getId())
					.execute(Accumulators.getFirstUnique())).usingRecursiveComparison().isEqualTo(new LiteCompany("Stalactite Corp"));
			
			// testing update cascade
			referentDevice = devicePersister.select(referentDevice.getId());
			modifiedDevice.getManufacturer().setName("Skynet");
			devicePersister.update(modifiedDevice, referentDevice, false);
			modifiedDevice = devicePersister.select(referentDevice.getId());
			// ensuring that capital was not deleted nor updated
			assertThat(companySelector
					.set("id", stalactiteCorp.getId())
					.execute(Accumulators.getFirstUnique())).usingRecursiveComparison().isEqualTo(new LiteCompany("Skynet"));
			
			// testing delete cascade
			devicePersister.delete(modifiedDevice);
			// ensuring that capital was not deleted nor updated
			assertThat(companySelector
					.set("id", stalactiteCorp.getId())
					.execute(Accumulators.getFirstUnique())).usingRecursiveComparison().isEqualTo(new LiteCompany("Skynet"));
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
					.mapManyToOne(Device::getLocation, addressConfiguration)
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
					.mapManyToOne(Device::getLocation, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE)
							.mapKey(Address::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Address::getStreet), new Table<>("Township"))
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
					.mapManyToOne(Device::getLocation, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE, new Table<>("Town"))
							.mapKey(Address::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Address::getStreet))
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
					.mapManyToOne(Device::getLocation, MappingEase.entityBuilder(Address.class, Identifier.LONG_TYPE, new Table<>("Town"))
							.mapKey(Address::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Address::getStreet), new Table<>("Township"))
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
	
	@Test
	void multiple_manyToOne() throws SQLException {
		EntityPersister<Device, Identifier<Long>> devicePersister = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
				.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Device::getName)
				.mapManyToOne(Device::setManufacturer, companyConfiguration).cascading(ALL)
				.mapManyToOne(Device::setLocation, addressConfiguration).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider deviceIdProvider = new LongProvider();
		Device dummyDevice = new Device(deviceIdProvider.giveNewIdentifier());
		dummyDevice.setName("UPS");
		
		Company company = new Company(new LongProvider().giveNewIdentifier());
		company.setName("World Company");
		dummyDevice.setManufacturer(company);
		
		Address somewhere = new Address(new LongProvider().giveNewIdentifier());
		somewhere.setStreet("somewhere");
		dummyDevice.setLocation(somewhere);
		
		// testing insert cascade
		devicePersister.insert(dummyDevice);
		Device persistedDevice = devicePersister.select(dummyDevice.getId());
		assertThat(persistedDevice.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedDevice.getManufacturer().getName()).isEqualTo("World Company");
		assertThat(((Address) persistedDevice.getLocation()).getStreet()).isEqualTo("somewhere");
		assertThat(persistedDevice.getManufacturer().getId().isPersisted()).isTrue();
		assertThat(persistedDevice.getLocation().getId().isPersisted()).isTrue();
		
		// testing insert cascade with another Device reusing ManyToOne entities
		Device dummyDevice2 = new Device(deviceIdProvider.giveNewIdentifier());
		dummyDevice2.setName("France 2");
		dummyDevice2.setManufacturer(company);
		dummyDevice2.setLocation(somewhere);
		devicePersister.insert(dummyDevice2);
		// database must be up to date
		Device persistedDevice2 = devicePersister.select(dummyDevice2.getId());
		assertThat(persistedDevice2.getId()).isEqualTo(new PersistedIdentifier<>(1L));
		assertThat(persistedDevice2.getManufacturer().getName()).isEqualTo("World Company");
		assertThat(persistedDevice2.getManufacturer().getId().getDelegate()).isEqualTo(persistedDevice.getManufacturer().getId().getDelegate());
		assertThat(persistedDevice2.getLocation().getId().getDelegate()).isEqualTo(persistedDevice.getLocation().getId().getDelegate());
		assertThat(persistedDevice2.getManufacturer()).isNotSameAs(persistedDevice.getManufacturer());
		assertThat(persistedDevice2.getLocation()).isNotSameAs(persistedDevice.getLocation());
		
		// testing update cascade
		persistedDevice2.getManufacturer().setName("World Company renamed");
		((Address) persistedDevice2.getLocation()).setStreet("somewhere renamed");
		devicePersister.update(persistedDevice2, dummyDevice2, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from Company");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("World Company renamed");
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select street from Address");
		resultSet.next();
		assertThat(resultSet.getString("street")).isEqualTo("somewhere renamed");
		assertThat(resultSet.next()).isFalse();
		
		// testing delete cascade
		// but we have to remove first the other device that points to the same manufacturer, else will get a constraint violation
		assertThat(persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate(
				"update Device set manufacturerId = null, locationId = null where id = " + dummyDevice2.getId().getDelegate())).isEqualTo(1);
		devicePersister.delete(persistedDevice);
		// database must be up to date
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Device where id = " + persistedDevice.getId().getDelegate());
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Company where id = " + persistedDevice.getManufacturer().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Address where id = " + persistedDevice.getLocation().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
	}
	
	@Test
	void multiple_manyToOne_partialOrphanRemoval() throws SQLException {
		EntityPersister<Device, Identifier<Long>> devicePersister = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
				.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Device::getName)
				.mapManyToOne(Device::setManufacturer, companyConfiguration).cascading(ALL_ORPHAN_REMOVAL)
				.mapManyToOne(Device::setLocation, addressConfiguration).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider deviceIdProvider = new LongProvider();
		Device dummyDevice = new Device(deviceIdProvider.giveNewIdentifier());
		dummyDevice.setName("UPS");
		
		Company company = new Company(new LongProvider().giveNewIdentifier());
		company.setName("World Company");
		dummyDevice.setManufacturer(company);
		
		Address somewhere = new Address(new LongProvider().giveNewIdentifier());
		somewhere.setStreet("somewhere");
		dummyDevice.setLocation(somewhere);
		
		// testing insert cascade
		devicePersister.insert(dummyDevice);
		Device persistedDevice = devicePersister.select(dummyDevice.getId());
		assertThat(persistedDevice.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedDevice.getManufacturer().getName()).isEqualTo("World Company");
		assertThat(((Address) persistedDevice.getLocation()).getStreet()).isEqualTo("somewhere");
		assertThat(persistedDevice.getManufacturer().getId().isPersisted()).isTrue();
		assertThat(persistedDevice.getLocation().getId().isPersisted()).isTrue();
		
		// testing insert cascade with another Device reusing ManyToOne entities
		Device dummyDevice2 = new Device(deviceIdProvider.giveNewIdentifier());
		dummyDevice2.setName("UPS 2");
		dummyDevice2.setManufacturer(company);
		dummyDevice2.setLocation(somewhere);
		devicePersister.insert(dummyDevice2);
		// database must be up to date
		Device persistedDevice2 = devicePersister.select(dummyDevice2.getId());
		assertThat(persistedDevice2.getId()).isEqualTo(new PersistedIdentifier<>(1L));
		assertThat(persistedDevice2.getManufacturer().getName()).isEqualTo("World Company");
		assertThat(persistedDevice2.getManufacturer().getId().getDelegate()).isEqualTo(persistedDevice.getManufacturer().getId().getDelegate());
		assertThat(persistedDevice2.getLocation().getId().getDelegate()).isEqualTo(persistedDevice.getLocation().getId().getDelegate());
		assertThat(persistedDevice2.getManufacturer()).isNotSameAs(persistedDevice.getManufacturer());
		assertThat(persistedDevice2.getLocation()).isNotSameAs(persistedDevice.getLocation());
		
		// testing update cascade
		// but we have to remove first the other device that points to the same manufacturer, else will get a constraint violation
		assertThat(persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate(
				"update Device set manufacturerId = null, locationId = null where id = " + dummyDevice.getId().getDelegate())).isEqualTo(1);
		persistedDevice2.setManufacturer(null);
		((Address) persistedDevice2.getLocation()).setStreet("somewhere renamed");
		devicePersister.update(persistedDevice2, dummyDevice2, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from Company");
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select street from Address");
		resultSet.next();
		assertThat(resultSet.getString("street")).isEqualTo("somewhere renamed");
		assertThat(resultSet.next()).isFalse();
		
		// testing delete cascade
		devicePersister.delete(persistedDevice2);
		// database must be up to date
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Device where id = " + persistedDevice2.getId().getDelegate());
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Company where id = " + dummyDevice2.getManufacturer().getId().getDelegate());
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Address where id = " + persistedDevice2.getLocation().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
	}
	
	@Nested
	class BiDirectionality {
		
		@Test
		void reverseCollection() {
			EntityPersister<Device, Identifier<Long>> devicePersister = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					.mapManyToOne(Device::setManufacturer, companyConfiguration).cascading(ALL).reverseCollection(Company::getDevices)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			LongProvider deviceIdProvider = new LongProvider();
			Device device1 = new Device(deviceIdProvider.giveNewIdentifier());
			device1.setName("device 1");
			Device device2 = new Device(deviceIdProvider.giveNewIdentifier());
			device2.setName("device 2");
			
			Company company = new Company(new LongProvider().giveNewIdentifier());
			company.setName("World Company");
			device1.setManufacturer(company);
			company.addDevice(device1);
			device2.setManufacturer(company);
			company.addDevice(device2);
			
			devicePersister.insert(device1);
			devicePersister.insert(device2);
			
			Set<Device> select = devicePersister.select(Arrays.asSet(device1.getId(), device2.getId()));
			Device loadedDevice1 = Iterables.find(select, Device::getName, "device 1"::equals).getLeft();
			Device loadedDevice2 = Iterables.find(select, Device::getName, "device 2"::equals).getLeft();
			assertThat(loadedDevice1.getManufacturer().getDevices()).containsExactlyInAnyOrder(loadedDevice1, loadedDevice2);
		}
		
		@Test
		void reverselySetBy() {
			EntityPersister<Device, Identifier<Long>> devicePersister = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					.mapManyToOne(Device::setManufacturer, companyConfiguration).cascading(ALL).reverselySetBy(Company::addDevice)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			LongProvider deviceIdProvider = new LongProvider();
			Device device1 = new Device(deviceIdProvider.giveNewIdentifier());
			device1.setName("device 1");
			Device device2 = new Device(deviceIdProvider.giveNewIdentifier());
			device2.setName("device 2");
			
			Company company = new Company(new LongProvider().giveNewIdentifier());
			company.setName("World Company");
			device1.setManufacturer(company);
			company.addDevice(device1);
			device2.setManufacturer(company);
			company.addDevice(device2);
			
			devicePersister.insert(device1);
			devicePersister.insert(device2);
			
			Set<Device> select = devicePersister.select(Arrays.asSet(device1.getId(), device2.getId()));
			Device loadedDevice1 = Iterables.find(select, Device::getName, "device 1"::equals).getLeft();
			Device loadedDevice2 = Iterables.find(select, Device::getName, "device 2"::equals).getLeft();
			assertThat(loadedDevice1.getManufacturer().getDevices()).containsExactlyInAnyOrder(loadedDevice1, loadedDevice2);
		}
		
		@Test
		void reverselyInitializeWith() {
			EntityPersister<Device, Identifier<Long>> devicePersister = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Device::getName)
					.mapManyToOne(Device::setManufacturer, companyConfiguration).cascading(ALL)
						.reverseCollection(Company::getDevices)
						.reverselyInitializeWith(LinkedHashSet::new)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			LongProvider deviceIdProvider = new LongProvider();
			Device device1 = new Device(deviceIdProvider.giveNewIdentifier());
			device1.setName("device 1");
			Device device2 = new Device(deviceIdProvider.giveNewIdentifier());
			device2.setName("device 2");
			
			Company company = new Company(new LongProvider().giveNewIdentifier());
			company.setName("World Company");
			device1.setManufacturer(company);
			company.addDevice(device1);
			device2.setManufacturer(company);
			company.addDevice(device2);
			
			devicePersister.insert(device1);
			devicePersister.insert(device2);
			
			Set<Device> select = devicePersister.select(Arrays.asSet(device1.getId(), device2.getId()));
			Device loadedDevice1 = Iterables.find(select, Device::getName, "device 1"::equals).getLeft();
			Device loadedDevice2 = Iterables.find(select, Device::getName, "device 2"::equals).getLeft();
			assertThat(loadedDevice1.getManufacturer().getDevices()).containsExactlyInAnyOrder(loadedDevice1, loadedDevice2);
			assertThat(loadedDevice1.getManufacturer().getDevices()).isInstanceOf(LinkedHashSet.class);
		}
	}
	
	@Test
	void mandatory_withNullTarget_throwsException() {
		EntityPersister<Device, Identifier<Long>> devicePersister = MappingEase.entityBuilder(Device.class, Identifier.LONG_TYPE)
				.mapKey(Device::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Device::getName)
				// no cascade definition
				.mapManyToOne(Device::getManufacturer, companyConfiguration).cascading(ALL).mandatory()
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// asserting null check at insertion time
		Device dummyDevice = new Device(new PersistableIdentifier<>(42L));
		dummyDevice.setManufacturer(null);
		assertThatExceptionOfType(RuntimeMappingException.class).as("Non null value expected for relation o.c.s.e.m.d.Device o.c.s.e.m"
				+ ".Device.getManufacturer() on object Device@0").isThrownBy(() -> devicePersister.insert(dummyDevice));
		
		// completing the relation for update test hereafter
		Company company = new Company(new PersistableIdentifier<>(1L));
		dummyDevice.setManufacturer(company);
		devicePersister.insert(dummyDevice);
		
		// asserting null check at update time
		dummyDevice.setManufacturer(null);
		assertThatExceptionOfType(RuntimeMappingException.class).as("Non null value expected for relation o.c.s.e.m.d.Device o.c.s.e.m"
				+ ".Device.getManufacturer() on object Device@0").isThrownBy(() -> devicePersister.update(dummyDevice));
	}
	
	public static class LiteCompany {
		private final String name;
		
		public LiteCompany(String name) {
			this.name = name;
		}
	}
}
