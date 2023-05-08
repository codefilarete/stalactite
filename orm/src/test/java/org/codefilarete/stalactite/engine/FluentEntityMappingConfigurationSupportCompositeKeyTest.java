package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.model.compositekey.House;
import org.codefilarete.stalactite.engine.model.compositekey.House.HouseId;
import org.codefilarete.stalactite.engine.model.compositekey.Person;
import org.codefilarete.stalactite.engine.model.compositekey.Person.PersonId;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class FluentEntityMappingConfigurationSupportCompositeKeyTest {
	
	private final HSQLDBDialect dialect = new HSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();

	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
	}
	
	@Nested
	class ForeignKeyCreation {
		
		@Test
		void relationOwnedBySource() throws SQLException {
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
			Map<String, JdbcForeignKey> foreignKeyPerName = new HashMap<>();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(House.class)).getMapping().getTargetTable().getName().toUpperCase());
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
				foreignKeyPerName.compute(fkName, (k, fk) -> fk == null
						? new JdbcForeignKey(fkName, fktableName, fkcolumnName, pktableName, pkcolumnName)
						: new JdbcForeignKey(fkName, fktableName, fk.getSrcColumnName() + ", " + fkcolumnName, pktableName, fk.getTargetColumnName() + ", " + pkcolumnName));
				
			});
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_3D70E04A", "PERSON", "HOUSENUMBER, HOUSESTREET, HOUSEZIPCODE, HOUSECITY", "HOUSE", "NUMBER, STREET, ZIPCODE, CITY");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
		
		@Test
		void relationOwnedByTargetSide() throws SQLException {
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
			Map<String, JdbcForeignKey> foreignKeyPerName = new HashMap<>();
			ResultSet exportedKeysForPersonTable = currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) persistenceContext.getPersister(Person.class)).getMapping().getTargetTable().getName().toUpperCase());
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
				foreignKeyPerName.compute(fkName, (k, fk) -> fk == null
						? new JdbcForeignKey(fkName, fktableName, fkcolumnName, pktableName, pkcolumnName)
						: new JdbcForeignKey(fkName, fktableName, fk.getSrcColumnName() + ", " + fkcolumnName, pktableName, fk.getTargetColumnName() + ", " + pkcolumnName));
				
			});
			JdbcForeignKey foundForeignKey = Iterables.first(foreignKeyPerName).getValue();
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_D2936B99", "HOUSE", "OWNERFIRSTNAME, OWNERLASTNAME, OWNERADDRESS", "PERSON", "FIRSTNAME, LASTNAME, ADDRESS");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
	}
}
