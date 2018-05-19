package org.gama.stalactite.persistence.mapping;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

import org.gama.lang.collection.Maps;
import org.gama.sql.dml.WriteOperation;
import org.gama.sql.result.RowIterator;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.QueryBuilder;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.gama.stalactite.query.model.QueryEase.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
public class ZonedDateTimeMappingStrategyTest {
	
	private static Table targetTable;
	private static Column colA;
	private static Column colB;
	private HSQLDBDialect dialect;
	private JdbcConnectionProvider connectionProvider;
	
	@BeforeAll
	public static void setUpClass() {
		targetTable = new Table("Toto");
		colA = targetTable.addColumn("a", LocalDateTime.class);
		colB = targetTable.addColumn("b", ZoneId.class);
	}
	
	private ZonedDateTimeMappingStrategy testInstance;
	
	@BeforeEach
	public void setUp() throws SQLException {
		testInstance = new ZonedDateTimeMappingStrategy(colA, colB);
		// preparing schema
		connectionProvider = new JdbcConnectionProvider(new HSQLDBInMemoryDataSource());
		dialect = new HSQLDBDialect();
		dialect.getDdlSchemaGenerator().addTables(targetTable);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlSchemaGenerator(), connectionProvider);
		ddlDeployer.deployDDL();
	}
	
	@Test
	public void testInsertAndSelect() throws SQLException {
		// Given
		// Preparing insertion of a ZonedDateTime
		ZonedDateTime toBePersisted = ZonedDateTime.of(2018, Month.MAY.ordinal(), 16, 17, 53, 44, 00, ZoneId.of("America/Guadeloupe"));
		Map<Column, Object> insertValues = testInstance.getInsertValues(toBePersisted);
		DMLGenerator dmlGenerator = dialect.getDmlGenerator();
		ColumnParamedSQL insertOrder = dmlGenerator.buildInsert(testInstance.getColumns());
		insertOrder.setValues(insertValues);
		// execution
		WriteOperation<Column> writeOperation = new WriteOperation<>(insertOrder, connectionProvider);
		writeOperation.execute();
		
		// selecting it to see if it is correctly read
		ResultSet resultSet = connectionProvider.getCurrentConnection().prepareStatement(new QueryBuilder(select(colA, colB).from(targetTable)).toSQL()).executeQuery();
		RowIterator rowIterator = new RowIterator(resultSet,
				Maps.asMap(colA.getName(), dialect.getColumnBinderRegistry().getBinder(colA))
						.add(colB.getName(), dialect.getColumnBinderRegistry().getBinder(colB)));
		
		// Then
		assertTrue(rowIterator.hasNext());
		ZonedDateTime hydratedZonedDateTime = testInstance.transform(rowIterator.next());
		assertEquals(hydratedZonedDateTime, toBePersisted);
	}
	
	@Test
	public void testGetUpdateValues_execution() throws SQLException {
		// Given
		// Preparing insertion of a ZonedDateTime
		ZonedDateTime originalValue = ZonedDateTime.of(2018, Month.MAY.ordinal(), 16, 17, 53, 44, 00, ZoneId.of("America/Guadeloupe"));
		Map<Column, Object> insertValues = testInstance.getInsertValues(originalValue);
		DMLGenerator dmlGenerator = dialect.getDmlGenerator();
		ColumnParamedSQL insertOrder = dmlGenerator.buildInsert(testInstance.getColumns());
		insertOrder.setValues(insertValues);
		// execution
		WriteOperation<Column> writeOperation = new WriteOperation<>(insertOrder, connectionProvider);
		writeOperation.execute();
		
		ZonedDateTime modifiedZonedDateTime = originalValue.minusHours(4);
		Map<UpwhereColumn, Object> updateValues = testInstance.getUpdateValues(modifiedZonedDateTime, originalValue, true);
		// we set calue of the where clause (else SQL will update nothing or goes into error)
		updateValues.put(new UpwhereColumn(colA, false), originalValue.toLocalDateTime());
		updateValues.put(new UpwhereColumn(colB, false), originalValue.getZone());
		PreparedUpdate updateOrder = dmlGenerator.buildUpdate(testInstance.getColumns(), testInstance.getColumns());
		updateOrder.setValues(updateValues);
		// execution
		new WriteOperation<>(updateOrder, connectionProvider).execute();
		
		// selecting it to see if it is correctly read
		ResultSet resultSet = connectionProvider.getCurrentConnection().prepareStatement(new QueryBuilder(select(colA, colB).from(targetTable)).toSQL()).executeQuery();
		RowIterator rowIterator = new RowIterator(resultSet,
				Maps.asMap(colA.getName(), dialect.getColumnBinderRegistry().getBinder(colA))
						.add(colB.getName(), dialect.getColumnBinderRegistry().getBinder(colB)));
		
		// Then
		assertTrue(rowIterator.hasNext());
		ZonedDateTime hydratedZonedDateTime = testInstance.transform(rowIterator.next());
		assertEquals(hydratedZonedDateTime, modifiedZonedDateTime);
	}
	
	@Test
	public void testGetUpdateValues_nullInstance() throws SQLException {
		// Given
		// Preparing insertion of a ZonedDateTime
		ZonedDateTime originalValue = ZonedDateTime.of(2018, Month.MAY.ordinal(), 16, 17, 53, 44, 00, ZoneId.of("America/Guadeloupe"));
		Map<Column, Object> insertValues = testInstance.getInsertValues(originalValue);
		DMLGenerator dmlGenerator = dialect.getDmlGenerator();
		ColumnParamedSQL insertOrder = dmlGenerator.buildInsert(testInstance.getColumns());
		insertOrder.setValues(insertValues);
		// execution
		WriteOperation<Column> writeOperation = new WriteOperation<>(insertOrder, connectionProvider);
		writeOperation.execute();
		
		Map<UpwhereColumn, Object> updateValues = testInstance.getUpdateValues(null, originalValue, true);
		// we set calue of the where clause (else SQL will update nothing or goes into error)
		updateValues.put(new UpwhereColumn(colA, false), originalValue.toLocalDateTime());
		updateValues.put(new UpwhereColumn(colB, false), originalValue.getZone());
		PreparedUpdate updateOrder = dmlGenerator.buildUpdate(testInstance.getColumns(), testInstance.getColumns());
		updateOrder.setValues(updateValues);
		// execution
		new WriteOperation<>(updateOrder, connectionProvider).execute();
		
		// selecting it to see if it is correctly read
		ResultSet resultSet = connectionProvider.getCurrentConnection().prepareStatement(new QueryBuilder(select(colA, colB).from(targetTable)).toSQL()).executeQuery();
		RowIterator rowIterator = new RowIterator(resultSet,
				Maps.asMap(colA.getName(), dialect.getColumnBinderRegistry().getBinder(colA))
						.add(colB.getName(), dialect.getColumnBinderRegistry().getBinder(colB)));
		
		// Then
		assertTrue(rowIterator.hasNext());
		ZonedDateTime hydratedZonedDateTime = testInstance.transform(rowIterator.next());
		assertNull(hydratedZonedDateTime);
	}
	
	@Test
	public void testGetUpdateValues() {
		ZonedDateTime hydratedZonedDateTime = ZonedDateTime.of(2018, Month.MAY.ordinal(), 16, 17, 53, 44, 00, ZoneId.of("America/Guadeloupe"));
		
		// Date time update
		ZonedDateTime modifiedZonedDateTime = hydratedZonedDateTime.minusHours(4);
		Map<UpwhereColumn, Object> updateValues = testInstance.getUpdateValues(modifiedZonedDateTime, hydratedZonedDateTime, false);
		assertEquals(Maps.asMap(new UpwhereColumn(colA, true), modifiedZonedDateTime.toLocalDateTime()), updateValues);
		
		// Date time update, all columns
		modifiedZonedDateTime = hydratedZonedDateTime.minusHours(4);
		updateValues = testInstance.getUpdateValues(modifiedZonedDateTime, hydratedZonedDateTime, true);
		assertEquals(Maps.asMap(new UpwhereColumn(colA, true), (Object) modifiedZonedDateTime.toLocalDateTime())
				.add(new UpwhereColumn(colB, true), modifiedZonedDateTime.getZone()), updateValues);
		
		// Zone update
		modifiedZonedDateTime = hydratedZonedDateTime.withZoneSameLocal(ZoneId.of("Europe/Paris"));
		updateValues = testInstance.getUpdateValues(modifiedZonedDateTime, hydratedZonedDateTime, false);
		assertEquals(Maps.asMap(new UpwhereColumn(colB, true), modifiedZonedDateTime.getZone()), updateValues);
		
		// Date time and zone update
		modifiedZonedDateTime = hydratedZonedDateTime.withZoneSameInstant(ZoneId.of("Europe/Paris"));
		updateValues = testInstance.getUpdateValues(modifiedZonedDateTime, hydratedZonedDateTime, false);
		assertEquals(Maps.asMap(new UpwhereColumn(colA, true), (Object) modifiedZonedDateTime.toLocalDateTime())
				.add(new UpwhereColumn(colB, true), modifiedZonedDateTime.getZone()), updateValues);
	}
}