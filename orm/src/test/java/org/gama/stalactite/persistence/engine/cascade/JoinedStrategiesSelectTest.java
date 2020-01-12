package org.gama.stalactite.persistence.engine.cascade;

import java.util.Map;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.gama.stalactite.persistence.engine.cascade.AbstractJoin.JoinType.INNER;
import static org.gama.stalactite.persistence.engine.cascade.AbstractJoin.JoinType.OUTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class JoinedStrategiesSelectTest {
	
	static ClassMappingStrategy buildMappingStrategyMock(String tableName) {
		return buildMappingStrategyMock(new Table(tableName));
	}
	
	static ClassMappingStrategy buildMappingStrategyMock(Table table) {
		ClassMappingStrategy mappingStrategyMock = mock(ClassMappingStrategy.class);
		when(mappingStrategyMock.getTargetTable()).thenReturn(table);
		// the selected columns are plugged on the table ones
		when(mappingStrategyMock.getSelectableColumns()).thenAnswer(invocation -> table.getColumns());
		return mappingStrategyMock;
	}
	
	public static Object[][] testToSQL_singleStrategyData() {
		Table table1 = new Table("Toto");
		Column id1 = table1.addColumn("id", long.class);
		Table table2 = new Table("Toto");
		Column id2 = table2.addColumn("id", long.class);
		Column name2 = table2.addColumn("name", String.class);
		
		return new Object[][] {
				{ table1, "select Toto.id as Toto_id from Toto", Maps.asMap(id1, "Toto_id") },
				{ table2, "select Toto.id as Toto_id, Toto.name as Toto_name from Toto", Maps.asMap(id2, "Toto_id").add(name2, "Toto_name") }
		};
	}
	
	@ParameterizedTest
	@MethodSource("testToSQL_singleStrategyData")
	public void testToSQL_singleStrategy(Table table, String expected, Map<Column, String> expectedAliases) {
		ClassMappingStrategy mappingStrategyMock = buildMappingStrategyMock(table);
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect<>(mappingStrategyMock, c -> mock(ParameterBinder.class));
		SQLQueryBuilder SQLQueryBuilder = new SQLQueryBuilder(testInstance.buildSelectQuery());
		assertEquals(expected, SQLQueryBuilder.toSQL());
		assertEquals(expectedAliases, testInstance.getAliases());
	}
	
	@Test
	public void testAdd_targetStrategyDoesntExist_throwsException() {
		ClassMappingStrategy mappingStrategyMock = buildMappingStrategyMock(new Table("toto"));
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect<>(mappingStrategyMock, c -> mock(ParameterBinder.class));
		IllegalArgumentException thrownException = assertThrows(IllegalArgumentException.class, () -> {
			// we don't care about other arguments (null passed) because existing strategy name is checked first
			testInstance.addRelationJoin("XX", null, null, null, OUTER, null);
		});
		assertEquals("No strategy with name XX exists to add a new strategy on", thrownException.getMessage());
	}
	
	private static Object[] inheritance_tablePerClass_2Classes_testData() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		
		return new Object[] { // kind of inheritance "table per class" mapping
				totoMappingMock, tataMappingMock, totoPrimaryKey, tataPrimaryKey,
				"select Toto.id as Toto_id, Toto.name as Toto_name, Tata.id as Tata_id from Toto inner join Tata on Toto.id = Tata.id"
		};
	}
	
	public static Object[][] dataToSQL_multipleStrategy() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		totoTable.addColumn("id", long.class);
		// column for "noise" in select
		totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		Column totoOtherTableId = totoTable.addColumn("otherTable_id", long.class);
		
		Table tata2Table = new Table("Tata");
		tata2Table.addColumn("id", long.class);
		buildMappingStrategyMock(tata2Table);
		
		return new Object[][] {
				inheritance_tablePerClass_2Classes_testData(),
				new Object[] {
						totoMappingMock, tataMappingMock, totoOtherTableId, tataPrimaryKey,
						"select Toto.id as Toto_id, Toto.name as Toto_name, Toto.otherTable_id as Toto_otherTable_id,"
								+ " Tata.id as Tata_id from Toto inner join Tata on Toto.otherTable_id = Tata.id" }
		};
	}
	
	@ParameterizedTest
	@MethodSource("dataToSQL_multipleStrategy")
	public void testToSQL_multipleStrategy(ClassMappingStrategy rootMappingStrategy, ClassMappingStrategy classMappingStrategy,
										   Column leftJoinColumn, Column rightJoinColumn,
										   String expected) {
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(rootMappingStrategy, c -> mock(ParameterBinder.class));
		testInstance.addRelationJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, classMappingStrategy, leftJoinColumn, rightJoinColumn, INNER, null);
		SQLQueryBuilder SQLQueryBuilder = new SQLQueryBuilder(testInstance.buildSelectQuery());
		assertEquals(expected, SQLQueryBuilder.toSQL());
	}
	
	@Test
	public void testInheritance_tablePerClass_3Classes() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(totoMappingMock, c -> mock(ParameterBinder.class));
		String tataAddKey = testInstance.addRelationJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, tataMappingMock, totoPrimaryKey, tataPrimaryKey, INNER, null);
		testInstance.addRelationJoin(tataAddKey, tutuMappingMock, tataPrimaryKey, tutuPrimaryKey, INNER, null);
		SQLQueryBuilder SQLQueryBuilder = new SQLQueryBuilder(testInstance.buildSelectQuery());
		assertEquals("select"
						+ " Toto.id as Toto_id, Toto.name as Toto_name"
						+ ", Tata.id as Tata_id, Tata.name as Tata_name"
						+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
						+ " from Toto"
						+ " inner join Tata on Toto.id = Tata.id"
						+ " inner join Tutu on Tata.id = Tutu.id"
				, SQLQueryBuilder.toSQL());
		assertEquals(Maps.asMap(totoPrimaryKey, "Toto_id")
				.add(totoNameColumn, "Toto_name")
				.add(tataPrimaryKey, "Tata_id")
				.add(tataNameColumn, "Tata_name")
				.add(tutuPrimaryKey, "Tutu_id")
				.add(tutuNameColumn, "Tutu_name"),
				testInstance.getAliases());
	}
	
	@Test
	public void testJoin_2Relations() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		Column tataId = totoTable.addColumn("tataId", String.class);
		Column tutuId = totoTable.addColumn("tutuId", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(totoMappingMock, c -> mock(ParameterBinder.class));
		String tataAddKey = testInstance.addRelationJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, tataMappingMock, tataId, tataPrimaryKey, INNER, null);
		testInstance.addRelationJoin(tataAddKey, tutuMappingMock, tutuId, tutuPrimaryKey, OUTER, null);
		SQLQueryBuilder SQLQueryBuilder = new SQLQueryBuilder(testInstance.buildSelectQuery());
		assertEquals("select"
						+ " Toto.id as Toto_id, Toto.name as Toto_name, Toto.tataId as Toto_tataId, Toto.tutuId as Toto_tutuId"
						+ ", Tata.id as Tata_id, Tata.name as Tata_name"
						+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
						+ " from Toto"
						+ " inner join Tata on Toto.tataId = Tata.id"
						+ " left outer join Tutu on Toto.tutuId = Tutu.id"
				, SQLQueryBuilder.toSQL());
		assertEquals(Maps.asMap(totoPrimaryKey, "Toto_id")
						.add(totoNameColumn, "Toto_name")
						.add(tataId, "Toto_tataId")
						.add(tutuId, "Toto_tutuId")
						.add(tataPrimaryKey, "Tata_id")
						.add(tataNameColumn, "Tata_name")
						.add(tutuPrimaryKey, "Tutu_id")
						.add(tutuNameColumn, "Tutu_name"),
				testInstance.getAliases());
	}
	
	@Test
	public void testInheritance_tablePerClass_NClasses() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		ClassMappingStrategy titiMappingMock = buildMappingStrategyMock("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		Column titiPrimaryKey = titiTable.addColumn("id", long.class);
		// column for "noise" in select
		Column titiNameColumn = titiTable.addColumn("name", String.class);
		
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(totoMappingMock, c -> mock(ParameterBinder.class));
		String tataAddKey = testInstance.addRelationJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, tataMappingMock, totoPrimaryKey, tataPrimaryKey, INNER, null);
		String tutuAddKey = testInstance.addRelationJoin(tataAddKey, tutuMappingMock, tataPrimaryKey, tutuPrimaryKey, INNER, null);
		String titiAddKey = testInstance.addRelationJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, titiMappingMock, totoPrimaryKey, titiPrimaryKey, INNER, null);
		SQLQueryBuilder SQLQueryBuilder = new SQLQueryBuilder(testInstance.buildSelectQuery());
		assertEquals("select"
						+ " Toto.id as Toto_id, Toto.name as Toto_name"
						+ ", Tata.id as Tata_id, Tata.name as Tata_name"
						+ ", Titi.id as Titi_id, Titi.name as Titi_name"
						+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
						+ " from Toto"
						+ " inner join Tata on Toto.id = Tata.id"
						+ " inner join Titi on Toto.id = Titi.id"
						+ " inner join Tutu on Tata.id = Tutu.id"
				, SQLQueryBuilder.toSQL());
		assertEquals(Maps.asMap(totoPrimaryKey, "Toto_id")
						.add(totoNameColumn, "Toto_name")
						.add(tataPrimaryKey, "Tata_id")
						.add(tataNameColumn, "Tata_name")
						.add(tutuPrimaryKey, "Tutu_id")
						.add(tutuNameColumn, "Tutu_name")
						.add(titiPrimaryKey, "Titi_id")
						.add(titiNameColumn, "Titi_name"),
				testInstance.getAliases());
	}
	
	@Test
	public void giveTables() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		
		ClassMappingStrategy tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		
		ClassMappingStrategy titiMappingMock = buildMappingStrategyMock("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		Column titiPrimaryKey = titiTable.addColumn("id", long.class);
		
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(totoMappingMock, c -> mock(ParameterBinder.class));
		String tataAddKey = testInstance.addRelationJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, tataMappingMock, totoPrimaryKey, tataPrimaryKey, INNER, null);
		String tutuAddKey = testInstance.addRelationJoin(tataAddKey, tutuMappingMock, tataPrimaryKey, tutuPrimaryKey, INNER, null);
		String titiAddKey = testInstance.addRelationJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, titiMappingMock, totoPrimaryKey, titiPrimaryKey, INNER, null);
		
		assertEquals(Arrays.asHashSet(totoTable, tataTable, tutuTable, titiTable), testInstance.giveTables());
	}
	
	@Test
	public void testCopyTo() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		ClassMappingStrategy titiMappingMock = buildMappingStrategyMock("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		Column titiPrimaryKey = titiTable.addColumn("id", long.class);
		// column for "noise" in select
		Column titiNameColumn = titiTable.addColumn("name", String.class);
		
		JoinedStrategiesSelect testInstance1 = new JoinedStrategiesSelect(totoMappingMock, c -> mock(ParameterBinder.class));
		String tataAddKey = testInstance1.addRelationJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, tataMappingMock, totoPrimaryKey, tataPrimaryKey, INNER, null);
		String tutuAddKey = testInstance1.addRelationJoin(tataAddKey, tutuMappingMock, tataPrimaryKey, tutuPrimaryKey, INNER, null);
		
		JoinedStrategiesSelect testInstance2 = new JoinedStrategiesSelect(tataMappingMock, c -> mock(ParameterBinder.class));
		String titiAddKey = testInstance2.addRelationJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, titiMappingMock, tataPrimaryKey, titiPrimaryKey, INNER, null);
		
		testInstance2.getStrategyJoins(JoinedStrategiesSelect.FIRST_STRATEGY_NAME).copyTo(testInstance1, tataAddKey);
		
		SQLQueryBuilder SQLQueryBuilder = new SQLQueryBuilder(testInstance1.buildSelectQuery());
		assertEquals("select"
						+ " Toto.id as Toto_id, Toto.name as Toto_name"
						+ ", Tata.id as Tata_id, Tata.name as Tata_name"
						+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
						+ ", Titi.id as Titi_id, Titi.name as Titi_name"
						+ " from Toto"
						+ " inner join Tata on Toto.id = Tata.id"
						+ " inner join Tutu on Tata.id = Tutu.id"
						+ " inner join Titi on Tata.id = Titi.id"
				, SQLQueryBuilder.toSQL());
		assertEquals(Maps.asMap(totoPrimaryKey, "Toto_id")
						.add(totoNameColumn, "Toto_name")
						.add(tataPrimaryKey, "Tata_id")
						.add(tataNameColumn, "Tata_name")
						.add(tutuPrimaryKey, "Tutu_id")
						.add(tutuNameColumn, "Tutu_name")
						.add(titiPrimaryKey, "Titi_id")
						.add(titiNameColumn, "Titi_name"),
				testInstance1.getAliases());
	}
}