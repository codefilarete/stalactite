package org.gama.stalactite.persistence.engine;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.builder.SelectQueryBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.tngtech.java.junit.dataprovider.DataProviders.$;
import static com.tngtech.java.junit.dataprovider.DataProviders.$$;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class JoinedStrategiesSelectTest {
	
	private static ClassMappingStrategy buildMockMappingStrategy(String tableName) {
		return buildMockMappingStrategy(new Table(tableName));
	}
	
	private static ClassMappingStrategy buildMockMappingStrategy(Table table) {
		ClassMappingStrategy mappingStrategyMock = mock(ClassMappingStrategy.class);
		when(mappingStrategyMock.getTargetTable()).thenReturn(table);
		// the selected columns are plugged on the table ones
		when(mappingStrategyMock.getSelectableColumns()).thenAnswer(invocation -> table.getColumns().asSet());
		return mappingStrategyMock;
	}
	
	// TODO: tester avec les stratégies bizarres

	@DataProvider
	public static Object[][] testToSQL_singleStrategyData() {
		return $$(
				$(
						new Table("Toto").new Column("id", long.class).getTable(),
						"select Toto.id as Toto_id from Toto"
				),
				$(
						new Table("Toto").new Column("id", long.class).getTable().new Column("name", String.class).getTable(),
						"select Toto.id as Toto_id, Toto.name as Toto_name from Toto"
				)
		);
	}
	
	@Test
	@UseDataProvider(value = "testToSQL_singleStrategyData")
	public void testToSQL_singleStrategy(Table table, String expected) {
		ClassMappingStrategy mappingStrategyMock = buildMockMappingStrategy(table);
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect<>(mappingStrategyMock, c -> null);
		SelectQueryBuilder queryBuilder = new SelectQueryBuilder(testInstance.buildSelectQuery());
		assertEquals(expected, queryBuilder.toSQL());
	}
	
	private static Object[] inheritance_tablePerClass_2Classes_testData() {
		ClassMappingStrategy totoMappingMock = buildMockMappingStrategy("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.new Column("id", long.class);
		// column for "noise" in select
		totoTable.new Column("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMockMappingStrategy("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.new Column("id", long.class);
		
		return $( // kind of inheritance "table per class" mapping
				totoMappingMock, tataMappingMock, totoPrimaryKey, tataPrimaryKey,
				"select Toto.id as Toto_id, Toto.name as Toto_name, Tata.id as Tata_id from Toto inner join Tata on Toto.id = Tata.id"
		);
	}
	
	@DataProvider
	public static Object[][] dataToSQL_multipleStrategy() {
		ClassMappingStrategy totoMappingMock = buildMockMappingStrategy("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		totoTable.new Column("id", long.class);
		// column for "noise" in select
		totoTable.new Column("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMockMappingStrategy("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.new Column("id", long.class);
		Column totoOtherTableId = totoTable.new Column("otherTable_id", long.class);
		
		Table tata2Table = new Table("Tata");
		tata2Table.new Column("id", long.class);
		buildMockMappingStrategy(tata2Table);
		
		return $$(
				inheritance_tablePerClass_2Classes_testData(),
				$(
						totoMappingMock, tataMappingMock, totoOtherTableId, tataPrimaryKey,
						"select Toto.id as Toto_id, Toto.name as Toto_name, Toto.otherTable_id as Toto_otherTable_id,"
								+ " Tata.id as Tata_id from Toto inner join Tata on Toto.otherTable_id = Tata.id")
		);
	}
	
	@Test
	@UseDataProvider
	public void testToSQL_multipleStrategy(ClassMappingStrategy rootMappingStrategy, ClassMappingStrategy classMappingStrategy,
										   Column leftJoinColumn, Column rightJoinColumn,
										   String expected) {
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(rootMappingStrategy, c -> null);
		testInstance.add(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, classMappingStrategy, leftJoinColumn, rightJoinColumn, (a, b) -> {});
		SelectQueryBuilder queryBuilder = new SelectQueryBuilder(testInstance.buildSelectQuery());
		assertEquals(expected, queryBuilder.toSQL());
	}
	
	@Test
	public void testInheritance_tablePerClass_3Classes() {
		ClassMappingStrategy totoMappingMock = buildMockMappingStrategy("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.new Column("id", long.class);
		// column for "noise" in select
		totoTable.new Column("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMockMappingStrategy("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.new Column("id", long.class);
		// column for "noise" in select
		tataTable.new Column("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMockMappingStrategy("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.new Column("id", long.class);
		// column for "noise" in select
		tutuTable.new Column("name", String.class);
		
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(totoMappingMock, c -> null);
		String tataAddKey = testInstance.add(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, tataMappingMock, totoPrimaryKey, tataPrimaryKey, (a, b) -> {});
		testInstance.add(tataAddKey, tutuMappingMock, tataPrimaryKey, tutuPrimaryKey, (a, b) -> {});
		SelectQueryBuilder queryBuilder = new SelectQueryBuilder(testInstance.buildSelectQuery());
		assertEquals("select"
						+ " Toto.id as Toto_id, Toto.name as Toto_name"
						+ ", Tata.id as Tata_id, Tata.name as Tata_name"
						+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
						+ " from Toto"
						+ " inner join Tata on Toto.id = Tata.id"
						+ " inner join Tutu on Tata.id = Tutu.id"
				, queryBuilder.toSQL());
	}
	
	@Test
	public void testJoin_2Relations() {
		ClassMappingStrategy totoMappingMock = buildMockMappingStrategy("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.new Column("id", long.class);
		// column for "noise" in select
		totoTable.new Column("name", String.class);
		Column tataId = totoTable.new Column("tataId", String.class);
		Column tutuId = totoTable.new Column("tutuId", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMockMappingStrategy("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.new Column("id", long.class);
		// column for "noise" in select
		tataTable.new Column("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMockMappingStrategy("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.new Column("id", long.class);
		// column for "noise" in select
		tutuTable.new Column("name", String.class);
		
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(totoMappingMock, c -> null);
		String tataAddKey = testInstance.add(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, tataMappingMock, tataId, tataPrimaryKey, (a, b) -> {});
		testInstance.add(tataAddKey, tutuMappingMock, tutuId, tutuPrimaryKey, true, (a, b) -> {});
		SelectQueryBuilder queryBuilder = new SelectQueryBuilder(testInstance.buildSelectQuery());
		assertEquals("select"
						+ " Toto.id as Toto_id, Toto.name as Toto_name, Toto.tataId as Toto_tataId, Toto.tutuId as Toto_tutuId"
						+ ", Tata.id as Tata_id, Tata.name as Tata_name"
						+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
						+ " from Toto"
						+ " inner join Tata on Toto.tataId = Tata.id"
						+ " left outer join Tutu on Toto.tutuId = Tutu.id"
				, queryBuilder.toSQL());
	}
	
	@Test
	public void testInheritance_tablePerClass_NClasses() {
		ClassMappingStrategy totoMappingMock = buildMockMappingStrategy("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.new Column("id", long.class);
		// column for "noise" in select
		totoTable.new Column("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMockMappingStrategy("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.new Column("id", long.class);
		// column for "noise" in select
		tataTable.new Column("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMockMappingStrategy("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.new Column("id", long.class);
		// column for "noise" in select
		tutuTable.new Column("name", String.class);
		
		ClassMappingStrategy titiMappingMock = buildMockMappingStrategy("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		Column titiPrimaryKey = titiTable.new Column("id", long.class);
		// column for "noise" in select
		titiTable.new Column("name", String.class);
		
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(totoMappingMock, c -> null);
		String tataAddKey = testInstance.add(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, tataMappingMock, totoPrimaryKey, tataPrimaryKey, (a, b) -> {});
		String tutuAddKey = testInstance.add(tataAddKey, tutuMappingMock, tataPrimaryKey, tutuPrimaryKey, (a, b) -> {});
		String titiAddKey = testInstance.add(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, titiMappingMock, tataPrimaryKey, titiPrimaryKey, (a, b) -> {});
		SelectQueryBuilder queryBuilder = new SelectQueryBuilder(testInstance.buildSelectQuery());
		assertEquals("select"
						+ " Toto.id as Toto_id, Toto.name as Toto_name"
						+ ", Tata.id as Tata_id, Tata.name as Tata_name"
						+ ", Titi.id as Titi_id, Titi.name as Titi_name"
						+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
						+ " from Toto"
						+ " inner join Tata on Toto.id = Tata.id"
						+ " inner join Titi on Tata.id = Titi.id"
						+ " inner join Tutu on Tata.id = Tutu.id"
				, queryBuilder.toSQL());
	}
	
}