package org.gama.stalactite.persistence.engine.cascade;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.builder.QueryBuilder;
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
		when(mappingStrategyMock.getSelectableColumns()).thenAnswer(invocation -> table.getColumns());
		return mappingStrategyMock;
	}
	
	// TODO: tester avec les stratégies bizarres

	@DataProvider
	public static Object[][] testToSQL_singleStrategyData() {
		return $$(
				$(
						new Table("Toto").addColumn("id", long.class).getTable(),
						"select Toto.id as Toto_id from Toto"
				),
				$(
						new Table("Toto").addColumn("id", long.class).getTable().addColumn("name", String.class).getTable(),
						"select Toto.id as Toto_id, Toto.name as Toto_name from Toto"
				)
		);
	}
	
	@Test
	@UseDataProvider(value = "testToSQL_singleStrategyData")
	public void testToSQL_singleStrategy(Table table, String expected) {
		ClassMappingStrategy mappingStrategyMock = buildMockMappingStrategy(table);
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect<>(mappingStrategyMock, c -> mock(ParameterBinder.class));
		QueryBuilder queryBuilder = new QueryBuilder(testInstance.buildSelectQuery());
		assertEquals(expected, queryBuilder.toSQL());
	}
	
	private static Object[] inheritance_tablePerClass_2Classes_testData() {
		ClassMappingStrategy totoMappingMock = buildMockMappingStrategy("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMockMappingStrategy("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		
		return $( // kind of inheritance "table per class" mapping
				totoMappingMock, tataMappingMock, totoPrimaryKey, tataPrimaryKey,
				"select Toto.id as Toto_id, Toto.name as Toto_name, Tata.id as Tata_id from Toto inner join Tata on Toto.id = Tata.id"
		);
	}
	
	@DataProvider
	public static Object[][] dataToSQL_multipleStrategy() {
		ClassMappingStrategy totoMappingMock = buildMockMappingStrategy("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		totoTable.addColumn("id", long.class);
		// column for "noise" in select
		totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMockMappingStrategy("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		Column totoOtherTableId = totoTable.addColumn("otherTable_id", long.class);
		
		Table tata2Table = new Table("Tata");
		tata2Table.addColumn("id", long.class);
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
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(rootMappingStrategy, c -> mock(ParameterBinder.class));
		testInstance.add(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, classMappingStrategy, leftJoinColumn, rightJoinColumn, false, null);
		QueryBuilder queryBuilder = new QueryBuilder(testInstance.buildSelectQuery());
		assertEquals(expected, queryBuilder.toSQL());
	}
	
	@Test
	public void testInheritance_tablePerClass_3Classes() {
		ClassMappingStrategy totoMappingMock = buildMockMappingStrategy("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMockMappingStrategy("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		tataTable.addColumn("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMockMappingStrategy("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		tutuTable.addColumn("name", String.class);
		
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(totoMappingMock, c -> mock(ParameterBinder.class));
		String tataAddKey = testInstance.add(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, tataMappingMock, totoPrimaryKey, tataPrimaryKey, false, null);
		testInstance.add(tataAddKey, tutuMappingMock, tataPrimaryKey, tutuPrimaryKey, false, null);
		QueryBuilder queryBuilder = new QueryBuilder(testInstance.buildSelectQuery());
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
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		totoTable.addColumn("name", String.class);
		Column tataId = totoTable.addColumn("tataId", String.class);
		Column tutuId = totoTable.addColumn("tutuId", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMockMappingStrategy("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		tataTable.addColumn("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMockMappingStrategy("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		tutuTable.addColumn("name", String.class);
		
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(totoMappingMock, c -> mock(ParameterBinder.class));
		String tataAddKey = testInstance.add(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, tataMappingMock, tataId, tataPrimaryKey, false, null);
		testInstance.add(tataAddKey, tutuMappingMock, tutuId, tutuPrimaryKey, true, null);
		QueryBuilder queryBuilder = new QueryBuilder(testInstance.buildSelectQuery());
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
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMockMappingStrategy("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		tataTable.addColumn("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMockMappingStrategy("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		tutuTable.addColumn("name", String.class);
		
		ClassMappingStrategy titiMappingMock = buildMockMappingStrategy("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		Column titiPrimaryKey = titiTable.addColumn("id", long.class);
		// column for "noise" in select
		titiTable.addColumn("name", String.class);
		
		JoinedStrategiesSelect testInstance = new JoinedStrategiesSelect(totoMappingMock, c -> mock(ParameterBinder.class));
		String tataAddKey = testInstance.add(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, tataMappingMock, totoPrimaryKey, tataPrimaryKey, false, null);
		String tutuAddKey = testInstance.add(tataAddKey, tutuMappingMock, tataPrimaryKey, tutuPrimaryKey, false, null);
		String titiAddKey = testInstance.add(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, titiMappingMock, tataPrimaryKey, titiPrimaryKey, false, null);
		QueryBuilder queryBuilder = new QueryBuilder(testInstance.buildSelectQuery());
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