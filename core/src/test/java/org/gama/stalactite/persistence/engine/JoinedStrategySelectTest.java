package org.gama.stalactite.persistence.engine;

import java.util.function.Function;

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
public class JoinedStrategySelectTest {
	
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
		JoinedStrategySelect testInstance = new JoinedStrategySelect<>(mappingStrategyMock, c -> null);
		SelectQueryBuilder queryBuilder = new SelectQueryBuilder(testInstance.buildSelectQuery());
		assertEquals(expected, queryBuilder.toSQL());
	}
	
	private static Object[] inheritance_tablePerClass_2Classes() {
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
	
	private static ClassMappingStrategy buildMockMappingStrategy(String tableName) {
		return buildMockMappingStrategy(new Table(tableName));
	}
	
	private static ClassMappingStrategy buildMockMappingStrategy(Table table) {
		ClassMappingStrategy mappingStrategyMock = mock(ClassMappingStrategy.class);
		when(mappingStrategyMock.getTargetTable()).thenReturn(table);
		return mappingStrategyMock;
	}
	
	@DataProvider
	public static Object[][] testToSQL_multipleStrategyData() {
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
				inheritance_tablePerClass_2Classes(),
				$(
						totoMappingMock, tataMappingMock, totoOtherTableId, tataPrimaryKey,
						"select Toto.id as Toto_id, Toto.name as Toto_name, Toto.otherTable_id as Toto_otherTable_id," +
								" Tata.id as Tata_id from Toto inner join Tata on Toto.otherTable_id = Tata.id")
		);
	}
	
	@Test
	@UseDataProvider(value = "testToSQL_multipleStrategyData")
	public void testToSQL_multipleStrategy(ClassMappingStrategy rootMappingStrategy, ClassMappingStrategy classMappingStrategy,
										   Column leftJoinColumn, Column rightJoinColumn,
										   String expected) {
		JoinedStrategySelect testInstance = new JoinedStrategySelect(rootMappingStrategy, c -> null);
		testInstance.add(JoinedStrategySelect.FIRST_STRATEGY_NAME, classMappingStrategy, leftJoinColumn, rightJoinColumn, Function.identity());
		SelectQueryBuilder queryBuilder = new SelectQueryBuilder(testInstance.buildSelectQuery());
		assertEquals(expected, queryBuilder.toSQL());
	}
}