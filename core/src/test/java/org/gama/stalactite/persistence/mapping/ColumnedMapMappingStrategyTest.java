package org.gama.stalactite.persistence.mapping;

import org.gama.lang.collection.Maps;
import org.gama.lang.collection.Maps.ChainingMap;
import org.gama.stalactite.persistence.sql.result.Row;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ColumnedMapMappingStrategyTest {
	
	private static final String GET_INSERT_VALUES_DATA = "testGetInsertValuesData";
	private static final String GET_UPDATE_VALUES_DIFF_ONLY_DATA = "testGetUpdateValuesDiffOnlyData";
	private static final String GET_UPDATE_VALUES_ALL_COLUMNS_DATA = "testGetUpdateValuesAllColumnsData";
	
	private Table totoTable;
	private ColumnedMapMappingStrategy<Map<Integer, String>, Integer, String, String> testInstance;
	private Column col1;
	private Column col2;
	private Column col3;
	private Column col4;
	private Column col5;
	
	@BeforeTest
	public void setUp() throws Exception {
		totoTable = new Table(null, "Toto");
		final int nbCol = 5;
		final Map<Integer, Column> mappedColumnsOnKey = new HashMap<>();
		final Map<Column, Integer> mappedKeyOnColumn = new HashMap<>();
		for (int i = 1; i <= nbCol; i++) {
			String columnName = "col_" + i;
			Column column = totoTable.new Column(columnName, String.class);
			mappedColumnsOnKey.put(i, column);
			mappedKeyOnColumn.put(column, i);
		}
		
		testInstance = new ColumnedMapMappingStrategy<Map<Integer, String>, Integer, String, String>(totoTable, totoTable.getColumns().asSet(), HashMap.class) {
			@Override
			protected Column getColumn(Integer key) {
				if (key > 5) {
					throw new IllegalArgumentException("Unknown key " + key);
				}
				return mappedColumnsOnKey.get(key);
			}
			
			@Override
			protected Integer getKey(Column column) {
				return mappedKeyOnColumn.get(column);
			}
			
			@Override
			protected String toDatabaseValue(Integer key, String s) {
				return s;
			}
			
			@Override
			protected String toMapValue(Integer key, Object s) {
				return s == null ? null : s.toString();
			}
		};
		Map<String, Column> namedColumns = totoTable.mapColumnsOnName();
		col1 = namedColumns.get("col_1");
		col1.setPrimaryKey(true);
		col2 = namedColumns.get("col_2");
		col3 = namedColumns.get("col_3");
		col4 = namedColumns.get("col_4");
		col5 = namedColumns.get("col_5");
	}
	
	@DataProvider(name = GET_INSERT_VALUES_DATA)
	public Object[][] testGetInsertValuesData() throws Exception {
		return new Object[][] {
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "a").add(2, "b").add(3, null), Maps.asMap(col1, "a").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ null, Maps.asMap(col1, null).add(col2, null).add(col3, null).add(col4, null).add(col5, null) },
		};
	}
	
	@Test(dataProvider = GET_INSERT_VALUES_DATA)
	public void testGetInsertValues(ChainingMap<Integer, String> toInsert, ChainingMap<Column, String> expected) throws Exception {
		Map<Column, Object> insertValues = testInstance.getInsertValues(toInsert);
		assertEquals(insertValues, expected);
	}
	
	@DataProvider(name = GET_UPDATE_VALUES_DIFF_ONLY_DATA)
	private Object[][] testGetUpdateValues_diffOnlyData() {
		return new Object[][] {
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c") },
				{ Maps.asMap(1, "a").add(2, "b"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, null) },
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(1, "x").add(2, "y"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c") },
				{ Maps.asMap(1, "x").add(2, "b"), Maps.asMap(1, "x").add(2, "y"),
						Maps.asMap(col2, "b") },
				{ Maps.asMap(1, "x").add(2, "b"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col2, "b").add(col3, null) },
				{ Maps.asMap(1, "x").add(2, "b"), null,
						Maps.asMap(col1, "x").add(col2, "b") },
		};
	}
	
	@Test(dataProvider = GET_UPDATE_VALUES_DIFF_ONLY_DATA)
	public void testGetUpdateValues_diffOnly(HashMap<Integer, String> modified, HashMap<Integer, String> unmodified, Map<Integer, String> expected) throws Exception {
		Map<Column, Object> updateValues = testInstance.getUpdateValues(modified, unmodified, false);
		assertEquals(updateValues, expected);
	}
	
	@DataProvider(name = GET_UPDATE_VALUES_ALL_COLUMNS_DATA)
	private Object[][] testGetUpdateValues_allColumnsData() {
		return new Object[][] {
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "a").add(2, "b"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(1, "x").add(2, "y"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "x").add(2, "b"), Maps.asMap(1, "x").add(2, "y"),
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "x").add(2, "b"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "x").add(2, "b"), null,
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(1, "a").add(2, "b").add(3, "c"),
						new HashMap<>()},
		};
	}
	
	@Test(dataProvider = GET_UPDATE_VALUES_ALL_COLUMNS_DATA)
	public void testGetUpdateValues_allColumns(HashMap<Integer, String> modified, HashMap<Integer, String> unmodified, Map<Integer, String> expected) throws Exception {
		Map<Column, Object> updateValues = testInstance.getUpdateValues(modified, unmodified, true);
		assertEquals(updateValues, expected);
	}
	
	@Test
	public void testTransform() throws Exception {
		Row row = new Row();
		row.put(col1.getName(), "a");
		row.put(col2.getName(), "b");
		row.put(col3.getName(), "c");
		Map<Integer, String> toto = testInstance.transform(row);
		assertEquals(toto.get(1), "a");
		assertEquals(toto.get(2), "b");
		assertEquals(toto.get(3), "c");
		assertTrue(toto.containsKey(4));
		assertEquals(toto.get(4), null);
		assertTrue(toto.containsKey(5));
		assertEquals(toto.get(5), null);
		// there's not more element since mapping used 5 columns
		assertEquals(toto.size(), 5);
	}
}