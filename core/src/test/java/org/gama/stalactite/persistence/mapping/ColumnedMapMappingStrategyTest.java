package org.gama.stalactite.persistence.mapping;

import java.util.HashMap;
import java.util.Map;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.collection.Maps;
import org.gama.lang.collection.Maps.ChainingMap;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class ColumnedMapMappingStrategyTest {
	
	private static Table totoTable;
	private static Column col1;
	private static Column col2;
	private static Column col3;
	private static Column col4;
	private static Column col5;
	private static Map<Integer, Column> columnToKey;
	private static Map<Column, Integer> keyToColumn;
	
	@BeforeClass
	public static void setUpClass() {
		totoTable = new Table(null, "Toto");
		final int nbCol = 5;
		columnToKey = new HashMap<>();
		keyToColumn = new HashMap<>();
		for (int i = 1; i <= nbCol; i++) {
			String columnName = "col_" + i;
			Column column = totoTable.new Column(columnName, String.class);
			columnToKey.put(i, column);
			keyToColumn.put(column, i);
		}
		Map<String, Column> namedColumns = totoTable.mapColumnsOnName();
		col1 = namedColumns.get("col_1");
		col1.setPrimaryKey(true);
		col2 = namedColumns.get("col_2");
		col3 = namedColumns.get("col_3");
		col4 = namedColumns.get("col_4");
		col5 = namedColumns.get("col_5");
	}
	
	private ColumnedMapMappingStrategy<Map<Integer, String>, Integer, String, String> testInstance;
	
	@Before
	public void setUp() {
		testInstance = new ColumnedMapMappingStrategy<Map<Integer, String>, Integer, String, String>(totoTable, totoTable.getColumns().asSet(), HashMap.class) {
			@Override
			protected Column getColumn(Integer key) {
				if (key > 5) {
					throw new IllegalArgumentException("Unknown key " + key);
				}
				return columnToKey.get(key);
			}
			
			@Override
			protected Integer getKey(Column column) {
				return keyToColumn.get(column);
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
		
	}
	
	@DataProvider
	public static Object[][] testGetInsertValuesData() {
		return new Object[][] {
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "a").add(2, "b").add(3, null), Maps.asMap(col1, "a").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ null, Maps.asMap(col1, null).add(col2, null).add(col3, null).add(col4, null).add(col5, null) },
		};
	}
	
	@Test
	@UseDataProvider("testGetInsertValuesData")
	public void testGetInsertValues(ChainingMap<Integer, String> toInsert, ChainingMap<Column, String> expected) {
		Map<Column, Object> insertValues = testInstance.getInsertValues(toInsert);
		assertEquals(expected, insertValues);
	}
	
	@DataProvider
	public static Object[][] testGetUpdateValues_diffOnlyData() {
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
	
	@Test
	@UseDataProvider("testGetUpdateValues_diffOnlyData")
	public void testGetUpdateValues_diffOnly(HashMap<Integer, String> modified, HashMap<Integer, String> unmodified, Map<Column, String> expected) {
		Map<Column, Object> updateValues = testInstance.getUpdateValues(modified, unmodified, false);
		assertEquals(expected, updateValues);
	}
	
	@DataProvider
	public static Object[][] testGetUpdateValues_allColumnsData() {
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
	
	@Test
	@UseDataProvider("testGetUpdateValues_allColumnsData")
	public void testGetUpdateValues_allColumns(HashMap<Integer, String> modified, HashMap<Integer, String> unmodified, Map<Column, String> expected) {
		Map<Column, Object> updateValues = testInstance.getUpdateValues(modified, unmodified, true);
		assertEquals(expected, updateValues);
	}
	
	@Test
	public void testTransform() {
		Row row = new Row();
		row.put(col1.getName(), "a");
		row.put(col2.getName(), "b");
		row.put(col3.getName(), "c");
		Map<Integer, String> toto = testInstance.transform(row);
		assertEquals("a", toto.get(1));
		assertEquals("b", toto.get(2));
		assertEquals("c", toto.get(3));
		assertTrue(toto.containsKey(4));
		assertNull(toto.get(4));
		assertTrue(toto.containsKey(5));
		assertNull(toto.get(5));
		// there's not more element since mapping used 5 columns
		assertEquals(5, toto.size());
	}
}