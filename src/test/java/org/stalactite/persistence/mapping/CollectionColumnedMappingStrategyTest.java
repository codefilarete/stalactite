package org.stalactite.persistence.mapping;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Maps;
import org.stalactite.lang.collection.Maps.ChainingMap;
import org.stalactite.persistence.sql.result.Row;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CollectionColumnedMappingStrategyTest {
	
	private static final String GET_INSERT_VALUES_DATA = "testGetInsertValuesData";
	private static final String GET_UPDATE_VALUES_DIFF_ONLY_DATA = "testGetUpdateValuesDiffOnlyData";
	private static final String GET_UPDATE_VALUES_ALL_COLUMNS_DATA = "testGetUpdateValuesAllColumnsData";
	
	private Table totoTable;
	private CollectionColumnedMappingStrategy<List<String>, String> testInstance;
	private Column col1;
	private Column col2;
	private Column col3;
	private Column col4;
	private Column col5;
	
	@BeforeTest
	public void setUp() throws Exception {
		totoTable = new Table(null, "Toto");
		
		testInstance = new CollectionColumnedMappingStrategy<List<String>, String>(totoTable, String.class) {
			@Override
			protected LinkedHashSet<Column> initTargetColumns() {
				int nbCol = 5;
				String columnsPrefix = "col_";
				Map<String, Column> existingColumns = getTargetTable().mapColumnsOnName();
				LinkedHashSet<Column> toReturn = new LinkedHashSet<>(nbCol, 1);
				for (int i = 1; i <= nbCol; i++) {
					String columnName = columnsPrefix + i;
					Column column = existingColumns.get(columnName);
					if (column == null) {
						column = getTargetTable().new Column(columnName, getPersistentType());
					}
					toReturn.add(column);
				}
		
				return toReturn;
			}
			
			@Override
			public List<String> transform(Row row) {
				throw new UnsupportedOperationException("Not used in this test");
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
				{ Arrays.asList("a", "b", "c"), Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Arrays.asList("a", "b", null), Maps.asMap(col1, "a").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ null, Maps.asMap(col1, null).add(col2, null).add(col3, null).add(col4, null).add(col5, null) },
		};
	}
	
	@Test(dataProvider = GET_INSERT_VALUES_DATA)
	public void testGetInsertValues(List<String> toInsert, ChainingMap<Column, String> expected) throws Exception {
		PersistentValues insertValues = testInstance.getInsertValues(toInsert);
		assertEquals(insertValues.getUpsertValues(), expected);
	}
	
	@DataProvider(name = GET_UPDATE_VALUES_DIFF_ONLY_DATA)
	private Object[][] testGetUpdateValues_diffOnlyData() {
		return new Object[][] {
				{ Arrays.asList("a", "b", "c"), Arrays.asList("x", "y", "x"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c") },
				{ Arrays.asList("a", "b"), Arrays.asList("x", "y", "x"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, null) },
				{ Arrays.asList("a", "b", "c"), Arrays.asList("x", "y"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c") },
				{ Arrays.asList("x", "b"), Arrays.asList("x", "y"),
						Maps.asMap(col2, "b") },
				{ Arrays.asList("x", "b", null), Arrays.asList("x", "y", "z"),
						Maps.asMap(col2, "b").add(col3, null) },
				{ Arrays.asList("x", "b", null), null,
						Maps.asMap(col1, "x").add(col2, "b") },
		};
	}
	
	@Test(dataProvider = GET_UPDATE_VALUES_DIFF_ONLY_DATA)
	public void testGetUpdateValues_diffOnly(List<String> modified, List<String> unmodified, Map<Column, String> expected) throws Exception {
		PersistentValues insertValues = testInstance.getUpdateValues(modified, unmodified, false);
		assertEquals(insertValues.getUpsertValues(), expected);
	}
	
	@DataProvider(name = GET_UPDATE_VALUES_ALL_COLUMNS_DATA)
	private Object[][] testGetUpdateValues_allColumnsData() {
		return new Object[][] {
				{ Arrays.asList("a", "b", "c"), Arrays.asList("x", "y", "x"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Arrays.asList("a", "b"), Arrays.asList("x", "y", "x"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Arrays.asList("a", "b", "c"), Arrays.asList("x", "y"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Arrays.asList("x", "b"), Arrays.asList("x", "y"),
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Arrays.asList("x", "b", null), Arrays.asList("x", "y", "z"),
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Arrays.asList("x", "b", null), null,
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Arrays.asList("a", "b", "c"), Arrays.asList("a", "b", "c"),
						new HashMap<>() },
		};
	}
	
	@Test(dataProvider = GET_UPDATE_VALUES_ALL_COLUMNS_DATA)
	public void testGetUpdateValues_allColumns(List<String> modified, List<String> unmodified, Map<Column, String> expected) throws Exception {
		PersistentValues insertValues = testInstance.getUpdateValues(modified, unmodified, true);
		assertEquals(insertValues.getUpsertValues(), expected);
	}
	
	@Test
	public void testGetDeleteValues() throws Exception {
		// Pas de suppression de ligne avec cette stratégie en colonne
		assertTrue(testInstance.getDeleteValues(Arrays.asList("a", "b")).getWhereValues().isEmpty());
	}
	
	@Test
	public void testGetSelectValues() throws Exception {
		// Pas de suppression de ligne avec cette stratégie en colonne
		assertTrue(testInstance.getSelectValues(1).getWhereValues().isEmpty());
	}
	
	@Test
	public void testGetVersionedKeyValues() throws Exception {
		// Pas de suppression de ligne avec cette stratégie en colonne
		assertTrue(testInstance.getVersionedKeyValues(Arrays.asList("a", "b")).getWhereValues().isEmpty());
	}
}