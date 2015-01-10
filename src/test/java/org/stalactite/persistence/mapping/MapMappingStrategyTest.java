package org.stalactite.persistence.mapping;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.LinkedHashSet;
import java.util.Map;

import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MapMappingStrategyTest {
	
	private Table toto;
	private MapMappingStrategy<Map<Integer, String>, Integer, String, String> testInstance;
	private Column col1;
	private Column col2;
	private Column col3;
	private Column col4;
	private Column col5;
	
	@BeforeTest
	public void setUp() throws Exception {
		toto = new Table(null, "Toto");
		testInstance = new MapMappingStrategy<Map<Integer, String>, Integer, String, String>(toto, String.class) {
			protected LinkedHashSet<Column> initTargetColumns() {
				int nbCol = 5;
				String columnsPrefix = "col_";
				Map<String, Column> existingColumns = getTargetTable().mapColumnsOnName();
				LinkedHashSet<Column> toReturn = new LinkedHashSet<>(nbCol, 1);
				for (int i = 1; i <= nbCol; i++) {
					String columnName = getColumnName(columnsPrefix, i);
					Column column = existingColumns.get(columnName);
					if (column == null) {
						column = getTargetTable().new Column(columnName, getPersistentType());
					}
					toReturn.add(column);
				}
		
				return toReturn;
			}

			@Override
			protected Column getColumn(Integer key) {
				if (key > 5) {
					throw new IllegalArgumentException("Unknown key " + key);
				}
				return getTargetTable().mapColumnsOnName().get("col_"+key);
			}
			
			@Override
			protected String convertMapValue(String s) {
				return s;
			}
		};
		Map<String, Column> namedColumns = toto.mapColumnsOnName();
		col1 = namedColumns.get("col_1");
		col1.setPrimaryKey(true);
		col2 = namedColumns.get("col_2");
		col3 = namedColumns.get("col_3");
		col4 = namedColumns.get("col_4");
		col5 = namedColumns.get("col_5");
	}
	
	@Test
	public void testGetInsertValues() throws Exception {
		PersistentValues insertValues = testInstance.getInsertValues(Maps.fastMap(1, "a").put(2, "b").put(3, "c").getMap());
		assertEquals(insertValues.getUpsertValues(),
				Maps.fastMap(col1, "a").put(col2, "b").put(col3, "c").put(col4, null).put(col5, null).getMap());
	}
	
	@DataProvider
	private Object[][] testGetUpdateValuesData() {
		return new Object[][] {
				{ Maps.fastMap(1, "a").put(2, "b").put(3, "c").getMap(), Maps.fastMap(1, "x").put(2, "y").put(3, "z").getMap(),
						Maps.fastMap(col1, "a").put(col2, "b").put(col3, "c").getMap() },
				{ Maps.fastMap(1, "a").put(2, "b").getMap(), Maps.fastMap(1, "x").put(2, "y").put(3, "z").getMap(),
						Maps.fastMap(col1, "a").put(col2, "b").put(col3, null).getMap() },
				{ Maps.fastMap(1, "a").put(2, "b").put(3, "c").getMap(), Maps.fastMap(1, "x").put(2, "y").getMap(),
						Maps.fastMap(col1, "a").put(col2, "b").put(col3, "c").getMap() },
				{ Maps.fastMap(1, "x").put(2, "b").getMap(), Maps.fastMap(1, "x").put(2, "y").getMap(),
						Maps.fastMap(col2, "b").getMap() },
				{ Maps.fastMap(1, "x").put(2, "b").getMap(), Maps.fastMap(1, "x").put(2, "y").put(3, "z").getMap(),
						Maps.fastMap(col2, "b").put(col3, null).getMap() },
		};
	}
	
	@Test(dataProvider = "testGetUpdateValuesData")
	public void testGetUpdateValues(Map<Integer, String> modified, Map<Integer, String> unmodified, Map<Integer, String> expected) throws Exception {
		PersistentValues updateValues = testInstance.getUpdateValues(modified, unmodified);
		assertEquals(updateValues.getUpsertValues(),expected);
	}
	
	@Test
	public void testGetDeleteValues() throws Exception {
		// Pas de suppression de ligne avec cette stratégie en colonne
		assertTrue(testInstance.getDeleteValues(Maps.fastMap(1, "a").put(2, "b").getMap()).getWhereValues().isEmpty());
	}
	
	@Test
	public void testGetSelectValues() throws Exception {
		// Pas de sélection de ligne avec cette stratégie en colonne
		assertTrue(testInstance.getSelectValues(Maps.fastMap(1, "a").put(2, "b").getMap()).getWhereValues().isEmpty());
	}
	
	@Test
	public void testGetVersionedKeyValues() throws Exception {
		// Pas de clé versionnée avec cette stratégie en colonne
		assertTrue(testInstance.getVersionedKeyValues(Maps.fastMap(1, "a").put(2, "b").getMap()).getWhereValues().isEmpty());
	}
}