package org.stalactite.persistence.mapping;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CollectionColumnedMappingStrategyTest {
	
	private Table toto;
	private CollectionColumnedMappingStrategy<List<String>, String> testInstance;
	private Column col1;
	private Column col2;
	private Column col3;
	private Column col4;
	private Column col5;
	
	@BeforeTest
	public void setUp() throws Exception {
		toto = new Table(null, "Toto");
		
		testInstance = new CollectionColumnedMappingStrategy<>(toto, "col_", String.class, 5);
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
		PersistentValues insertValues = testInstance.getInsertValues(Arrays.asList("a", "b", "c"));
		assertEquals(insertValues.getUpsertValues(), 
				Maps.fastMap(col1, "a").put(col2, "b").put(col3, "c").put(col4, null).put(col5, null).getMap());
	}
	
	@DataProvider
	private Object[][] testGetUpdateValuesData() {
		return new Object[][] {
				{ Arrays.asList("a", "b", "c"), Arrays.asList("x", "y", "x"),
						Maps.fastMap(col1, "a").put(col2, "b").put(col3, "c").getMap() },
				{ Arrays.asList("a", "b"), Arrays.asList("x", "y", "x"),
						Maps.fastMap(col1, "a").put(col2, "b").put(col3, null).getMap() },
				{ Arrays.asList("a", "b", "c"), Arrays.asList("x", "y"),
						Maps.fastMap(col1, "a").put(col2, "b").put(col3, "c").getMap() },
				{ Arrays.asList("x", "b"), Arrays.asList("x", "y"),
						Maps.fastMap(col2, "b").getMap() },
				{ Arrays.asList("x", "b", null), Arrays.asList("x", "y", "z"),
						Maps.fastMap(col2, "b").put(col3, null).getMap() },
		};
	}
	
	@Test(dataProvider = "testGetUpdateValuesData")
	public void testGetUpdateValues(List<String> modified, List<String> unmodified, Map<Column, String> expected) throws Exception {
		PersistentValues insertValues = testInstance.getUpdateValues(modified, unmodified);
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
		assertTrue(testInstance.getSelectValues(Arrays.asList("a", "b")).getWhereValues().isEmpty());
	}
	
	@Test
	public void testGetVersionedKeyValues() throws Exception {
		// Pas de suppression de ligne avec cette stratégie en colonne
		assertTrue(testInstance.getVersionedKeyValues(Arrays.asList("a", "b")).getWhereValues().isEmpty());
	}
}