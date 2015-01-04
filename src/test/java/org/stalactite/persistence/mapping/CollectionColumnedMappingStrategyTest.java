package org.stalactite.persistence.mapping;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.Test;

public class CollectionColumnedMappingStrategyTest {
	
	@Test
	public void testGetInsertValues() throws Exception {
		Table toto = new Table(null, "Toto");
		
		CollectionColumnedMappingStrategy<List<String>, String> testInstance = new CollectionColumnedMappingStrategy<>(toto, "col_", String.class, 5);
		Map<String, Column> namedColumns = toto.mapColumnsOnName();
		Column col1 = namedColumns.get("col_1");
		col1.setPrimaryKey(true);
		Column col2 = namedColumns.get("col_2");
		Column col3 = namedColumns.get("col_3");
		Column col4 = namedColumns.get("col_4");
		Column col5 = namedColumns.get("col_5");
		
		PersistentValues insertValues = testInstance.getInsertValues(Arrays.asList("a", "b", "c"));
		assertEquals(insertValues.getUpsertValues(), 
				Maps.fastMap(col1, "a").put(col2, "b").put(col3, "c").put(col4, null).put(col5, null).getMap());
	}
	
	@Test
	public void testGetUpdateValues() throws Exception {
		Table toto = new Table(null, "Toto");
		
		CollectionColumnedMappingStrategy<List<String>, String> testInstance = new CollectionColumnedMappingStrategy<>(toto, "col_", String.class, 5);
		Map<String, Column> namedColumns = toto.mapColumnsOnName();
		Column col1 = namedColumns.get("col_1");
		col1.setPrimaryKey(true);
		Column col2 = namedColumns.get("col_2");
		Column col3 = namedColumns.get("col_3");
		
		PersistentValues insertValues = testInstance.getUpdateValues(Arrays.asList("a", "b", "c"), Arrays.asList("x", "y", "x"));
		assertEquals(insertValues.getUpsertValues(), 
				Maps.fastMap(col1, "a").put(col2, "b").put(col3, "c").getMap());
		insertValues = testInstance.getUpdateValues(Arrays.asList("a", "b"), Arrays.asList("x", "y", "x"));
		assertEquals(insertValues.getUpsertValues(), 
				Maps.fastMap(col1, "a").put(col2, "b").put(col3, null).getMap());
		insertValues = testInstance.getUpdateValues(Arrays.asList("a", "b", "c"), Arrays.asList("x", "y"));
		assertEquals(insertValues.getUpsertValues(), 
				Maps.fastMap(col1, "a").put(col2, "b").put(col3, "c").getMap());
		insertValues = testInstance.getUpdateValues(Arrays.asList("x", "b"), Arrays.asList("x", "y"));
		assertEquals(insertValues.getUpsertValues(), 
				Maps.fastMap(col2, "b").getMap());
		insertValues = testInstance.getUpdateValues(Arrays.asList("x", "b", null), Arrays.asList("x", "y", "z"));
		assertEquals(insertValues.getUpsertValues(), 
				Maps.fastMap(col2, "b").put(col3, null).getMap());
	}
	
	@Test
	public void testGetDeleteValues() throws Exception {
		Table toto = new Table(null, "Toto");
		CollectionColumnedMappingStrategy<List<String>, String> testInstance = new CollectionColumnedMappingStrategy<>(toto, "col_", String.class, 5);
		assertTrue(testInstance.getDeleteValues(Arrays.asList("a", "b")).getWhereValues().isEmpty());
	}
	
	@Test
	public void testGetSelectValues() throws Exception {
		Table toto = new Table(null, "Toto");
		CollectionColumnedMappingStrategy<List<String>, String> testInstance = new CollectionColumnedMappingStrategy<>(toto, "col_", String.class, 5);
		assertTrue(testInstance.getSelectValues(Arrays.asList("a", "b")).getWhereValues().isEmpty());
	}
	
	@Test
	public void testGetVersionedKeyValues() throws Exception {
		Table toto = new Table(null, "Toto");
		CollectionColumnedMappingStrategy<List<String>, String> testInstance = new CollectionColumnedMappingStrategy<>(toto, "col_", String.class, 5);
		assertTrue(testInstance.getVersionedKeyValues(Arrays.asList("a", "b")).getWhereValues().isEmpty());
	}
}