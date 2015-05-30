package org.gama.stalactite.persistence.mapping;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.sql.result.Row;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FieldMappingStrategyTest {
	
	private static final String GET_INSERT_VALUES_DATA = "testGetInsertValuesData";
	private static final String GET_UPDATE_VALUES_DIFF_ONLY_DATA = "testGetUpdateValuesDiffOnlyData";
	private static final String GET_UPDATE_VALUES_ALL_COLUMNS_DATA = "testGetUpdateValuesAllColumnsData";
	
	private Column colA;
	private Column colB;
	private Column colC;
	private FieldMappingStrategy<Toto> testInstance;
	
	@BeforeTest
	public void setUp() {
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Table totoClassTable = new Table(null, "Toto");
		Map<Field, Column> totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column> columns = totoClassTable.mapColumnsOnName();
		colA = columns.get("a");
		colA.setPrimaryKey(true);
		colB = columns.get("b");
		colC = columns.get("c");
		
		testInstance = new FieldMappingStrategy<>(totoClassMapping);
	}
	
	@DataProvider(name = GET_INSERT_VALUES_DATA)
	public Object[][] testGetInsertValuesData() throws Exception {
		return new Object[][] {
				{ new Toto(1, 2, 3), Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
				{ new Toto(null, null, null), Maps.asMap(colA, null).add(colB, null).add(colC, null) },
				{ new Toto(null, 2, 3), Maps.asMap(colA, null).add(colB, 2).add(colC, 3) },
		};
	}
	
	@Test(dataProvider = GET_INSERT_VALUES_DATA)
	public void testGetInsertValues(Toto modified, Map<Column, Object> expectedResult) throws Exception {
		StatementValues valuesToInsert = testInstance.getInsertValues(modified);
		
		assertEquals(valuesToInsert.getUpsertValues(), expectedResult);
	}
	
	@DataProvider(name = GET_UPDATE_VALUES_DIFF_ONLY_DATA)
	public Object[][] testGetUpdateValues_diffOnlyData() throws Exception {
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(1, 5, 6), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, null, null), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 42), Maps.asMap(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 23, 42), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, null, null), new Toto(1, 2, 3), Maps.asMap(colB, null).add(colC, null) },
				{ new Toto(null, null, null), new Toto(null, 2, 3), Maps.asMap(colB, null).add(colC, null) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 3), new HashMap<>() },
				{ new Toto(null, null, null), new Toto(null, null, null), new HashMap<>() },
				{ new Toto(1, 2, 3), null, Maps.asMap(colB, 2).add(colC, 3) },
		};
	}
	
	@Test(dataProvider = GET_UPDATE_VALUES_DIFF_ONLY_DATA)
	public void testGetUpdateValues_diffOnly(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) throws Exception {
		StatementValues valuesToInsert = testInstance.getUpdateValues(modified, unmodified, false);
		
		assertEquals(valuesToInsert.getUpsertValues(), expectedResult);
		assertEquals(valuesToInsert.getWhereValues(), Maps.asMap(colA, modified.a));
	}
	
	@DataProvider(name = GET_UPDATE_VALUES_ALL_COLUMNS_DATA)
	public Object[][] testGetUpdateValues_allColumnsData() throws Exception {
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(1, 2, 42), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(null, null, null), new Toto(null, null, null), new HashMap<>() },
				{ new Toto(1, 2, 3), null, Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 3), new HashMap<>() },
		};
	}
	
	@Test(dataProvider = GET_UPDATE_VALUES_ALL_COLUMNS_DATA)
	public void testGetUpdateValues_allColumns(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) throws Exception {
		StatementValues valuesToInsert = testInstance.getUpdateValues(modified, unmodified, true);
		
		assertEquals(valuesToInsert.getUpsertValues(), expectedResult);
		assertEquals(valuesToInsert.getWhereValues(), Maps.asMap(colA, modified.a));
	}
	
	@Test
	public void testGetDeleteValues() throws Exception {
		StatementValues versionedKeyValues = testInstance.getDeleteValues(new Toto(1, 2, 3));
		assertEquals(versionedKeyValues.getWhereValues(), Maps.asMap(colA, 1));
	}
	
	@Test
	public void testGetSelectValues() throws Exception {
		StatementValues versionedKeyValues = testInstance.getSelectValues(1);
		assertEquals(versionedKeyValues.getWhereValues(), Maps.asMap(colA, 1));
	}
	
	@Test
	public void testGetVersionedKeyValues() throws Exception {
		StatementValues versionedKeyValues = testInstance.getVersionedKeyValues(new Toto(1, 2, 3));
		assertEquals(versionedKeyValues.getWhereValues(), Maps.asMap(colA, 1));
	}
	
	@Test
	public void testTransform() throws Exception {
		Row row = new Row();
		row.put("a", 1);
		row.put("b", 2);
		row.put("c", 3);
		Toto toto = testInstance.transform(row);
		assertEquals((int) toto.a, 1);
		assertEquals((int) toto.b, 2);
		assertEquals((int) toto.c, 3);
	}
	
	private static class Toto {
		private Integer a, b, c;
		
		public Toto() {
			
		}
		
		public Toto(Integer a, Integer b, Integer c) {
			this.a = a;
			this.b = b;
			this.c = c;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "[" + Maps.asMap("a", a).add("b", b).add("c", c) + "]";
		}
	}

}