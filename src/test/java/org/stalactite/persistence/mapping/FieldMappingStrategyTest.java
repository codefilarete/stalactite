package org.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FieldMappingStrategyTest {
	
	private static final String GET_UPDATE_VALUES_DATA = "testGetUpdateValuesData";
	private static final String GET_INSERT_VALUES_DATA = "testGetInsertValuesData";
	
	private Column colA;
	private Column colB;
	private Column colC;
	private FieldMappingStrategy<Toto> testInstance;
	
	@BeforeTest
	public void setUp() {
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		List<Field> fields = persistentFieldHarverster.getFields(Toto.class);
		
		Map<Field, Column> totoClassMapping = new HashMap<>(5);
		Table totoClassTable = new Table(null, "Toto");
		for (Field field : fields) {
			totoClassMapping.put(field, totoClassTable.new Column(field.getName(), Integer.class));
		}
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
				{ new Toto(1, 2, 3), Maps.fastMap(colA, 1).put(colB, 2).put(colC, 3).getMap() },
				{ new Toto(null, null, null), Maps.fastMap(colA, null).put(colB, null).put(colC, null).getMap() },
				{ new Toto(null, 2, 3), Maps.fastMap(colA, null).put(colB, 2).put(colC, 3).getMap() },
		};
	}
	
	@Test(dataProvider = GET_INSERT_VALUES_DATA)
	public void testGetInsertValues(Toto modified, Map<Column, Object> expectedResult) throws Exception {
		PersistentValues valuesToInsert = testInstance.getInsertValues(modified);
		
		Assert.assertEquals(valuesToInsert.getUpsertValues(), expectedResult);
	}
	
	@DataProvider(name = GET_UPDATE_VALUES_DATA)
	public Object[][] testGetUpdateValuesData() throws Exception {
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(4, 5, 6), Maps.fastMap(colA, 1).put(colB, 2).put(colC, 3).getMap() },
				{ new Toto(1, 2, 3), new Toto(null, null, null), Maps.fastMap(colA, 1).put(colB, 2).put(colC, 3).getMap() },
				{ new Toto(1, 2, 3), new Toto(1, 2, 42), Maps.fastMap(colC, 3).getMap() },
				{ new Toto(1, 2, 3), new Toto(1, 23, 42), Maps.fastMap(colB, 2).put(colC, 3).getMap() },
				{ new Toto(null, null, null), new Toto(1, 2, 3), Maps.fastMap(colA, null).put(colB, null).put(colC, null).getMap() },
				{ new Toto(null, null, null), new Toto(null, 2, 3), Maps.fastMap(colB, null).put(colC, null).getMap() },
				{ new Toto(1, 2, 3), new Toto(1, 2, 3), new HashMap<>() },
				{ new Toto(null, null, null), new Toto(null, null, null), new HashMap<>() },
		};
	}
	
	@Test(dataProvider = GET_UPDATE_VALUES_DATA)
	public void testGetUpdateValues(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) throws Exception {
		PersistentValues valuesToInsert = testInstance.getUpdateValues(modified, unmodified);
		
		Assert.assertEquals(valuesToInsert.getUpsertValues(), expectedResult);
		Assert.assertEquals(valuesToInsert.getWhereValues(), Maps.fastMap(colA, unmodified.a).getMap());
	}
	
	@Test
	public void testGetDeleteValues() throws Exception {
		PersistentValues versionedKeyValues = testInstance.getDeleteValues(new Toto(1, 2, 3));
		Assert.assertEquals(versionedKeyValues.getWhereValues(), Maps.fastMap(colA, 1).getMap());
	}
	
	@Test
	public void testGetSelectValues() throws Exception {
		PersistentValues versionedKeyValues = testInstance.getSelectValues(new Toto(1, 2, 3));
		Assert.assertEquals(versionedKeyValues.getWhereValues(), Maps.fastMap(colA, 1).getMap());
	}
	
	@Test
	public void testGetVersionedKeyValues() throws Exception {
		PersistentValues versionedKeyValues = testInstance.getVersionedKeyValues(new Toto(1, 2, 3));
		
		Assert.assertEquals(versionedKeyValues.getWhereValues(), Maps.fastMap(colA, 1).getMap());
	}
	
	private static class Toto {
		private Integer a, b, c;
		
		public Toto(Integer a, Integer b, Integer c) {
			this.a = a;
			this.b = b;
			this.c = c;
		}
	}

}