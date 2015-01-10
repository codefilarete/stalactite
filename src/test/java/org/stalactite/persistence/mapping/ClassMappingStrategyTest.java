package org.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClassMappingStrategyTest {
	
	private static final String GET_INSERT_VALUES_DATA = "testGetInsertValuesData";
	
	private Column colA;
	private Column colB;
	private Column colC;
	private Column colD1;
	private Column colD2;
	private Column colE1;
	private Column colE2;
	private ClassMappingStrategy<Toto> testInstance;
	
	@BeforeTest
	public void setUp() throws NoSuchFieldException {
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
		
		// Remplacement du mapping par défaut pour la List (attribut d) par une strategy adhoc
		Field d = Toto.class.getDeclaredField("d");
		totoClassMapping.remove(d);
		// Remplacement du mapping par défaut pour la Map (attribut e) par une strategy adhoc
		Field e = Toto.class.getDeclaredField("e");
		totoClassMapping.remove(e);
		testInstance = new ClassMappingStrategy<>(Toto.class, totoClassTable, totoClassMapping);
		testInstance.put(d, new CollectionColumnedMappingStrategy<List<String>, String>(totoClassTable, String.class) {
			@Override
			protected LinkedHashSet<Column> initTargetColumns() {
				int nbCol = 2;
				String columnsPrefix = "cold_";
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
		});
		testInstance.put(e, new MapMappingStrategy<Map<String, String>, String, String, String>(totoClassTable, String.class) {
			
			@Override
			protected LinkedHashSet<Column> initTargetColumns() {
				int nbCol = 2;
				String columnsPrefix = "cole_";
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
			protected Column getColumn(String key) {
				String columnName;
				switch (key) {
					case "x":
						columnName = "cole_1";
						break;
					default:
					throw new IllegalArgumentException("Unknown key " + key);
				}
				return getTargetTable().mapColumnsOnName().get(columnName);
			}
			
			@Override
			protected String convertMapValue(String s) {
				return s;
			}
		});
		
		columns = totoClassTable.mapColumnsOnName();
		colD1 = columns.get("cold_1");
		colD2 = columns.get("cold_2");
		colE1 = columns.get("cole_1");
		colE2 = columns.get("cole_2");
	}
	
	@DataProvider(name = GET_INSERT_VALUES_DATA)
	public Object[][] testGetInsertValuesData() throws Exception {
		return new Object[][] {
				{ new Toto(1, 2, 3), Maps.fastMap(colA, 1).put(colB, 2).put(colC, 3)
						.put(colD1, null).put(colD2, null).put(colE1, null).put(colE2, null).getMap() },
				{ new Toto(null, null, null), Maps.fastMap(colA, null).put(colB, null).put(colC, null)
						.put(colD1, null).put(colD2, null).put(colE1, null).put(colE2, null).getMap() },
				{ new Toto(null, 2, 3), Maps.fastMap(colA, null).put(colB, 2).put(colC, 3)
						.put(colD1, null).put(colD2, null).put(colE1, null).put(colE2, null).getMap() },
				{ new Toto(1, 2, 3, Arrays.asList("a")), Maps.fastMap(colA, (Object) 1).put(colB, 2).put(colC, 3)
						.put(colD1, "a").put(colD2, null).put(colE1, null).put(colE2, null).getMap() },
				{ new Toto(1, 2, 3, Maps.fastMap("x", "y").getMap()), Maps.fastMap(colA, (Object) 1).put(colB, 2).put(colC, 3)
						.put(colD1, null).put(colD2, null).put(colE1, "y").put(colE2, null).getMap() },
		};
	}
	
	@Test(dataProvider = GET_INSERT_VALUES_DATA)
	public void testGetInsertValues(Toto modified, Map<Column, Object> expectedResult) throws Exception {
		PersistentValues valuesToInsert = testInstance.getInsertValues(modified);
		
		Assert.assertEquals(valuesToInsert.getUpsertValues(), expectedResult);
	}
	
	private static class Toto {
		private Integer a, b, c;
		
		private List<String> d;
		
		private Map<String, String> e;
		
		public Toto(Integer a, Integer b, Integer c) {
			this.a = a;
			this.b = b;
			this.c = c;
		}
		
		public Toto(Integer a, Integer b, Integer c, List<String> d) {
			this(a, b, c);
			this.d = d;
		}
		
		public Toto(Integer a, Integer b, Integer c, Map<String, String> e) {
			this(a, b, c);
			this.e = e;
		}
	}
}