package org.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.util.ArrayList;
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
	private static final String GET_UPDATE_VALUES_DIFF_ONLY_DATA = "testGetUpdateValuesDiffOnlyData";
	private static final String GET_UPDATE_VALUES_ALL_COLUMNS_DATA = "testGetUpdateValuesAllColumnsData";
	
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
		Table totoClassTable = new Table(null, "Toto");
		Map<Field, Column> totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
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
		testInstance.put(d, new CollectionColumnedMappingStrategy<List<String>, String>(totoClassTable, String.class, ArrayList.class) {
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
			
			@Override
			protected String toCollectionValue(Object t) {
				return t.toString();
			}
		});
		testInstance.put(e, new MapMappingStrategy<Map<String, String>, String, String, String>(totoClassTable, String.class, HashMap.class) {
			
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
			protected String getKey(Column column) {
				return null;
			}
			
			@Override
			protected String toDatabaseValue(String s) {
				return s;
			}
			
			@Override
			protected String toMapValue(Object s) {
				return s.toString();
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
				{ new Toto(1, 2, 3), Maps.asMap(colA, 1).add(colB, 2).add(colC, 3)
						.add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null) },
				{ new Toto(null, null, null), Maps.asMap(colA, null).add(colB, null).add(colC, null)
						.add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null) },
				{ new Toto(null, 2, 3), Maps.asMap(colA, null).add(colB, 2).add(colC, 3)
						.add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null) },
				{ new Toto(1, 2, 3, Arrays.asList("a")), Maps.asMap(colA, (Object) 1).add(colB, 2).add(colC, 3)
						.add(colD1, "a").add(colD2, null).add(colE1, null).add(colE2, null) },
				{ new Toto(1, 2, 3, Maps.asMap("x", "y")), Maps.asMap(colA, (Object) 1).add(colB, 2).add(colC, 3)
						.add(colD1, null).add(colD2, null).add(colE1, "y").add(colE2, null) },
		};
	}
	
	@Test(dataProvider = GET_INSERT_VALUES_DATA)
	public void testGetInsertValues(Toto modified, Map<Column, Object> expectedResult) throws Exception {
		PersistentValues valuesToInsert = testInstance.getInsertValues(modified);
		
		Assert.assertEquals(valuesToInsert.getUpsertValues(), expectedResult);
	}
	
	@DataProvider(name = GET_UPDATE_VALUES_DIFF_ONLY_DATA)
	public Object[][] testGetUpdateValues_diffOnlyData() throws Exception {
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(1, 5, 6), Maps.asMap(colB, 2).add(colC, 3)}, 
				{ new Toto(1, 2, 3), new Toto(1, null, null), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 42), Maps.asMap(colC, 3) },
				{ new Toto(1, null, null), new Toto(1, 2, 3), Maps.asMap(colB, null).add(colC, null) },
				{ new Toto(null, null, null), new Toto(null, 2, 3), Maps.asMap(colB, null).add(colC, null) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 3), new HashMap<>() },
				{ new Toto(null, null, null), new Toto(null, null, null), new HashMap<>() },
				{ new Toto(1, 2, 3), null, Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3, Arrays.asList("a")), new Toto(1, 5, 6, Arrays.asList("b")),
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colD1, "a") },
				{ new Toto(1, 2, 3, Maps.asMap("x", "y")), new Toto(1, 5, 6, Maps.asMap("x", "z")),
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colE1, "y") },
				{ new Toto(1, 2, 3, Arrays.asList("a"), Maps.asMap("x", "y")), null,
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colD1, "a").add(colE1, "y") },
		};
	}
	
	@Test(dataProvider = GET_UPDATE_VALUES_DIFF_ONLY_DATA)
	public void testGetUpdateValues_diffOnly(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) throws Exception {
		PersistentValues valuesToUpdate = testInstance.getUpdateValues(modified, unmodified, false);
		
		Assert.assertEquals(valuesToUpdate.getUpsertValues(), expectedResult);
		Assert.assertEquals(valuesToUpdate.getWhereValues(), Maps.asMap(colA, modified.a));
	}
	
	@DataProvider(name = GET_UPDATE_VALUES_ALL_COLUMNS_DATA)
	public Object[][] testGetUpdateValues_allColumnsData() throws Exception {
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(1, 5, 6),
						Maps.asMap(colB, 2).add(colC, 3).add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null)},
				{ new Toto(1, 2, 3), new Toto(1, null, null),
						Maps.asMap(colB, 2).add(colC, 3).add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null)},
				{ new Toto(1, 2, 3), new Toto(1, 2, 42),
						Maps.asMap(colB, 2).add(colC, 3).add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null)},
				{ new Toto(1, null, null), new Toto(1, 2, 3),
						Maps.asMap(colB, null).add(colC, null).add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null)},
				{ new Toto(null, null, null), new Toto(null, 2, 3),
						Maps.asMap(colB, null).add(colC, null).add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null)},
				{ new Toto(1, 2, 3), new Toto(1, 2, 3),
						new HashMap<>()},
				{ new Toto(null, null, null), new Toto(null, null, null),
						new HashMap<>()},
				{ new Toto(1, 2, 3), null,
						Maps.asMap(colB, 2).add(colC, 3).add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null)},
				{ new Toto(1, 2, 3, Arrays.asList("a")), new Toto(1, 5, 6, Arrays.asList("b")),
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colD1, "a").add(colD2, null).add(colE1, null).add(colE2, null)},
				{ new Toto(1, 2, 3, Maps.asMap("x", "y")), new Toto(1, 5, 6, Maps.asMap("x", "z")),
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colD1, null).add(colD2, null).add(colE1, "y").add(colE2, null)},
				{ new Toto(1, 2, 3, Arrays.asList("a"), Maps.asMap("x", "y")), null,
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colD1, "a").add(colD2, null).add(colE1, "y").add(colE2, null)},
		};
	}
	
	@Test(dataProvider = GET_UPDATE_VALUES_ALL_COLUMNS_DATA)
	public void testGetUpdateValues_allColumns(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) throws Exception {
		PersistentValues valuesToUpdate = testInstance.getUpdateValues(modified, unmodified, true);
		
		Assert.assertEquals(valuesToUpdate.getUpsertValues(), expectedResult);
		Assert.assertEquals(valuesToUpdate.getWhereValues(), Maps.asMap(colA, modified.a));
	}
	
	private static class Toto {
		private Integer a, b, c;
		
		private List<String> d;
		
		private Map<String, String> e;
		
		public Toto() {
		}
		
		public Toto(Integer a, Integer b, Integer c) {
			this(a, b, c, null, null);
		}
		
		public Toto(Integer a, Integer b, Integer c, List<String> d) {
			this(a, b, c, d, null);
		}
		
		public Toto(Integer a, Integer b, Integer c, Map<String, String> e) {
			this(a, b, c, null, e);
		}
		
		public Toto(Integer a, Integer b, Integer c, List<String> d, Map<String, String> e) {
			this.a = a;
			this.b = b;
			this.c = c;
			this.d = d;
			this.e = e;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("a", (Object) a).add("b", b).add("c", c).add("d", d).add("e", e)
					+ "]";
		}
	}
}