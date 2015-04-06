package org.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
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
		// création de l'instance à tester
		// NB: pas de générateur d'id car ce n'est pas ce qu'on teste (à changer si besoin)
		testInstance = new ClassMappingStrategy<>(Toto.class, totoClassTable, totoClassMapping, null);
		
		final int nbCol = 2;
		Set<Column> collectionColumn = new LinkedHashSet<>(nbCol);
		for (int i = 1; i <= nbCol; i++) {
			String columnName = "cold_" + i;
			collectionColumn.add(totoClassTable.new Column(columnName, String.class));
		}
		testInstance.put(d, new ColumnedCollectionMappingStrategy<List<String>, String>(totoClassTable, collectionColumn, ArrayList.class) {
			
			@Override
			protected String toCollectionValue(Object t) {
				return t.toString();
			}
		});
		
		final BidiMap<String, Column> mappedColumnsOnKey = new DualHashBidiMap<>();
		for (int i = 1; i <= 2; i++) {
			String columnName = "cole_" + i;
			Column column = totoClassTable.new Column(columnName, String.class);
			switch (i) {
				case 1:
					mappedColumnsOnKey.put("x", column);
					break;
				case 2:
					mappedColumnsOnKey.put("y", column);
					break;
			}
		}
		testInstance.put(e, new ColumnedMapMappingStrategy<Map<String, String>, String, String, String>(totoClassTable, mappedColumnsOnKey.values(), HashMap.class) {
			
			@Override
			protected Column getColumn(String key) {
				Column column = mappedColumnsOnKey.get(key);
				if (column == null) {
					throw new IllegalArgumentException("Unknown key " + key);
				} else {
					return column;
				}
			}
			
			@Override
			protected String getKey(Column column) {
				return null;
			}
			
			@Override
			protected String toDatabaseValue(String key, String s) {
				return s;
			}
			
			@Override
			protected String toMapValue(String key, Object s) {
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