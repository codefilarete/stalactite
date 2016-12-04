package org.gama.stalactite.persistence.mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class ClassMappingStrategyTest {
	
	private static Column colA;
	private static Column colB;
	private static Column colC;
	private static Column colD1;
	private static Column colD2;
	private static Column colE1;
	private static Column colE2;
	private static Map<PropertyAccessor, Column> classMapping;
	private static Table targetTable;
	private static PersistentFieldHarverster persistentFieldHarverster;
	private static Map<String, Column> columnMapOnName;
	private static PropertyAccessor myListField;
	private static PropertyAccessor myMapField;
	
	private static ClassMappingStrategy<Toto, Integer> testInstance;
	
	@BeforeClass
	public static void setUpClass() {
		persistentFieldHarverster = new PersistentFieldHarverster();
		targetTable = new Table("Toto");
		classMapping = persistentFieldHarverster.mapFields(Toto.class, targetTable);
		
		
		columnMapOnName = targetTable.mapColumnsOnName();
		colA = columnMapOnName.get("a");
		colA.setPrimaryKey(true);
		colB = columnMapOnName.get("b");
		colC = columnMapOnName.get("c");
		
		// Remplacement du mapping par défaut pour la List (attribut myListField) par une strategy adhoc
		myListField = PropertyAccessor.forProperty(Reflections.findField(Toto.class, "myList"));
		classMapping.remove(myListField);
		// Remplacement du mapping par défaut pour la Map (attribut myMapField) par une strategy adhoc
		myMapField = PropertyAccessor.forProperty(Reflections.findField(Toto.class, "myMap"));
		classMapping.remove(myMapField);
		
		setUpTestInstance();
	}
	
	public static void setUpTestInstance() {
		// instance to test building
		// The basic mapping will be altered to add special mapping for field "myListField" (a Collection) and "myMapField" (a Map)
		testInstance = new ClassMappingStrategy<>(Toto.class,
				targetTable,
				classMapping,
				PropertyAccessor.forProperty(persistentFieldHarverster.getField("a")),
				// Basic mapping to prevent NullPointerException, even if it's not the goal of our test
				new AlreadyAssignedIdentifierManager<>(Integer.class));
		
		
		// Additionnal mapping: the list is mapped to 2 additionnal columns
		int nbCol = 2;
		Set<Column> collectionColumn = new LinkedHashSet<>(nbCol);
		for (int i = 1; i <= nbCol; i++) {
			String columnName = "cold_" + i;
			collectionColumn.add(targetTable.new Column(columnName, String.class));
		}
		testInstance.put(myListField, new ColumnedCollectionMappingStrategy<List<String>, String>(targetTable, collectionColumn, ArrayList.class) {
			
			@Override
			protected String toCollectionValue(Object t) {
				return t.toString();
			}
		});
		
		// Additionnal mapping: the map is mapped to 2 additionnal columns
		final Map<String, Column> mappedColumnsOnKey = new HashMap<>();
		for (int i = 1; i <= 2; i++) {
			String columnName = "cole_" + i;
			Column column = targetTable.new Column(columnName, String.class);
			switch (i) {
				case 1:
					mappedColumnsOnKey.put("x", column);
					break;
				case 2:
					mappedColumnsOnKey.put("y", column);
					break;
			}
		}
		testInstance.put(myMapField, new ColumnedMapMappingStrategy<Map<String, String>, String, String, String>(targetTable, new HashSet<>(mappedColumnsOnKey.values()), HashMap.class) {
			
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
		
		columnMapOnName = targetTable.mapColumnsOnName();
		colD1 = columnMapOnName.get("cold_1");
		colD2 = columnMapOnName.get("cold_2");
		colE1 = columnMapOnName.get("cole_1");
		colE2 = columnMapOnName.get("cole_2");
	}
	
	@DataProvider
	public static Object[][] testGetInsertValuesData() {
		setUpClass();
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
	
	@Test
	@UseDataProvider("testGetInsertValuesData")
	public void testGetInsertValues(Toto modified, Map<Column, Object> expectedResult) {
		Map<Column, Object> valuesToInsert = testInstance.getInsertValues(modified);
		
		assertEquals(expectedResult, valuesToInsert);
	}
	
	@DataProvider
	public static Object[][] testGetUpdateValues_diffOnlyData() {
		setUpClass();
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
	
	@Test
	@UseDataProvider("testGetUpdateValues_diffOnlyData")
	public void testGetUpdateValues_diffOnly(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) {
		Map<UpwhereColumn, Object> valuesToUpdate = testInstance.getUpdateValues(modified, unmodified, false);
		
		assertEquals(expectedResult, UpwhereColumn.getUpdateColumns(valuesToUpdate));
		if (!expectedResult.isEmpty()) {
			assertEquals(Maps.asMap(colA, modified.a), UpwhereColumn.getWhereColumns(valuesToUpdate));
		} else {
			assertEquals(new HashMap<Column, Object>(), UpwhereColumn.getWhereColumns(valuesToUpdate));
		}
	}
	
	@DataProvider
	public static Object[][] testGetUpdateValues_allColumnsData() {
		setUpClass();
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
	
	@Test
	@UseDataProvider("testGetUpdateValues_allColumnsData")
	public void testGetUpdateValues_allColumns(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) {
		Map<UpwhereColumn, Object> valuesToUpdate = testInstance.getUpdateValues(modified, unmodified, true);
		
		assertEquals(expectedResult, UpwhereColumn.getUpdateColumns(valuesToUpdate));
		if (!expectedResult.isEmpty()) {
			assertEquals(Maps.asMap(colA, modified.a), UpwhereColumn.getWhereColumns(valuesToUpdate));
		} else {
			assertEquals(new HashMap<Column, Object>(), UpwhereColumn.getWhereColumns(valuesToUpdate));
		}
	}
	
	private static class Toto {
		private Integer a, b, c;
		
		private List<String> myList;
		
		private Map<String, String> myMap;
		
		public Toto() {
		}
		
		public Toto(Integer a, Integer b, Integer c) {
			this(a, b, c, null, null);
		}
		
		public Toto(Integer a, Integer b, Integer c, List<String> myList) {
			this(a, b, c, myList, null);
		}
		
		public Toto(Integer a, Integer b, Integer c, Map<String, String> myMap) {
			this(a, b, c, null, myMap);
		}
		
		public Toto(Integer a, Integer b, Integer c, List<String> myList, Map<String, String> myMap) {
			this.a = a;
			this.b = b;
			this.c = c;
			this.myList = myList;
			this.myMap = myMap;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("a", (Object) a).add("b", b).add("c", c).add("myList", myList).add("myMap", myMap)
					+ "]";
		}
	}
}