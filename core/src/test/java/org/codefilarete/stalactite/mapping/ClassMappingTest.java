package org.codefilarete.stalactite.mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.Mapping.UpwhereColumn;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Guillaume Mary
 */
class ClassMappingTest {
	
	private static Column colA;
	private static Column colB;
	private static Column colC;
	private static Column colD1;
	private static Column colD2;
	private static Column colE1;
	private static Column colE2;
	private static Map<? extends ReversibleAccessor<Toto, Object>, ? extends Column<?, Object>> classMapping;
	private static Table targetTable;
	private static PersistentFieldHarvester persistentFieldHarvester;
	private static Map<String, Column> columnMapOnName;
	private static PropertyAccessor<Toto, List<String>> myListField;
	private static PropertyAccessor<Toto, Map<String, String>> myMapField;
	
	private static ClassMapping<Toto, Integer, ?> testInstance;
	
	@BeforeAll
	static void setUpClass() {
		persistentFieldHarvester = new PersistentFieldHarvester();
		targetTable = new Table<>("Toto");
		classMapping = persistentFieldHarvester.mapFields(Toto.class, targetTable);
		
		
		columnMapOnName = targetTable.mapColumnsOnName();
		colA = columnMapOnName.get("a");
		colA.setPrimaryKey(true);
		colB = columnMapOnName.get("b");
		colC = columnMapOnName.get("c");
		
		// Replacing default mapping for the List (attribute myList) by a dedicated strategy
		myListField = Accessors.propertyAccessor(Reflections.findField(Toto.class, "myList"));
		classMapping.remove(myListField);
		// Replacing default mapping for the Map (attribute myMap) by a dedicated strategy
		myMapField = Accessors.propertyAccessor(Reflections.findField(Toto.class, "myMap"));
		classMapping.remove(myMapField);
		
		setUpTestInstance();
	}
	
	static <T extends Table<T>> void setUpTestInstance() {
		// instance to test
		// The basic mapping will be altered to add special mapping for field "myListField" (a Collection) and "myMapField" (a Map)
		testInstance = new ClassMapping<>(
				Toto.class,
				(T) targetTable,
				(Map<? extends ReversibleAccessor<Toto, Object>, ? extends Column<T, Object>>) classMapping,
				Accessors.propertyAccessor(persistentFieldHarvester.getField("a")),
				// Basic mapping to prevent NullPointerException, even if it's not the goal of our test
				new AlreadyAssignedIdentifierManager<>(Integer.class, c -> {
				}, c -> false));
		
		
		// Additional mapping: the list is mapped to 2 additional columns
		int nbCol = 2;
		Set<Column<T, Object>> collectionColumn = new LinkedHashSet<>(nbCol);
		for (int i = 1; i <= nbCol; i++) {
			String columnName = "cold_" + i;
			collectionColumn.add(targetTable.addColumn(columnName, String.class));
		}
		ColumnedCollectionMapping<List<String>, String, T> columnedCollectionMapping = new ColumnedCollectionMapping<List<String>, String, T>((T) targetTable,
				collectionColumn, (Class<List<String>>) (Class) ArrayList.class) {
			
			@Override
			protected String toCollectionValue(Object object) {
				return object.toString();
			}
		};
		((ClassMapping<Toto, Integer, T>) testInstance).put(myListField, columnedCollectionMapping);
		
		// Additional mapping: the map is mapped to 2 additional columns
		Map<String, Column<T, Object>> mappedColumnsByKey = new HashMap<>();
		for (int i = 1; i <= 2; i++) {
			String columnName = "cole_" + i;
			Column<T, Object> column = targetTable.addColumn(columnName, String.class);
			switch (i) {
				case 1:
					mappedColumnsByKey.put("x", column);
					break;
				case 2:
					mappedColumnsByKey.put("y", column);
					break;
			}
		}
		ColumnedMapMapping<Map<String, String>, String, String, T> columnedMapMapping = new ColumnedMapMapping<Map<String, String>, String, String, T>(
				(T) targetTable,
				new HashSet<>(mappedColumnsByKey.values()),
				(Class<Map<String, String>>) (Class) HashMap.class) {
			
			@Override
			protected Column<T, Object> getColumn(String key) {
				Column<T, Object> column = mappedColumnsByKey.get(key);
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
			protected String toMapValue(String key, Object o) {
				return o.toString();
			}
			
			@Override
			public AbstractTransformer<Map<String, String>> copyTransformerWithAliases(ColumnedRow columnedRow) {
				return null;
			}
		};
		((ClassMapping<Toto, Integer, T>) testInstance).put(myMapField, columnedMapMapping);
		
		columnMapOnName = targetTable.mapColumnsOnName();
		colD1 = columnMapOnName.get("cold_1");
		colD2 = columnMapOnName.get("cold_2");
		colE1 = columnMapOnName.get("cole_1");
		colE2 = columnMapOnName.get("cole_2");
	}
	
	static Object[][] getInsertValues() {
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
	
	@ParameterizedTest
	@MethodSource("getInsertValues")
	void testGetInsertValues(Toto modified, Map<Column<?, Object>, Object> expectedResult) {
		Map<? extends Column<?, ?>, Object> valuesToInsert = testInstance.getInsertValues(modified);
		assertThat(valuesToInsert).isEqualTo(expectedResult);
	}
	
	static Object[][] getUpdateValues_diffOnly() {
		setUpClass();
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(1, 5, 6), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, null, null), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 42), Maps.asMap(colC, 3) },
				{ new Toto(1, null, null), new Toto(1, 2, 3), Maps.asMap(colB, null).add(colC, null) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 3), new HashMap<>() },
				{ new Toto(1, 2, 3), null, Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3, Arrays.asList("a")), new Toto(1, 5, 6, Arrays.asList("b")),
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colD1, "a") },
				{ new Toto(1, 2, 3, Maps.asMap("x", "y")), new Toto(1, 5, 6, Maps.asMap("x", "z")),
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colE1, "y") },
				{ new Toto(1, 2, 3, Arrays.asList("a"), Maps.asMap("x", "y")), null,
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colD1, "a").add(colE1, "y") },
				// 2 different entities should not return modifications
				{ new Toto(1, 2, 3), new Toto(10, 20, 30), new HashMap<>() },
			
		};
	}
	
	@ParameterizedTest
	@MethodSource("getUpdateValues_diffOnly")
	<T extends Table<T>> void getUpdateValues_diffOnly(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) {
		Map<? extends UpwhereColumn<T>, Object> valuesToUpdate = (Map) testInstance.getUpdateValues(modified, unmodified, false);
		
		assertThat(UpwhereColumn.getUpdateColumns(valuesToUpdate)).isEqualTo(expectedResult);
		if (!expectedResult.isEmpty()) {
			assertThat(UpwhereColumn.getWhereColumns(valuesToUpdate)).isEqualTo(Maps.asMap(colA, modified.a));
		} else {
			assertThat(UpwhereColumn.getWhereColumns(valuesToUpdate)).isEqualTo(new HashMap<Column, Object>());
		}
	}
	
	static Object[][] getUpdateValues_allColumns() {
		setUpClass();
		return new Object[][] {
				{ new Toto(1, 2, 3),
						new Toto(1, 5, 6),
						Maps.asMap(colB, 2).add(colC, 3).add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null) },
				{ new Toto(1, 2, 3),
						new Toto(1, null, null),
						Maps.asMap(colB, 2).add(colC, 3).add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null) },
				{ new Toto(1, 2, 3),
						new Toto(1, 2, 42),
						Maps.asMap(colB, 2).add(colC, 3).add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null) },
				{ new Toto(1, null, null),
						new Toto(1, 2, 3),
						Maps.asMap(colB, null).add(colC, null).add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null) },
				{ new Toto(1, 2, 3),
						new Toto(1, 2, 3),
						new HashMap<>() },
				{ new Toto(1, 2, 3),
						null,
						Maps.asMap(colB, 2).add(colC, 3).add(colD1, null).add(colD2, null).add(colE1, null).add(colE2, null) },
				{ new Toto(1, 2, 3, Arrays.asList("a")),
						new Toto(1, 5, 6, Arrays.asList("b")),
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colD1, "a").add(colD2, null).add(colE1, null).add(colE2, null) },
				{ new Toto(1, 2, 3, Maps.asMap("x", "y")),
						new Toto(1, 5, 6, Maps.asMap("x", "z")),
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colD1, null).add(colD2, null).add(colE1, "y").add(colE2, null) },
				{ new Toto(1, 2, 3, Arrays.asList("a"), Maps.asMap("x", "y")),
						null,
						Maps.asMap(colB, (Object) 2).add(colC, 3).add(colD1, "a").add(colD2, null).add(colE1, "y").add(colE2, null) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("getUpdateValues_allColumns")
	<T extends Table<T>> void getUpdateValues_allColumns(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) {
		Map<? extends UpwhereColumn<T>, Object> valuesToUpdate = (Map) testInstance.getUpdateValues(modified, unmodified, true);
		
		assertThat(UpwhereColumn.getUpdateColumns(valuesToUpdate)).isEqualTo(expectedResult);
		if (!expectedResult.isEmpty()) {
			assertThat(UpwhereColumn.getWhereColumns(valuesToUpdate)).isEqualTo(Maps.asMap(colA, modified.a));
		} else {
			assertThat(UpwhereColumn.getWhereColumns(valuesToUpdate)).isEqualTo(new HashMap<Column, Object>());
		}
	}
	
	@Test
	<T extends Table<T>> void beanKeyIsPresent() {
		PropertyAccessor<Toto, Integer> identifierAccesor = Accessors.propertyAccessor(persistentFieldHarvester.getField("a"));
		assertThatExceptionOfType(IllegalArgumentException.class).as("Bean identifier '" + identifierAccesor + "' must have its matching column in "
				+ "the mapping").isThrownBy(() -> new ClassMapping<>(Toto.class,
				(T) targetTable,
				(Map<? extends ReversibleAccessor<Toto, Object>, ? extends Column<T, Object>>) (Map) Maps.asMap(Accessors.propertyAccessor(Toto.class, "b"), colB),
				// identifier is not present in previous statement so it leads to the expected exception
				identifierAccesor,
				new AlreadyAssignedIdentifierManager<>(Integer.class, c -> {}, c -> false)
		));
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