package org.codefilarete.stalactite.mapping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.mapping.Mapping.UpwhereColumn;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @author Guillaume Mary
 */
class DefaultEntityMappingTest {
	
	private static class TestData<T extends Table<T>> {
		private Column<T, ?> colA;
		private Column<T, ?> colB;
		private Column<T, ?> colC;
		private Map<? extends ReadWritePropertyAccessPoint<Toto, ?>, Column<T, ?>> propertyMapping;
		private T targetTable;
		private PersistentFieldHarvester persistentFieldHarvester;
		private Map<String, Column<T, ?>> columnMapOnName;
		private ReadWritePropertyAccessPoint<Toto, List<String>> myListField;
		private ReadWritePropertyAccessPoint<Toto, Map<String, String>> myMapField;
		private DefaultEntityMapping<Toto, Integer, ?> entityMapping;
		
		private TestData() {
			persistentFieldHarvester = new PersistentFieldHarvester();
			targetTable = (T) new Table("Toto");
			propertyMapping = persistentFieldHarvester.mapFields(Toto.class, targetTable);
			
			
			columnMapOnName = targetTable.mapColumnsOnName();
			colA = columnMapOnName.get("a");
			colA.setPrimaryKey(true);
			colB = columnMapOnName.get("b");
			colC = columnMapOnName.get("c");
			
			// Replacing default mapping for the List (attribute myList) by a dedicated strategy
			myListField = Accessors.propertyAccessor(Reflections.findField(Toto.class, "myList"));
			propertyMapping.remove(myListField);
			// Replacing default mapping for the Map (attribute myMap) by a dedicated strategy
			myMapField = Accessors.propertyAccessor(Reflections.findField(Toto.class, "myMap"));
			propertyMapping.remove(myMapField);
			
			// instance to test
			// The basic mapping will be altered to add special mapping for field "myListField" (a Collection) and "myMapField" (a Map)
			entityMapping = new DefaultEntityMapping<>(
					Toto.class,
					targetTable,
					propertyMapping,
					Accessors.propertyAccessor(persistentFieldHarvester.getField("a")),
					// Basic mapping to prevent NullPointerException, even if it's not the goal of our test
					new AlreadyAssignedIdentifierManager<>(Integer.class, c -> {
					}, c -> false));
			
			columnMapOnName = targetTable.mapColumnsOnName();
		}
	}
	
	private static Column colA;
	private static Column colB;
	private static Column colC;
	private static DefaultEntityMapping testInstance;
	
	
	static <T extends Table<T>> void setUpInstance() {
		TestData<T> testData = new TestData<>();
		
		colA = testData.colA;
		colB = testData.colB;
		colC = testData.colC;
		testInstance = testData.entityMapping;
	}
	
	static Object[][] getInsertValues() {
		setUpInstance();
		return new Object[][] {
				{ new Toto(1, 2, 3), Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
				{ new Toto(null, null, null), Maps.asMap(colA, null).add(colB, null).add(colC, null) },
				{ new Toto(null, 2, 3), Maps.asMap(colA, null).add(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3, Arrays.asList("a")), Maps.asMap(colA, (Object) 1).add(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3, Maps.asMap("x", "y")), Maps.asMap(colA, (Object) 1).add(colB, 2).add(colC, 3) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("getInsertValues")
	void testGetInsertValues(Toto modified, Map<Column<?, Object>, Object> expectedResult) {
		Map<? extends Column<?, ?>, ?> valuesToInsert = testInstance.getInsertValues(modified);
		assertThat(valuesToInsert).isEqualTo(expectedResult);
	}
	
	static Object[][] getUpdateValues_diffOnly() {
		setUpInstance();
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(1, 5, 6), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, null, null), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 42), Maps.asMap(colC, 3) },
				{ new Toto(1, null, null), new Toto(1, 2, 3), Maps.asMap(colB, null).add(colC, null) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 3), new HashMap<>() },
				{ new Toto(1, 2, 3), null, Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3, Arrays.asList("a")), new Toto(1, 5, 6, Arrays.asList("b")),
						Maps.asMap(colB, (Object) 2).add(colC, 3) },
				{ new Toto(1, 2, 3, Maps.asMap("x", "y")), new Toto(1, 5, 6, Maps.asMap("x", "z")),
						Maps.asMap(colB, (Object) 2).add(colC, 3) },
				{ new Toto(1, 2, 3, Arrays.asList("a"), Maps.asMap("x", "y")), null,
						Maps.asMap(colB, (Object) 2).add(colC, 3) },
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
		setUpInstance();
		return new Object[][] {
				{ new Toto(1, 2, 3),
						new Toto(1, 5, 6),
						Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3),
						new Toto(1, null, null),
						Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3),
						new Toto(1, 2, 42),
						Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, null, null),
						new Toto(1, 2, 3),
						Maps.asMap(colB, null).add(colC, null) },
				{ new Toto(1, 2, 3),
						new Toto(1, 2, 3),
						new HashMap<>() },
				{ new Toto(1, 2, 3),
						null,
						Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3, Arrays.asList("a")),
						new Toto(1, 5, 6, Arrays.asList("b")),
						Maps.asMap(colB, (Object) 2).add(colC, 3) },
				{ new Toto(1, 2, 3, Maps.asMap("x", "y")),
						new Toto(1, 5, 6, Maps.asMap("x", "z")),
						Maps.asMap(colB, (Object) 2).add(colC, 3) },
				{ new Toto(1, 2, 3, Arrays.asList("a"), Maps.asMap("x", "y")),
						null,
						Maps.asMap(colB, (Object) 2).add(colC, 3) },
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
		TestData<T> testData = new TestData<>();
		ReadWritePropertyAccessPoint<Toto, Integer> identifierAccessor = Accessors.propertyAccessor(testData.persistentFieldHarvester.getField("a"));
		assertThatCode(() -> new DefaultEntityMapping<>(Toto.class,
				testData.targetTable,
				(Map<? extends ReadWritePropertyAccessPoint<Toto, ?>, ? extends Column<T, ?>>) (Map) Maps.asMap(Accessors.propertyAccessor(Toto.class, "b"), colB),
				// identifier is not present in previous statement so it leads to the expected exception
				identifierAccessor,
				new AlreadyAssignedIdentifierManager<>(Integer.class, c -> {}, c -> false)
		)).isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Bean identifier 'o.c.s.m.DefaultEntityMappingTest$Toto.a' must have its matching column in the mapping");
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
