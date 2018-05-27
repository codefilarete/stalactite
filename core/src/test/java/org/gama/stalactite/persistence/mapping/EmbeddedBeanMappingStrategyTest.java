package org.gama.stalactite.persistence.mapping;

import java.util.HashMap;
import java.util.Map;

import org.gama.lang.collection.Maps;
import org.gama.reflection.Accessors;
import org.gama.reflection.PropertyAccessor;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class EmbeddedBeanMappingStrategyTest {
	
	private static Table targetTable;
	private static Column<Table, Integer> colA;
	private static Column<Table, Integer> colB;
	private static Column<Table, Integer> colC;
	private static Map<PropertyAccessor<Toto, Object>, Column<Table, Integer>> classMapping;
	
	@BeforeAll
	public static void setUpClass() {
		targetTable = new Table("Toto");
		colA = targetTable.addColumn("a", Integer.class);
		colB = targetTable.addColumn("b", Integer.class);
		colC = targetTable.addColumn("c", Integer.class);
		classMapping = Maps.asMap(Accessors.forProperty(Toto.class, "a"), colA)
				.add(Accessors.forProperty(Toto.class, "b"), colB)
				.add(Accessors.forProperty(Toto.class, "c"), colC);
	}
	
	private EmbeddedBeanMappingStrategy<Toto, Table> testInstance;
	
	@BeforeEach
	public void setUp() {
		testInstance = new EmbeddedBeanMappingStrategy<Toto, Table>(Toto.class, (Map) classMapping);
	}
	
	public static Object[][] testGetInsertValuesData() {
		setUpClass();
		return new Object[][] {
				{ new Toto(1, 2, 3), Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
				{ new Toto(null, null, null), Maps.asMap(colA, null).add(colB, null).add(colC, null) },
				{ new Toto(null, 2, 3), Maps.asMap(colA, null).add(colB, 2).add(colC, 3) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testGetInsertValuesData")
	public void testGetInsertValues(Toto modified, Map<Column, Object> expectedResult) {
		Map<Column<Table, Object>, Object> valuesToInsert = testInstance.getInsertValues(modified);
		
		assertEquals(expectedResult, valuesToInsert);
	}
	
	public static Object[][] testGetUpdateValues_diffOnlyData() {
		setUpClass();
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(1, 5, 6), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, null, null), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 42), Maps.asMap(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 23, 42), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, null, null), new Toto(1, 2, 3), Maps.asMap(colB, null).add(colC, null) },
				{ new Toto(null, null, null), new Toto(null, 2, 3), Maps.asMap(colB, null).add(colC, null) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 3), new HashMap<>() },
				{ new Toto(null, null, null), new Toto(null, null, null), new HashMap<>() },
				{ new Toto(1, 2, 3), null, Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testGetUpdateValues_diffOnlyData")
	public void testGetUpdateValues_diffOnly(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) {
		Map<UpwhereColumn<Table>, Object> valuesToInsert = testInstance.getUpdateValues(modified, unmodified, false);
		
		assertEquals(expectedResult, UpwhereColumn.getUpdateColumns(valuesToInsert));
		assertEquals(new HashMap<Column, Object>(), UpwhereColumn.getWhereColumns(valuesToInsert));
	}
	
	public static Object[][] testGetUpdateValues_allColumnsData() {
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(1, 2, 42), Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
				{ new Toto(null, null, null), new Toto(null, null, null), new HashMap<>() },
				{ new Toto(1, 2, 3), null, Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 3), new HashMap<>() },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testGetUpdateValues_allColumnsData")
	public void testGetUpdateValues_allColumns(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) {
		Map<UpwhereColumn<Table>, Object> valuesToInsert = testInstance.getUpdateValues(modified, unmodified, true);
		
		assertEquals(expectedResult, UpwhereColumn.getUpdateColumns(valuesToInsert));
		assertEquals(new HashMap<Column, Object>(), UpwhereColumn.getWhereColumns(valuesToInsert));
	}
	
	@Test
	public void testTransform() {
		Row row = new Row();
		row.put("a", 1);
		row.put("b", 2);
		row.put("c", 3);
		Toto toto = testInstance.transform(row);
		assertEquals(1, (int) toto.a);
		assertEquals(2, (int) toto.b);
		assertEquals(3, (int) toto.c);
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