package org.gama.stalactite.persistence.mapping;

import java.util.HashMap;
import java.util.Map;

import org.gama.lang.Duo;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.reflection.AccessorChainMutator;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.sql.result.Row;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.gama.reflection.Accessors.*;
import static org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
		classMapping = Maps.asMap(propertyAccessor(Toto.class, "a"), colA)
				.add(propertyAccessor(Toto.class, "b"), colB)
				.add(propertyAccessor(Toto.class, "c"), colC);
	}
	
	private EmbeddedBeanMappingStrategy<Toto, Table> testInstance;
	
	@BeforeEach
	public void setUp() {
		testInstance = new EmbeddedBeanMappingStrategy<Toto, Table>(Toto.class, targetTable, (Map) classMapping);
	}
	
	public static Object[][] testGetInsertValuesData() {
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
				// null properties against a null instance : columns must be updated
				{ new Toto(null, 2, 3), null, Maps.asMap(colA, null).add(colB, 2).add(colC, 3) },
				{ new Toto(null, null, null), null, Maps.asMap(colA, null).add(colB, null).add(colC, null) },
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
	
	@Test
	public void testTransform_withNullValueInRow_returnsNotNull() {
		Row row = new Row();
		row.put("a", null);
		row.put("b", null);
		row.put("c", null);
		EmbeddedBeanMappingStrategy<Toto, Table> testInstance = new EmbeddedBeanMappingStrategy<Toto, Table>(Toto.class, targetTable, (Map) classMapping);
		Toto toto = testInstance.transform(row);
		assertNotNull(toto);
		assertNull(toto.a);
		assertNull(toto.b);
		assertNull(toto.c);
	}
	
	@Test
	public void testDefaultValueDeterminer() {
		DefaultValueDeterminer testInstance = new DefaultValueDeterminer() {};
		assertTrue(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(Toto.class, "a")), null));
		assertTrue(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(ClassWithPrimitiveTypeProperties.class, "x")), 0));
		assertTrue(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(ClassWithPrimitiveTypeProperties.class, "y")), false));
		assertTrue(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(Toto.class, "a")), null));
		assertTrue(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(ClassWithPrimitiveTypeProperties.class, "x")), 0));
		assertTrue(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(ClassWithPrimitiveTypeProperties.class, "y")), false));
		assertTrue(testInstance.isDefaultValue(new Duo<>(colA, mutatorByMethodReference(ClassWithPrimitiveTypeProperties::setX)), 0));
		assertTrue(testInstance.isDefaultValue(new Duo<>(colA, mutatorByMethodReference(ClassWithPrimitiveTypeProperties::setY)), false));
		assertTrue(testInstance.isDefaultValue(new Duo<>(colA, new AccessorChainMutator(
				Arrays.asList(accessorByMethodReference(Object::toString)), mutatorByMethodReference(String::concat))), null));
		
		assertFalse(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(Toto.class, "a")), 42));
		assertFalse(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(ClassWithPrimitiveTypeProperties.class, "x")), 42));
		assertFalse(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(ClassWithPrimitiveTypeProperties.class, "y")), true));
		assertFalse(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(Toto.class, "a")), 42));
		assertFalse(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(ClassWithPrimitiveTypeProperties.class, "x")), 42));
		assertFalse(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(ClassWithPrimitiveTypeProperties.class, "y")), true));
		assertFalse(testInstance.isDefaultValue(new Duo<>(colA, mutatorByMethodReference(ClassWithPrimitiveTypeProperties::setX)), 42));
		assertFalse(testInstance.isDefaultValue(new Duo<>(colA, mutatorByMethodReference(ClassWithPrimitiveTypeProperties::setY)), true));
		assertFalse(testInstance.isDefaultValue(new Duo<>(colA, new AccessorChainMutator(
				Arrays.asList(accessorByMethodReference(Object::toString)), mutatorByMethodReference(String::concat))), ""));
		
	}
	
	@Test
	public void testConstructor_columnFiltering() {
		Table targetTable = new Table("Toto");
		Column<Table, Integer> colA = targetTable.addColumn("a", Integer.class).primaryKey();
		Column<Table, Integer> colB = targetTable.addColumn("b", Integer.class).primaryKey().autoGenerated();
		Column<Table, Integer> colC = targetTable.addColumn("c", Integer.class);
		Map<PropertyAccessor<Toto, Object>, Column<Table, Integer>> classMapping = Maps.asMap(propertyAccessor(Toto.class, "a"), colA)
				.add(propertyAccessor(Toto.class, "b"), colB)
				.add(propertyAccessor(Toto.class, "c"), colC);
		EmbeddedBeanMappingStrategy testInstance = new EmbeddedBeanMappingStrategy<Toto, Table>(Toto.class, targetTable, (Map) classMapping);
		// primary key shall no be written by this class
		assertFalse(testInstance.getPropertyToColumn().containsValue(colA));
		assertTrue(testInstance.getRowTransformer().getColumnToMember().containsKey(colA));
		// generated keys shall no be written by this class
		assertFalse(testInstance.getPropertyToColumn().containsValue(colB));
		assertTrue(testInstance.getRowTransformer().getColumnToMember().containsKey(colB));
		// standard columns shall be written by this class
		assertTrue(testInstance.getPropertyToColumn().containsValue(colC));
		assertTrue(testInstance.getRowTransformer().getColumnToMember().containsKey(colC));
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
	
	private static class ClassWithPrimitiveTypeProperties {
		private int x;
		private boolean y;
		
		public void setX(int x) {
			this.x = x;
		}
		
		public void setY(boolean y) {
			this.y = y;
		}
	}
}