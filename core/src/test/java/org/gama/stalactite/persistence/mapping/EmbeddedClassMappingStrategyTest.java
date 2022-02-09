package org.codefilarete.stalactite.persistence.mapping;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.reflection.AccessorChainMutator;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.persistence.mapping.MappingStrategy.UpwhereColumn;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.reflection.Accessors.accessorByMethodReference;
import static org.codefilarete.reflection.Accessors.mutatorByField;
import static org.codefilarete.reflection.Accessors.mutatorByMethodReference;
import static org.codefilarete.reflection.Accessors.propertyAccessor;
import static org.codefilarete.stalactite.persistence.mapping.EmbeddedClassMappingStrategy.*;

/**
 * @author Guillaume Mary
 */
public class EmbeddedClassMappingStrategyTest {
	
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
	
	private EmbeddedClassMappingStrategy<Toto, Table> testInstance;
	
	@BeforeEach
	public void setUp() {
		testInstance = new EmbeddedClassMappingStrategy<Toto, Table>(Toto.class, targetTable, (Map) classMapping);
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
		
		assertThat(valuesToInsert).isEqualTo(expectedResult);
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
		
		assertThat(UpwhereColumn.getUpdateColumns(valuesToInsert)).isEqualTo(expectedResult);
		assertThat(UpwhereColumn.getWhereColumns(valuesToInsert)).isEqualTo(new HashMap<Column, Object>());
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
		
		assertThat(UpwhereColumn.getUpdateColumns(valuesToInsert)).isEqualTo(expectedResult);
		assertThat(UpwhereColumn.getWhereColumns(valuesToInsert)).isEqualTo(new HashMap<Column, Object>());
	}
	
	@Test
	public void testTransform() {
		Row row = new Row();
		row.put("a", 1);
		row.put("b", 2);
		row.put("c", 3);
		Toto toto = testInstance.transform(row);
		assertThat((int) toto.a).isEqualTo(1);
		assertThat((int) toto.b).isEqualTo(2);
		assertThat((int) toto.c).isEqualTo(3);
	}
	
	@Test
	public void testTransform_withNullValueInRow_returnsNotNull() {
		Row row = new Row();
		row.put("a", null);
		row.put("b", null);
		row.put("c", null);
		EmbeddedClassMappingStrategy<Toto, Table> testInstance = new EmbeddedClassMappingStrategy<Toto, Table>(Toto.class, targetTable, (Map) classMapping);
		Toto toto = testInstance.transform(row);
		assertThat(toto).isNotNull();
		assertThat(toto.a).isNull();
		assertThat(toto.b).isNull();
		assertThat(toto.c).isNull();
	}
	
	@Test
	public void testDefaultValueDeterminer() {
		DefaultValueDeterminer testInstance = new DefaultValueDeterminer() {};
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(Toto.class, "a")), null)).isTrue();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(ClassWithPrimitiveTypeProperties.class, "x")), 0)).isTrue();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(ClassWithPrimitiveTypeProperties.class, "y")), false)).isTrue();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(Toto.class, "a")), null)).isTrue();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(ClassWithPrimitiveTypeProperties.class, "x")), 0)).isTrue();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(ClassWithPrimitiveTypeProperties.class, "y")), false)).isTrue();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, mutatorByMethodReference(ClassWithPrimitiveTypeProperties::setX)), 0)).isTrue();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, mutatorByMethodReference(ClassWithPrimitiveTypeProperties::setY)), false)).isTrue();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, new AccessorChainMutator(
				Arrays.asList(accessorByMethodReference(Object::toString)), mutatorByMethodReference(String::concat))), null)).isTrue();
		
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(Toto.class, "a")), 42)).isFalse();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(ClassWithPrimitiveTypeProperties.class, "x")), 42)).isFalse();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, propertyAccessor(ClassWithPrimitiveTypeProperties.class, "y")), true)).isFalse();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(Toto.class, "a")), 42)).isFalse();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(ClassWithPrimitiveTypeProperties.class, "x")), 42)).isFalse();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, mutatorByField(ClassWithPrimitiveTypeProperties.class, "y")), true)).isFalse();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, mutatorByMethodReference(ClassWithPrimitiveTypeProperties::setX)), 42)).isFalse();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, mutatorByMethodReference(ClassWithPrimitiveTypeProperties::setY)), true)).isFalse();
		assertThat(testInstance.isDefaultValue(new Duo<>(colA, new AccessorChainMutator(
				Arrays.asList(accessorByMethodReference(Object::toString)), mutatorByMethodReference(String::concat))), "")).isFalse();
		
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
		EmbeddedClassMappingStrategy testInstance = new EmbeddedClassMappingStrategy<Toto, Table>(Toto.class, targetTable, (Map) classMapping);
		// primary key shall not be written by this class
		assertThat(testInstance.getInsertableColumns().contains(colA)).isTrue();
		assertThat(testInstance.getUpdatableColumns().contains(colA)).isFalse();
		assertThat(testInstance.getRowTransformer().getColumnToMember().containsKey(colA)).isTrue();
		// generated keys shall not be written by this class
		assertThat(testInstance.getInsertableColumns().contains(colB)).isFalse();
		assertThat(testInstance.getUpdatableColumns().contains(colB)).isFalse();
		assertThat(testInstance.getRowTransformer().getColumnToMember().containsKey(colB)).isTrue();
		// standard columns shall be written by this class
		assertThat(testInstance.getInsertableColumns().contains(colC)).isTrue();
		assertThat(testInstance.getUpdatableColumns().contains(colC)).isTrue();
		assertThat(testInstance.getRowTransformer().getColumnToMember().containsKey(colC)).isTrue();
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