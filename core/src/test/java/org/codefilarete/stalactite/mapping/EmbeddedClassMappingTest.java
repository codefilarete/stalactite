package org.codefilarete.stalactite.mapping;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.AccessorChainMutator;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.mapping.Mapping.UpwhereColumn;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.reflection.Accessors.*;
import static org.codefilarete.stalactite.mapping.EmbeddedClassMapping.DefaultValueDeterminer;

/**
 * @author Guillaume Mary
 */
class EmbeddedClassMappingTest {
	
	private static Table targetTable;
	private static Column<Table, Integer> colA;
	private static Column<Table, Integer> colB;
	private static Column<Table, Integer> colC;
	private static Map<PropertyAccessor<Toto, Object>, Column<Table, Object>> classMapping;
	
	@BeforeAll
	static void setUpClass() {
		targetTable = new Table("Toto");
		colA = targetTable.addColumn("a", Integer.class);
		colB = targetTable.addColumn("b", Integer.class);
		colC = targetTable.addColumn("c", Integer.class);
		classMapping = (Map) Maps
				.forHashMap((Class<PropertyAccessor<Toto, Object>>) null, (Class<Column<Table, ?>>) null)
				.add(propertyAccessor(Toto.class, "a"), colA)
				.add(propertyAccessor(Toto.class, "b"), colB)
				.add(propertyAccessor(Toto.class, "c"), colC);
	}
	
	private EmbeddedClassMapping<Toto, ?> testInstance;
	
	@BeforeEach
	void setUp() {
		testInstance = new EmbeddedClassMapping<>(Toto.class, targetTable, classMapping);
	}
	
	static Object[][] getInsertValues() {
		return new Object[][] {
				{ new Toto(1, 2, 3), Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
				{ new Toto(null, null, null), Maps.asMap(colA, null).add(colB, null).add(colC, null) },
				{ new Toto(null, 2, 3), Maps.asMap(colA, null).add(colB, 2).add(colC, 3) },
		};
	}
	
	@ParameterizedTest
	@MethodSource
	void getInsertValues(Toto modified, Map<Column, Object> expectedResult) {
		Map<? extends Column<?, ?>, Object> valuesToInsert = testInstance.getInsertValues(modified);
		
		assertThat(valuesToInsert).isEqualTo(expectedResult);
	}
	
	static Object[][] getUpdateValues_onlyNecessaryColumns() {
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
	@MethodSource
	<T extends Table<T>> void getUpdateValues_onlyNecessaryColumns(Toto modified, Toto unmodified, Map<Column<T, Object>, Object> expectedResult) {
		Map<? extends UpwhereColumn<T>, Object> valuesToInsert = (Map) testInstance.getUpdateValues(modified, unmodified, false);
		assertThat(UpwhereColumn.getUpdateColumns(valuesToInsert)).isEqualTo(expectedResult);
		assertThat(UpwhereColumn.getWhereColumns(valuesToInsert)).isEmpty();
	}
	
	static Object[][] getUpdateValues_allColumns() {
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(1, 2, 42), Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
				{ new Toto(null, null, null), new Toto(null, null, null), new HashMap<>() },
				{ new Toto(1, 2, 3), null, Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 3), new HashMap<>() },
		};
	}
	
	@ParameterizedTest
	@MethodSource
	<T extends Table<T>> void getUpdateValues_allColumns(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) {
		Map<? extends UpwhereColumn<T>, Object> valuesToInsert = (Map) testInstance.getUpdateValues(modified, unmodified, true);
		assertThat(UpwhereColumn.getUpdateColumns(valuesToInsert)).isEqualTo(expectedResult);
		assertThat(UpwhereColumn.getWhereColumns(valuesToInsert)).isEqualTo(new HashMap<Column, Object>());
	}
	
	@Test
	<T extends Table<T>> void getUpdateValues_withModificationOnBeanAnNoModificationInShadowColumns() {
		Column<T, String> myShadowColumn = targetTable.addColumn("myShadowColumn", String.class);
		EmbeddedClassMapping<Toto, T> testInstance = new EmbeddedClassMapping(Toto.class, targetTable, classMapping);
		// modification on bean
		Toto modified = new Toto(1, 2, 42);
		Toto unmodified = new Toto(1, 2, 3);
		testInstance.addShadowColumnUpdate(new Mapping.ShadowColumnValueProvider<Toto, T>() {
			@Override
			public Set<Column<T, ?>> getColumns() {
				return Arrays.asSet(myShadowColumn);
			}
			
			@Override
			public Map<Column<T, ?>, ?> giveValue(Toto bean) {
				// no modification on shadow column : whatever bean (modified or not), value is the same
				return Maps.asMap(myShadowColumn, "a");
			}
		});
		
		Map<Column, Object> expectedResult = Maps.forHashMap(Column.class, Object.class)
				.add(colA, 1).add(colB, 2).add(colC, 42)
				.add(myShadowColumn, "a");
		Map<? extends UpwhereColumn<T>, Object> valuesToUpdate = testInstance.getUpdateValues(modified, unmodified, true);
		assertThat(UpwhereColumn.getUpdateColumns(valuesToUpdate)).isEqualTo(expectedResult);
		assertThat(UpwhereColumn.getWhereColumns(valuesToUpdate)).isEqualTo(new HashMap<Column, Object>());
	}
	
	@Test
	<T extends Table<T>> void getUpdateValues_withModificationOnBeanAnModificationInShadowColumns() {
		Column<T, String> myShadowColumn = targetTable.addColumn("myShadowColumn", String.class);
		EmbeddedClassMapping<Toto, T> testInstance = new EmbeddedClassMapping(Toto.class, targetTable, classMapping);
		// modification on bean
		Toto modified = new Toto(1, 2, 42);
		Toto unmodified = new Toto(1, 2, 3);
		testInstance.addShadowColumnUpdate(new Mapping.ShadowColumnValueProvider<Toto, T>() {
			@Override
			public Set<Column<T, ?>> getColumns() {
				return Arrays.asSet(myShadowColumn);
			}
			
			@Override
			public Map<Column<T, ?>, ?> giveValue(Toto bean) {
				// modification on shadow column
				if (bean == modified) {
					return Maps.asMap(myShadowColumn, "b");
				}
				if (bean == unmodified) {
					return Maps.asMap(myShadowColumn, "a");
				}
				return null;
			}
		});
		
		Map<Column, Object> expectedResult = Maps.forHashMap(Column.class, Object.class)
				.add(colA, 1).add(colB, 2).add(colC, 42)
				.add(myShadowColumn, "b");
		Map<? extends UpwhereColumn<T>, Object> valuesToUpdate = testInstance.getUpdateValues(modified, unmodified, true);
		assertThat(UpwhereColumn.getUpdateColumns(valuesToUpdate)).isEqualTo(expectedResult);
		assertThat(UpwhereColumn.getWhereColumns(valuesToUpdate)).isEqualTo(new HashMap<Column, Object>());
	}
	
	@Test
	<T extends Table<T>> void getUpdateValues_withNoModificationOnBeanAnModificationInShadowColumns() {
		Column<T, String> myShadowColumn = targetTable.addColumn("myShadowColumn", String.class);
		EmbeddedClassMapping<Toto, T> testInstance = new EmbeddedClassMapping(Toto.class, targetTable, classMapping);
		// no modification on bean
		Toto modified = new Toto(1, 2, 3);
		Toto unmodified = new Toto(1, 2, 3);
		testInstance.addShadowColumnUpdate(new Mapping.ShadowColumnValueProvider<Toto, T>() {
			@Override
			public Set<Column<T, ?>> getColumns() {
				return Arrays.asSet(myShadowColumn);
			}
			
			@Override
			public Map<Column<T, ?>, ?> giveValue(Toto bean) {
				// modification on shadow column
				if (bean == modified) {
					return Maps.asMap(myShadowColumn, "b");
				}
				if (bean == unmodified) {
					return Maps.asMap(myShadowColumn, "a");
				}
				return null;
			}
		});
		
		Map<Column, Object> expectedResult = Maps.forHashMap(Column.class, Object.class)
				.add(colA, 1).add(colB, 2).add(colC, 3)
				.add(myShadowColumn, "b");
		Map<? extends UpwhereColumn<T>, Object> valuesToUpdate = testInstance.getUpdateValues(modified, unmodified, true);
		assertThat(UpwhereColumn.getUpdateColumns(valuesToUpdate)).isEqualTo(expectedResult);
		assertThat(UpwhereColumn.getWhereColumns(valuesToUpdate)).isEqualTo(new HashMap<Column, Object>());
	}
	
	@Test
	<T extends Table<T>> void getUpdateValues_withNoModificationOnBeanAnNoModificationInShadowColumns() {
		Column<T, String> myShadowColumn = targetTable.addColumn("myShadowColumn", String.class);
		EmbeddedClassMapping<Toto, T> testInstance = new EmbeddedClassMapping(Toto.class, targetTable, classMapping);
		// no modification on bean
		Toto modified = new Toto(1, 2, 3);
		Toto unmodified = new Toto(1, 2, 3);
		testInstance.addShadowColumnUpdate(new Mapping.ShadowColumnValueProvider<Toto, T>() {
			@Override
			public Set<Column<T, ?>> getColumns() {
				return Arrays.asSet(myShadowColumn);
			}
			
			@Override
			public Map<Column<T, ?>, ?> giveValue(Toto bean) {
				// no modification on shadow column : whatever bean (modified or not), value is the same
				return Maps.asMap(myShadowColumn, "a");
			}
		});
		
		Map<? extends UpwhereColumn<T>, Object> valuesToUpdate = testInstance.getUpdateValues(modified, unmodified, true);
		assertThat(UpwhereColumn.getUpdateColumns(valuesToUpdate)).isEmpty();
		assertThat(UpwhereColumn.getWhereColumns(valuesToUpdate)).isEmpty();
	}
	
	@Test
	void transform() {
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
	void transform_withNullValueInRow_returnsNotNull() {
		Row row = new Row();
		row.put("a", null);
		row.put("b", null);
		row.put("c", null);
		EmbeddedClassMapping<Toto, ?> testInstance = new EmbeddedClassMapping<>(Toto.class, targetTable, (Map) classMapping);
		Toto toto = testInstance.transform(row);
		assertThat(toto).isNotNull();
		assertThat(toto.a).isNull();
		assertThat(toto.b).isNull();
		assertThat(toto.c).isNull();
	}
	
	@Test
	void defaultValueDeterminer() {
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
	void constructor_columnFiltering() {
		Table targetTable = new Table("Toto");
		Column<Table, Integer> colA = targetTable.addColumn("a", Integer.class).primaryKey();
		Column<Table, Integer> colB = targetTable.addColumn("b", Integer.class).primaryKey().autoGenerated();
		Column<Table, Integer> colC = targetTable.addColumn("c", Integer.class);
		Map<PropertyAccessor<Toto, Object>, Column<Table, Object>> classMapping = (Map) Maps
				.forHashMap((Class<PropertyAccessor<Toto, Object>>) null, (Class<Column<Table, ?>>) null)
				.add(propertyAccessor(Toto.class, "a"), colA)
				.add(propertyAccessor(Toto.class, "b"), colB)
				.add(propertyAccessor(Toto.class, "c"), colC);
		EmbeddedClassMapping<Toto, ?> testInstance = new EmbeddedClassMapping<>(Toto.class, targetTable, classMapping);
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