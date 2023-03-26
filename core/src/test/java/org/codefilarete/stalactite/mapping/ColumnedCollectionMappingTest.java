package org.codefilarete.stalactite.mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.Maps.ChainingMap;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.mapping.Mapping.UpwhereColumn;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class ColumnedCollectionMappingTest {
	
	private static Table targetTable;
	private static Column col1;
	private static Column col2;
	private static Column col3;
	private static Column col4;
	private static Column col5;
	
	@BeforeAll
	static void setUpClass() {
		targetTable = new Table<>(null, "Toto");
		int nbCol = 5;
		for (int i = 1; i <= nbCol; i++) {
			String columnName = "col_" + i;
			targetTable.addColumn(columnName, String.class);
		}
		Map<String, Column> namedColumns = targetTable.mapColumnsOnName();
		col1 = namedColumns.get("col_1");
		col1.setPrimaryKey(true);
		col2 = namedColumns.get("col_2");
		col3 = namedColumns.get("col_3");
		col4 = namedColumns.get("col_4");
		col5 = namedColumns.get("col_5");
	}
	
	private ColumnedCollectionMapping<List<String>, String, ?> testInstance;
	
	@BeforeEach
	<T extends Table<T>> void setUp() {
		testInstance = new ColumnedCollectionMapping<List<String>, String, T>((T) targetTable, targetTable.getColumns(), (Class<List<String>>) (Class) ArrayList.class) {
			@Override
			protected String toCollectionValue(Object object) {
				return object == null ?  null : object.toString();
			}
		};
	}
	
	
	static Object[][] getInsertValues() {
		setUpClass();
		return new Object[][] {
				{ Arrays.asList("a", "b", "c"), Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Arrays.asList("a", "b", null), Maps.asMap(col1, "a").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ null, Maps.asMap(col1, null).add(col2, null).add(col3, null).add(col4, null).add(col5, null) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("getInsertValues")
	void getInsertValues(List<String> toInsert, ChainingMap<Column, String> expected) {
		Map<? extends Column<?, Object>, Object> insertValues = testInstance.getInsertValues(toInsert);
		assertThat(expected).isEqualTo(insertValues);
	}
	
	static Object[][] getUpdateValues_diffOnly() {
		setUpClass();
		return new Object[][] {
				{ Arrays.asList("a", "b", "c"), Arrays.asList("x", "y", "x"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c") },
				{ Arrays.asList("a", "b"), Arrays.asList("x", "y", "x"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, null) },
				{ Arrays.asList("a", "b", "c"), Arrays.asList("x", "y"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c") },
				{ Arrays.asList("x", "b"), Arrays.asList("x", "y"),
						Maps.asMap(col2, "b") },
				{ Arrays.asList("x", "b", null), Arrays.asList("x", "y", "z"),
						Maps.asMap(col2, "b").add(col3, null) },
				{ Arrays.asList("x", "b", null), null,
						Maps.asMap(col1, "x").add(col2, "b") },
		};
	}
	
	@ParameterizedTest
	@MethodSource("getUpdateValues_diffOnly")
	void testGetUpdateValues_diffOnly(List<String> modified, List<String> unmodified, Map<Column, String> expected) {
		Map<? extends UpwhereColumn<?>, Object> updateValues = testInstance.getUpdateValues(modified, unmodified, false);
		Map<UpwhereColumn, Object> expectationWithUpwhereColumn = new HashMap<>();
		expected.forEach((c, s) -> expectationWithUpwhereColumn.put(new UpwhereColumn(c, true), s));
		assertThat(updateValues).isEqualTo(expectationWithUpwhereColumn);
	}
	
	static Object[][] getUpdateValues_allColumns() {
		setUpClass();
		return new Object[][] {
				{ Arrays.asList("a", "b", "c"), Arrays.asList("x", "y", "x"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Arrays.asList("a", "b"), Arrays.asList("x", "y", "x"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Arrays.asList("a", "b", "c"), Arrays.asList("x", "y"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Arrays.asList("x", "b"), Arrays.asList("x", "y"),
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Arrays.asList("x", "b", null), Arrays.asList("x", "y", "z"),
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Arrays.asList("x", "b", null), null,
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Arrays.asList("a", "b", "c"), Arrays.asList("a", "b", "c"),
						new HashMap<>() },
		};
	}
	
	@ParameterizedTest
	@MethodSource("getUpdateValues_allColumns")
	void getUpdateValues_allColumns(List<String> modified, List<String> unmodified, Map<Column, String> expected) {
		Map<? extends UpwhereColumn<?>, Object> updateValues = testInstance.getUpdateValues(modified, unmodified, true);
		Map<UpwhereColumn, Object> expectationWithUpwhereColumn = new HashMap<>();
		expected.forEach((c, s) -> expectationWithUpwhereColumn.put(new UpwhereColumn(c, true), s));
		assertThat(updateValues).isEqualTo(expectationWithUpwhereColumn);
	}
	
	@Test
	void transform() {
		Row row = new Row();
		row.put(col1.getName(), "a");
		row.put(col2.getName(), "b");
		row.put(col3.getName(), "c");
		List<String> toto = testInstance.transform(row);
		// all 5th first element should be filled
		assertThat(toto.get(0)).isEqualTo("a");
		assertThat(toto.get(1)).isEqualTo("b");
		assertThat(toto.get(2)).isEqualTo("c");
		assertThat(toto.get(3)).isNull();
		assertThat(toto.get(4)).isNull();
		// there's not more element since mapping used 5 columns
		assertThat(toto.size()).isEqualTo(5);
	}
}