package org.gama.stalactite.persistence.mapping;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.Maps.ChainingMap;
import org.gama.stalactite.persistence.mapping.MappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class ColumnedMapMappingStrategyTest {
	
	private static Table totoTable;
	private static Column col1;
	private static Column col2;
	private static Column col3;
	private static Column col4;
	private static Column col5;
	private static Map<Integer, Column> columnToKey;
	private static Map<Column, Integer> keyToColumn;
	
	public static void setUpClass() {
		totoTable = new Table(null, "Toto");
		final int nbCol = 5;
		columnToKey = new HashMap<>();
		keyToColumn = new HashMap<>();
		for (int i = 1; i <= nbCol; i++) {
			String columnName = "col_" + i;
			Column column = totoTable.addColumn(columnName, String.class);
			columnToKey.put(i, column);
			keyToColumn.put(column, i);
		}
		Map<String, Column> namedColumns = totoTable.mapColumnsOnName();
		col1 = namedColumns.get("col_1");
		col1.setPrimaryKey(true);
		col2 = namedColumns.get("col_2");
		col3 = namedColumns.get("col_3");
		col4 = namedColumns.get("col_4");
		col5 = namedColumns.get("col_5");
	}
	
	private ColumnedMapMappingStrategy<Map<Integer, String>, Integer, String, Table> testInstance;
	
	@BeforeEach
	public void setUp() {
		testInstance = new ColumnedMapMappingStrategy<Map<Integer, String>, Integer, String, Table>(totoTable, totoTable.getColumns(), (Class<Map<Integer, String>>) (Class) HashMap.class) {
			@Override
			protected Column getColumn(Integer key) {
				if (key > 5) {
					throw new IllegalArgumentException("Unknown key " + key);
				}
				return columnToKey.get(key);
			}
			
			@Override
			protected Integer getKey(Column column) {
				return keyToColumn.get(column);
			}
			
			@Override
			protected String toDatabaseValue(Integer key, String s) {
				return s;
			}
			
			@Override
			protected String toMapValue(Integer key, Object s) {
				return s == null ? null : s.toString();
			}
			
			@Override
			public AbstractTransformer<Map<Integer, String>> copyTransformerWithAliases(ColumnedRow columnedRow) {
				return null;
			}
		};
		
	}
	
	public static Object[][] testGetInsertValuesData() {
		setUpClass();
		return new Object[][] {
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "a").add(2, "b").add(3, null), Maps.asMap(col1, "a").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ null, Maps.asMap(col1, null).add(col2, null).add(col3, null).add(col4, null).add(col5, null) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testGetInsertValuesData")
	public void testGetInsertValues(ChainingMap<Integer, String> toInsert, ChainingMap<Column, String> expected) {
		Map<Column<Table, Object>, Object> insertValues = testInstance.getInsertValues(toInsert);
		assertThat(insertValues).isEqualTo(expected);
	}
	
	public static Object[][] testGetUpdateValues_diffOnlyData() {
		setUpClass();
		return new Object[][] {
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c") },
				{ Maps.asMap(1, "a").add(2, "b"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, null) },
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(1, "x").add(2, "y"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c") },
				{ Maps.asMap(1, "x").add(2, "b"), Maps.asMap(1, "x").add(2, "y"),
						Maps.asMap(col2, "b") },
				{ Maps.asMap(1, "x").add(2, "b"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col2, "b").add(col3, null) },
				{ Maps.asMap(1, "x").add(2, "b"), null,
						Maps.asMap(col1, "x").add(col2, "b") },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testGetUpdateValues_diffOnlyData")
	public void testGetUpdateValues_diffOnly(HashMap<Integer, String> modified, HashMap<Integer, String> unmodified, Map<Column, String> expected) {
		Map<UpwhereColumn<Table>, Object> updateValues = testInstance.getUpdateValues(modified, unmodified, false);
		Map<UpwhereColumn, Object> expectationWithUpwhereColumn = new HashMap<>();
		expected.forEach((c, s) -> expectationWithUpwhereColumn.put(new UpwhereColumn(c, true), s));
		assertThat(updateValues).isEqualTo(expectationWithUpwhereColumn);
	}
	
	public static Object[][] testGetUpdateValues_allColumnsData() {
		setUpClass();
		return new Object[][] {
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "a").add(2, "b"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(1, "x").add(2, "y"),
						Maps.asMap(col1, "a").add(col2, "b").add(col3, "c").add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "x").add(2, "b"), Maps.asMap(1, "x").add(2, "y"),
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "x").add(2, "b"), Maps.asMap(1, "x").add(2, "y").add(3, "z"),
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "x").add(2, "b"), null,
						Maps.asMap(col1, "x").add(col2, "b").add(col3, null).add(col4, null).add(col5, null) },
				{ Maps.asMap(1, "a").add(2, "b").add(3, "c"), Maps.asMap(1, "a").add(2, "b").add(3, "c"),
						new HashMap<>()},
		};
	}
	
	@ParameterizedTest
	@MethodSource("testGetUpdateValues_allColumnsData")
	public void testGetUpdateValues_allColumns(HashMap<Integer, String> modified, HashMap<Integer, String> unmodified, Map<Column, String> expected) {
		Map<UpwhereColumn<Table>, Object> updateValues = testInstance.getUpdateValues(modified, unmodified, true);
		Map<UpwhereColumn, Object> expectationWithUpwhereColumn = new HashMap<>();
		expected.forEach((c, s) -> expectationWithUpwhereColumn.put(new UpwhereColumn(c, true), s));
		assertThat(updateValues).isEqualTo(expectationWithUpwhereColumn);
	}
	
	@Test
	public void testTransform() {
		Row row = new Row();
		row.put(col1.getName(), "a");
		row.put(col2.getName(), "b");
		row.put(col3.getName(), "c");
		Map<Integer, String> toto = testInstance.transform(row);
		assertThat(toto.get(1)).isEqualTo("a");
		assertThat(toto.get(2)).isEqualTo("b");
		assertThat(toto.get(3)).isEqualTo("c");
		assertThat(toto.containsKey(4)).isTrue();
		assertThat(toto.get(4)).isNull();
		assertThat(toto.containsKey(5)).isTrue();
		assertThat(toto.get(5)).isNull();
		// there's not more element since mapping used 5 columns
		assertThat(toto.size()).isEqualTo(5);
	}
}