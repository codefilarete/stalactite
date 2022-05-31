package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.query.builder.IdentityMap;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.INNER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntityTreeQueryBuilderTest {
	
	@Test
	void buildSelectQuery_robustToSeveralJoinsUsingSameTable() {
		ClassMapping totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		Column totoTata1IdColumn = totoTable.addColumn("tata1Id", String.class);
		Column totoTata2IdColumn = totoTable.addColumn("tata2Id", String.class);
		
		ClassMapping tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tata1NodeName = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME,
															  new EntityMappingAdapter(tataMappingMock), totoTata1IdColumn, tataPrimaryKey, "x", INNER, null, Collections.emptySet());
		String tata2NodeName = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME,
															  new EntityMappingAdapter(tataMappingMock), totoTata2IdColumn, tataPrimaryKey, "y", INNER, null, Collections.emptySet());
		
		IdentityMap<JoinNode, Table> tableCloneMap = new IdentityMap<>();
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree, c -> mock(ParameterBinder.class)) {
			@Override
			Table cloneTable(JoinNode joinNode) {
				Table tableClone = super.cloneTable(joinNode);
				tableCloneMap.put(joinNode, tableClone);
				return tableClone;
			}
		};
		
		EntityTreeQuery<?> entityTreeQuery = testInstance.buildSelectQuery();
		QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(entityTreeQuery.getQuery());
		assertThat(sqlQueryBuilder.toSQL()).isEqualTo("select"
				+ " Toto.id as Toto_id, Toto.name as Toto_name"
				+ ", Toto.tata1Id as Toto_tata1Id, Toto.tata2Id as Toto_tata2Id"
				+ ", x.id as x_id, x.name as x_name"
				+ ", y.id as y_id, y.name as y_name"
				+ " from Toto"
				+ " inner join Tata as x on Toto.tata1Id = x.id"
				+ " inner join Tata as y on Toto.tata2Id = y.id");
		
		// checking root aliases computation
		JoinNode root = entityJoinTree.getRoot();
		Map<String, Column> totoTableClone = tableCloneMap.get(root).mapColumnsOnName();
		Map<String, String> actualTotoJoinAliases = new HashMap<>();
		totoTableClone.forEach((columnName, column) -> actualTotoJoinAliases.put(columnName, entityTreeQuery.getColumnAliases().get(column)));
		
		Map<String, String> expectedAliases = Maps.forHashMap(String.class, String.class)
				.add("id", "Toto_id")
				.add("name", "Toto_name")
				.add("tata1Id", "Toto_tata1Id")
				.add("tata2Id", "Toto_tata2Id");
		
		org.assertj.core.api.Assertions.assertThat(actualTotoJoinAliases).isEqualTo(expectedAliases);
		
		
		// checking tata1 aliases computation
		JoinNode tata1JoinNode = entityJoinTree.getJoin(tata1NodeName);
		
		Map<String, Column> tata1TableClone = tableCloneMap.get(tata1JoinNode).mapColumnsOnName();
		Map<String, String> actualTata1JoinAliases = new HashMap<>();
		tata1TableClone.forEach((columnName, column) -> actualTata1JoinAliases.put(columnName, entityTreeQuery.getColumnAliases().get(column)));
		
		Map<String, String> expectedAliases1 = Maps.forHashMap(String.class, String.class)
				.add("id", "x_id")
				.add("name", "x_name");
		
		org.assertj.core.api.Assertions.assertThat(actualTata1JoinAliases).isEqualTo(expectedAliases1);
		
		// checking tata2 aliases computation
		JoinNode tata2JoinNode = entityJoinTree.getJoin(tata2NodeName);
		Map<String, Column> tata2TableClone = tableCloneMap.get(tata2JoinNode).mapColumnsOnName();
		Map<String, String> actualTata2JoinAliases = new HashMap<>();
		tata2TableClone.forEach((columnName, column) -> actualTata2JoinAliases.put(columnName, entityTreeQuery.getColumnAliases().get(column)));
		
		Map<String, String> expectedAliases2 = Maps.forHashMap(String.class, String.class)
				.add("id", "y_id")
				.add("name", "y_name");
		
		org.assertj.core.api.Assertions.assertThat(actualTata2JoinAliases).isEqualTo(expectedAliases2);
		
	}
	
	
	static ClassMapping buildMappingStrategyMock(String tableName) {
		return buildMappingStrategyMock(new Table(tableName));
	}
	
	static ClassMapping buildMappingStrategyMock(Table table) {
		ClassMapping mappingStrategyMock = mock(ClassMapping.class);
		when(mappingStrategyMock.getTargetTable()).thenReturn(table);
		// the selected columns are plugged on the table ones
		when(mappingStrategyMock.getSelectableColumns()).thenAnswer(invocation -> table.getColumns());
		return mappingStrategyMock;
	}
	
	static Object[][] toSQL_singleStrategyData() {
		Table table1 = new Table("Toto");
		Column id1 = table1.addColumn("id", long.class);
		Table table2 = new Table("Toto");
		Column id2 = table2.addColumn("id", long.class);
		Column name2 = table2.addColumn("name", String.class);
		
		return new Object[][] {
				{ table1, "select Toto.id as Toto_id from Toto", Maps.forHashMap(Column.class, String.class)
						.add(id1, "Toto_id") },
				{ table2, "select Toto.id as Toto_id, Toto.name as Toto_name from Toto", Maps.forHashMap(Column.class, String.class)
						.add(id2, "Toto_id")
						.add(name2, "Toto_name") }
		};
	}
	
	@ParameterizedTest
	@MethodSource("toSQL_singleStrategyData")
	void toSQL_singleStrategy(Table table, String expected, Map<Column, String> expectedAliases) {
		ClassMapping mappingStrategyMock = buildMappingStrategyMock(table);
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(mappingStrategyMock), table);
		
		IdentityMap<Table, Table> tableCloneMap = new IdentityMap<>();
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree, c -> mock(ParameterBinder.class)) {
			@Override
			Table cloneTable(JoinNode joinNode) {
				Table tableClone = super.cloneTable(joinNode);
				tableCloneMap.put(joinNode.getTable(), tableClone);
				return tableClone;
			}
		};
		
		EntityTreeQuery treeQuery = testInstance.buildSelectQuery();
		QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(treeQuery.getQuery());
		assertThat(sqlQueryBuilder.toSQL()).isEqualTo(expected);
		
		IdentityMap<Column, String> expectedColumnClones = new IdentityMap<>();
		expectedAliases.forEach((column, value) -> expectedColumnClones.put(tableCloneMap.get(column.getTable()).getColumn(column.getName()), value));
		
		// because IdentityMap does not implement equals() / hashCode() (not need in production code) we compare them through their footprint
		assertThat(treeQuery.getColumnAliases().getDelegate())
				.isEqualTo(expectedColumnClones.getDelegate());
	}
	
	private static Object[] inheritance_tablePerClass_2Classes_testData() {
		ClassMapping totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		totoTable.addColumn("name", String.class);
		
		ClassMapping tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		
		return new Object[] { // kind of inheritance "table per class" mapping
				totoMappingMock, tataMappingMock, totoPrimaryKey, tataPrimaryKey,
				"select Toto.id as Toto_id, Toto.name as Toto_name, Tata.id as Tata_id from Toto inner join Tata as Tata on Toto.id = Tata.id"
		};
	}
	
	static Object[][] dataToSQL_multipleStrategy() {
		ClassMapping totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		totoTable.addColumn("id", long.class);
		// column for "noise" in select
		totoTable.addColumn("name", String.class);
		
		ClassMapping tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		Column totoOtherTableId = totoTable.addColumn("otherTable_id", long.class);
		
		Table tata2Table = new Table("Tata");
		tata2Table.addColumn("id", long.class);
		buildMappingStrategyMock(tata2Table);
		
		return new Object[][] {
				inheritance_tablePerClass_2Classes_testData(),
				new Object[] {
						totoMappingMock, tataMappingMock, totoOtherTableId, tataPrimaryKey,
						"select Toto.id as Toto_id, Toto.name as Toto_name, Toto.otherTable_id as Toto_otherTable_id,"
								+ " Tata.id as Tata_id from Toto inner join Tata as Tata on Toto.otherTable_id = Tata.id" }
		};
	}
	
	@ParameterizedTest
	@MethodSource("dataToSQL_multipleStrategy")
	void toSQL_multipleStrategy(ClassMapping rootMappingStrategy, ClassMapping classMappingStrategy,
								Column leftJoinColumn, Column rightJoinColumn,
								String expected) {
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(rootMappingStrategy), rootMappingStrategy.getTargetTable());
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree, c -> mock(ParameterBinder.class));
		entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(classMappingStrategy), leftJoinColumn, rightJoinColumn, null, INNER, null, Collections.emptySet());
		QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(testInstance.buildSelectQuery().getQuery());
		assertThat(sqlQueryBuilder.toSQL()).isEqualTo(expected);
	}
	
	@Test
	void testInheritance_tablePerClass_3Classes() {
		ClassMapping totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		ClassMapping tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMapping tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		
		IdentityMap<Table, Table> tableCloneMap = new IdentityMap<>();
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree, c -> mock(ParameterBinder.class)) {
			@Override
			Table cloneTable(JoinNode joinNode) {
				Table tableClone = super.cloneTable(joinNode);
				tableCloneMap.put(joinNode.getTable(), tableClone);
				return tableClone;
			}
		};
		
		EntityTreeQuery entityTreeQuery = testInstance.buildSelectQuery();
		QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(entityTreeQuery.getQuery());
		assertThat(sqlQueryBuilder.toSQL()).isEqualTo("select"
				+ " Toto.id as Toto_id, Toto.name as Toto_name"
				+ ", Tata.id as Tata_id, Tata.name as Tata_name"
				+ ", Tata_Tutu.id as Tata_Tutu_id, Tata_Tutu.name as Tata_Tutu_name"
				+ " from Toto"
				+ " inner join Tata as Tata on Toto.id = Tata.id"
				+ " inner join Tutu as Tata_Tutu on Tata.id = Tata_Tutu.id");
		
		Map<Column, String> expectedAliases = Maps.forHashMap(Column.class, String.class)
				.add(totoPrimaryKey, "Toto_id")
				.add(totoNameColumn, "Toto_name")
				.add(tataPrimaryKey, "Tata_id")
				.add(tataNameColumn, "Tata_name")
				.add(tutuPrimaryKey, "Tata_Tutu_id")
				.add(tutuNameColumn, "Tata_Tutu_name");
		
		IdentityMap<Column, String> expectedColumnClones = new IdentityMap<>();
		expectedAliases.entrySet().forEach(entry -> {
			Column column = entry.getKey();
			expectedColumnClones.put(tableCloneMap.get(column.getTable()).getColumn(column.getName()), entry.getValue());
		});
		
		// because IdentityMap does not implement equals() / hashCode() (not need in production code) we compare them through their footprint
		assertThat(entityTreeQuery.getColumnAliases().getDelegate())
				.isEqualTo(expectedColumnClones.getDelegate());
	}
	
	@Test
	void testJoin_2Relations() {
		ClassMapping totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		Column tataId = totoTable.addColumn("tataId", String.class);
		Column tutuId = totoTable.addColumn("tutuId", String.class);
		
		ClassMapping tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMapping tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tataMappingMock), tataId, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tutuMappingMock), tutuId, tutuPrimaryKey, null, OUTER, null, Collections.emptySet());
		
		IdentityMap<Table, Table> tableCloneMap = new IdentityMap<>();
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree, c -> mock(ParameterBinder.class)) {
			@Override
			Table cloneTable(JoinNode joinNode) {
				Table tableClone = super.cloneTable(joinNode);
				tableCloneMap.put(joinNode.getTable(), tableClone);
				return tableClone;
			}
		};
		
		EntityTreeQuery entityTreeQuery = testInstance.buildSelectQuery();
		QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(entityTreeQuery.getQuery());
		assertThat(sqlQueryBuilder.toSQL()).isEqualTo("select"
				+ " Toto.id as Toto_id, Toto.name as Toto_name, Toto.tataId as Toto_tataId, Toto.tutuId as Toto_tutuId"
				+ ", Tata.id as Tata_id, Tata.name as Tata_name"
				+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
				+ " from Toto"
				+ " inner join Tata as Tata on Toto.tataId = Tata.id"
				+ " left outer join Tutu as Tutu on Toto.tutuId = Tutu.id");
		
		Map<Column, String> expectedAliases = Maps.forHashMap(Column.class, String.class)
				.add(totoPrimaryKey, "Toto_id")
				.add(totoNameColumn, "Toto_name")
				.add(tataId, "Toto_tataId")
				.add(tutuId, "Toto_tutuId")
				.add(tataPrimaryKey, "Tata_id")
				.add(tataNameColumn, "Tata_name")
				.add(tutuPrimaryKey, "Tutu_id")
				.add(tutuNameColumn, "Tutu_name");
		
		IdentityMap<Column, String> expectedColumnClones = new IdentityMap<>();
		expectedAliases.entrySet().forEach(entry -> {
			Column column = entry.getKey();
			expectedColumnClones.put(tableCloneMap.get(column.getTable()).getColumn(column.getName()), entry.getValue());
		});
		
		
		// because IdentityMap does not implement equals() / hashCode() (not need in production code) we compare them through their footprint
		assertThat(entityTreeQuery.getColumnAliases().getDelegate())
				.isEqualTo(expectedColumnClones.getDelegate());
	}
	
	@Test
	void testInheritance_tablePerClass_NClasses() {
		ClassMapping totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		ClassMapping tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMapping tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		ClassMapping titiMappingMock = buildMappingStrategyMock("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		Column titiPrimaryKey = titiTable.addColumn("id", long.class);
		// column for "noise" in select
		Column titiNameColumn = titiTable.addColumn("name", String.class);
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		String titiAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(titiMappingMock), totoPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());
		
		IdentityMap<Table, Table> tableCloneMap = new IdentityMap<>();
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree, c -> mock(ParameterBinder.class)) {
			@Override
			Table cloneTable(JoinNode joinNode) {
				Table tableClone = super.cloneTable(joinNode);
				tableCloneMap.put(joinNode.getTable(), tableClone);
				return tableClone;
			}
		};
		
		EntityTreeQuery entityTreeQuery = testInstance.buildSelectQuery();
		QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(entityTreeQuery.getQuery());
		assertThat(sqlQueryBuilder.toSQL()).isEqualTo("select"
				+ " Toto.id as Toto_id, Toto.name as Toto_name"
				+ ", Tata.id as Tata_id, Tata.name as Tata_name"
				+ ", Titi.id as Titi_id, Titi.name as Titi_name"
				+ ", Tata_Tutu.id as Tata_Tutu_id, Tata_Tutu.name as Tata_Tutu_name"
				+ " from Toto"
				+ " inner join Tata as Tata on Toto.id = Tata.id"
				+ " inner join Titi as Titi on Toto.id = Titi.id"
				+ " inner join Tutu as Tata_Tutu on Tata.id = Tata_Tutu.id");
		
		Map<Column, String> expectedAliases = Maps.forHashMap(Column.class, String.class)
				.add(totoPrimaryKey, "Toto_id")
				.add(totoNameColumn, "Toto_name")
				.add(tataPrimaryKey, "Tata_id")
				.add(tataNameColumn, "Tata_name")
				.add(tutuPrimaryKey, "Tata_Tutu_id")
				.add(tutuNameColumn, "Tata_Tutu_name")
				.add(titiPrimaryKey, "Titi_id")
				.add(titiNameColumn, "Titi_name");
		
		IdentityMap<Column, String> expectedColumnClones = new IdentityMap<>();
		expectedAliases.entrySet().forEach(entry -> {
			Column column = entry.getKey();
			expectedColumnClones.put(tableCloneMap.get(column.getTable()).getColumn(column.getName()), entry.getValue());
		});
		
		// because IdentityMap does not implement equals() / hashCode() (not need in production code) we compare them through their footprint
		assertThat(entityTreeQuery.getColumnAliases().getDelegate())
				.isEqualTo(expectedColumnClones.getDelegate());
	}
	
	/**
	 * A simple factory method to easily start building of  a {@link IdentityMap}
	 *
	 * @param keyType key type
	 * @param valueType value type
	 * @param <K> key type
	 * @param <V> value type
	 * @return a new {@link ChainingIdentityMap} that allows chaining of additional key and value
	 */
	public static <K, V> ChainingIdentityMap<K, V> forIdentityMap(Class<K> keyType, Class<V> valueType) {
		return new ChainingIdentityMap<>();
	}
	
	
	/**
	 * Simple {@link IdentityHashMap} that allows to chain calls to {@link #add(Object, Object)} (same as put) and so quickly create a Map.
	 *
	 * @param <K>
	 * @param <V>
	 */
	public static class ChainingIdentityMap<K, V> extends IdentityMap<K, V> {
		
		public ChainingIdentityMap() {
		}
		
		public ChainingIdentityMap<K, V> add(K key, V value) {
			put(key, value);
			return this;
		}
	}
	
}