package org.gama.stalactite.persistence.engine.runtime.load;

import java.util.IdentityHashMap;

import org.gama.lang.test.Assertions;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater.EntityMappingStrategyAdapter;
import org.gama.stalactite.persistence.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.IdentityMap;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType.INNER;
import static org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntityTreeQueryBuilderTest {
	
	static ClassMappingStrategy buildMappingStrategyMock(String tableName) {
		return buildMappingStrategyMock(new Table(tableName));
	}
	
	static ClassMappingStrategy buildMappingStrategyMock(Table table) {
		ClassMappingStrategy mappingStrategyMock = mock(ClassMappingStrategy.class);
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
				{ table1, "select Toto.id as Toto_id from Toto", forIdentityMap(Column.class, String.class)
						.add(id1, "Toto_id") },
				{ table2, "select Toto.id as Toto_id, Toto.name as Toto_name from Toto", forIdentityMap(Column.class, String.class)
						.add(id2, "Toto_id")
						.add(name2, "Toto_name") }
		};
	}
	
	@ParameterizedTest
	@MethodSource("toSQL_singleStrategyData")
	void toSQL_singleStrategy(Table table, String expected, IdentityMap<Column, String> expectedAliases) {
		ClassMappingStrategy mappingStrategyMock = buildMappingStrategyMock(table);
		EntityJoinTree entityJoinTree = new EntityJoinTree(new JoinRoot(new EntityMappingStrategyAdapter(mappingStrategyMock), table));
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree);
		EntityTreeQuery treeQuery = testInstance.buildSelectQuery(c -> mock(ParameterBinder.class));
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(treeQuery.getQuery());
		assertEquals(expected, sqlQueryBuilder.toSQL());
		Assertions.assertEquals(expectedAliases, treeQuery.getColumnAliases(),
				// because IdentityMap does not implement equals() / hashCode() (not need in production code) we compare them through their footprint
				IdentityMap::getDelegate);
	}
	
	private static Object[] inheritance_tablePerClass_2Classes_testData() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		
		return new Object[] { // kind of inheritance "table per class" mapping
				totoMappingMock, tataMappingMock, totoPrimaryKey, tataPrimaryKey,
				"select Toto.id as Toto_id, Toto.name as Toto_name, Tata.id as Tata_id from Toto inner join Tata on Toto.id = Tata.id"
		};
	}
	
	static Object[][] dataToSQL_multipleStrategy() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		totoTable.addColumn("id", long.class);
		// column for "noise" in select
		totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
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
								+ " Tata.id as Tata_id from Toto inner join Tata on Toto.otherTable_id = Tata.id" }
		};
	}
	
	@ParameterizedTest
	@MethodSource("dataToSQL_multipleStrategy")
	void toSQL_multipleStrategy(ClassMappingStrategy rootMappingStrategy, ClassMappingStrategy classMappingStrategy,
										   Column leftJoinColumn, Column rightJoinColumn,
										   String expected) {
		EntityJoinTree entityJoinTree = new EntityJoinTree(new JoinRoot(new EntityMappingStrategyAdapter(rootMappingStrategy), rootMappingStrategy.getTargetTable()));
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree);
		entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(classMappingStrategy), leftJoinColumn, rightJoinColumn, INNER, null);
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(testInstance.buildSelectQuery(c -> mock(ParameterBinder.class)).getQuery());
		assertEquals(expected, sqlQueryBuilder.toSQL());
	}
	
	@Test
	void testInheritance_tablePerClass_3Classes() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new JoinRoot(new EntityMappingStrategyAdapter(totoMappingMock), totoMappingMock.getTargetTable()));
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, INNER, null);
		entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, INNER, null);
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree);
		EntityTreeQuery entityTreeQuery = testInstance.buildSelectQuery(c -> mock(ParameterBinder.class));
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(entityTreeQuery.getQuery());
		assertEquals("select"
						+ " Toto.id as Toto_id, Toto.name as Toto_name"
						+ ", Tata.id as Tata_id, Tata.name as Tata_name"
						+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
						+ " from Toto"
						+ " inner join Tata on Toto.id = Tata.id"
						+ " inner join Tutu on Tata.id = Tutu.id"
				, sqlQueryBuilder.toSQL());
		Assertions.assertEquals(forIdentityMap(Column.class, String.class)
						.add(totoPrimaryKey, "Toto_id")
						.add(totoNameColumn, "Toto_name")
						.add(tataPrimaryKey, "Tata_id")
						.add(tataNameColumn, "Tata_name")
						.add(tutuPrimaryKey, "Tutu_id")
						.add(tutuNameColumn, "Tutu_name"),
				entityTreeQuery.getColumnAliases(),
				// because IdentityMap does not implement equals() / hashCode() (not need in production code) we compare them through their footprint
				IdentityMap::getDelegate);
	}
	
	@Test
	void testJoin_2Relations() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		Column tataId = totoTable.addColumn("tataId", String.class);
		Column tutuId = totoTable.addColumn("tutuId", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new JoinRoot(new EntityMappingStrategyAdapter(totoMappingMock), totoMappingMock.getTargetTable()));
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataMappingMock), tataId, tataPrimaryKey, INNER, null);
		entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(tutuMappingMock), tutuId, tutuPrimaryKey, OUTER, null);
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree);
		
		EntityTreeQuery entityTreeQuery = testInstance.buildSelectQuery(c -> mock(ParameterBinder.class));
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(entityTreeQuery.getQuery());
		assertEquals("select"
						+ " Toto.id as Toto_id, Toto.name as Toto_name, Toto.tataId as Toto_tataId, Toto.tutuId as Toto_tutuId"
						+ ", Tata.id as Tata_id, Tata.name as Tata_name"
						+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
						+ " from Toto"
						+ " inner join Tata on Toto.tataId = Tata.id"
						+ " left outer join Tutu on Toto.tutuId = Tutu.id"
				, sqlQueryBuilder.toSQL());
		Assertions.assertEquals(forIdentityMap(Column.class, String.class)
						.add(totoPrimaryKey, "Toto_id")
						.add(totoNameColumn, "Toto_name")
						.add(tataId, "Toto_tataId")
						.add(tutuId, "Toto_tutuId")
						.add(tataPrimaryKey, "Tata_id")
						.add(tataNameColumn, "Tata_name")
						.add(tutuPrimaryKey, "Tutu_id")
						.add(tutuNameColumn, "Tutu_name"),
				entityTreeQuery.getColumnAliases(),
				// because IdentityMap does not implement equals() / hashCode() (not need in production code) we compare them through their footprint
				IdentityMap::getDelegate);
	}
	
	@Test
	void testInheritance_tablePerClass_NClasses() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMappingStrategy tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		ClassMappingStrategy titiMappingMock = buildMappingStrategyMock("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		Column titiPrimaryKey = titiTable.addColumn("id", long.class);
		// column for "noise" in select
		Column titiNameColumn = titiTable.addColumn("name", String.class);
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new JoinRoot(new EntityMappingStrategyAdapter(totoMappingMock), totoMappingMock.getTargetTable()));
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, INNER, null);
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, INNER, null);
		String titiAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(titiMappingMock), totoPrimaryKey, titiPrimaryKey, INNER, null);
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree);
		
		EntityTreeQuery entityTreeQuery = testInstance.buildSelectQuery(c -> mock(ParameterBinder.class));
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(entityTreeQuery.getQuery());
		assertEquals("select"
						+ " Toto.id as Toto_id, Toto.name as Toto_name"
						+ ", Tata.id as Tata_id, Tata.name as Tata_name"
						+ ", Titi.id as Titi_id, Titi.name as Titi_name"
						+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
						+ " from Toto"
						+ " inner join Tata on Toto.id = Tata.id"
						+ " inner join Titi on Toto.id = Titi.id"
						+ " inner join Tutu on Tata.id = Tutu.id"
				, sqlQueryBuilder.toSQL());
		Assertions.assertEquals(forIdentityMap(Column.class, String.class)
						.add(totoPrimaryKey, "Toto_id")
						.add(totoNameColumn, "Toto_name")
						.add(tataPrimaryKey, "Tata_id")
						.add(tataNameColumn, "Tata_name")
						.add(tutuPrimaryKey, "Tutu_id")
						.add(tutuNameColumn, "Tutu_name")
						.add(titiPrimaryKey, "Titi_id")
						.add(titiNameColumn, "Titi_name"),
				entityTreeQuery.getColumnAliases(),
				// because IdentityMap does not implement equals() / hashCode() (not need in production code) we compare them through their footprint
				IdentityMap::getDelegate);
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