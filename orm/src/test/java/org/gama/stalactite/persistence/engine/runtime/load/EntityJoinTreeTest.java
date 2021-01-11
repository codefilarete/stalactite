package org.gama.stalactite.persistence.engine.runtime.load;

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

import static org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType.INNER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntityJoinTreeTest {
	
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
	
	@Test
	void projectTo() {
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
		
		
		EntityJoinTree entityJoinTree1 = new EntityJoinTree(new JoinRoot(new EntityMappingStrategyAdapter(totoMappingMock), totoMappingMock.getTargetTable()));
		String tataAddKey = entityJoinTree1.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, INNER, null);
		String tutuAddKey = entityJoinTree1.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, INNER, null);
		
		EntityJoinTree entityJoinTree2 = new EntityJoinTree(new JoinRoot(new EntityMappingStrategyAdapter(tataMappingMock), tataMappingMock.getTargetTable()));
		String titiAddKey = entityJoinTree2.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, INNER, null);
		
		entityJoinTree2.projectTo(entityJoinTree1, tataAddKey);
		
		EntityTreeQueryBuilder testInstance = new EntityTreeQueryBuilder(entityJoinTree1);
		
		EntityTreeQuery entityTreeQuery = testInstance.buildSelectQuery(c -> mock(ParameterBinder.class));
		
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(entityTreeQuery.getQuery());
		assertEquals("select"
						+ " Toto.id as Toto_id, Toto.name as Toto_name"
						+ ", Tata.id as Tata_id, Tata.name as Tata_name"
						+ ", Tutu.id as Tutu_id, Tutu.name as Tutu_name"
						+ ", Titi.id as Titi_id, Titi.name as Titi_name"
						+ " from Toto"
						+ " inner join Tata on Toto.id = Tata.id"
						+ " inner join Tutu on Tata.id = Tutu.id"
						+ " inner join Titi on Tata.id = Titi.id"
				, sqlQueryBuilder.toSQL());
		Assertions.assertEquals(EntityTreeQueryBuilderTest.forIdentityMap(Column.class, String.class)
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
	
}