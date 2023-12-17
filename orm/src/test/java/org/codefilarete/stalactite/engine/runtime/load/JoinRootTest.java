package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Collections;

import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.INNER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class JoinRootTest {
	
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
	
	@Test
	public void addRelationJoin_targetNodeDoesntExist_throwsException() {
		Table table = new Table("toto");
		ClassMapping mappingStrategyMock = buildMappingStrategyMock(table);
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(mappingStrategyMock), table);
		assertThatThrownBy(() -> {
					// we don't care about other arguments (null passed) because existing strategy name is checked first
					entityJoinTree.addRelationJoin("XX", null, null, null, null, OUTER, null, Collections.emptySet());
				})
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("No join named XX exists to add a new join on");
	}
	
	@Test
	public void giveTables() {
		ClassMapping totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		totoTable.addColumn("id", long.class).primaryKey();
		PrimaryKey totoPrimaryKey = totoTable.getPrimaryKey();
		
		ClassMapping tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		tataTable.addColumn("id", long.class).primaryKey();
		PrimaryKey tataPrimaryKey = tataTable.getPrimaryKey();
		
		ClassMapping tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		tutuTable.addColumn("id", long.class).primaryKey();
		PrimaryKey tutuPrimaryKey = tutuTable.getPrimaryKey();
		
		ClassMapping titiMappingMock = buildMappingStrategyMock("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		titiTable.addColumn("id", long.class).primaryKey();
		PrimaryKey titiPrimaryKey = titiTable.getPrimaryKey();
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		String titiAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(titiMappingMock), totoPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());
		
		assertThat(entityJoinTree.giveTables()).isEqualTo(Arrays.asHashSet(totoTable, tataTable, tutuTable, titiTable));
	}
}