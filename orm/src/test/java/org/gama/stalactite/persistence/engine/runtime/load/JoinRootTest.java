package org.gama.stalactite.persistence.engine.runtime.load;

import java.util.Collections;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater.EntityMappingStrategyAdapter;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType.INNER;
import static org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class JoinRootTest {
	
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
	public void addRelationJoin_targetNodeDoesntExist_throwsException() {
		Table table = new Table("toto");
		ClassMappingStrategy mappingStrategyMock = buildMappingStrategyMock(table);
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingStrategyAdapter(mappingStrategyMock), table);
		assertThatThrownBy(() -> {
					// we don't care about other arguments (null passed) because existing strategy name is checked first
					entityJoinTree.addRelationJoin("XX", null, null, null, null, OUTER, null, Collections.emptySet());
				})
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("No strategy with name XX exists to add a new strategy on");
	}
	
	@Test
	public void giveTables() {
		ClassMappingStrategy totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column totoPrimaryKey = totoTable.addColumn("id", long.class);
		
		ClassMappingStrategy tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		Column tataPrimaryKey = tataTable.addColumn("id", long.class);
		
		ClassMappingStrategy tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		Column tutuPrimaryKey = tutuTable.addColumn("id", long.class);
		
		ClassMappingStrategy titiMappingMock = buildMappingStrategyMock("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		Column titiPrimaryKey = titiTable.addColumn("id", long.class);
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingStrategyAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		String titiAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(titiMappingMock), totoPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());
		
		assertThat(entityJoinTree.giveTables()).isEqualTo(Arrays.asHashSet(totoTable, tataTable, tutuTable, titiTable));
	}
}