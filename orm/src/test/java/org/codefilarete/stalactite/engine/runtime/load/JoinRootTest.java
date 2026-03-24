package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Collections;

import org.codefilarete.reflection.PropertyAccessPoint;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
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
	
	static DefaultEntityMapping buildMappingStrategyMock(String tableName) {
		return buildMappingStrategyMock(new Table(tableName));
	}
	
	static DefaultEntityMapping buildMappingStrategyMock(Table table) {
		DefaultEntityMapping mappingStrategyMock = mock(DefaultEntityMapping.class);
		when(mappingStrategyMock.getTargetTable()).thenReturn(table);
		// the selected columns are plugged on the table ones
		when(mappingStrategyMock.getSelectableColumns()).thenAnswer(invocation -> table.getColumns());
		return mappingStrategyMock;
	}
	
	@Test
	public void addRelationJoin_targetNodeDoesntExist_throwsException() {
		Table table = new Table("toto");
		DefaultEntityMapping mappingStrategyMock = buildMappingStrategyMock(table);
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(mappingStrategyMock), table);
		assertThatThrownBy(() -> {
					// we don't care about other arguments (null passed) because existing strategy name is checked first
					entityJoinTree.addRelationJoin("XX", null, mock(PropertyAccessPoint.class), null, null, null, OUTER, null, Collections.emptySet());
				})
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("No join named XX exists to add a new join on");
	}
	
	@Test
	public <T1 extends Table<T1>, T2 extends Table<T2>, T3 extends Table<T3>, T4 extends Table<T4>>
	void giveTables() {
		DefaultEntityMapping totoMappingMock = buildMappingStrategyMock("Toto");
		T1 totoTable = (T1) totoMappingMock.getTargetTable();
		totoTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<T1, Long> totoPrimaryKey = totoTable.getPrimaryKey();
		
		DefaultEntityMapping tataMappingMock = buildMappingStrategyMock("Tata");
		T2 tataTable = (T2) tataMappingMock.getTargetTable();
		tataTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<T2, Long> tataPrimaryKey = tataTable.getPrimaryKey();
		
		DefaultEntityMapping tutuMappingMock = buildMappingStrategyMock("Tutu");
		T3 tutuTable = (T3) tutuMappingMock.getTargetTable();
		tutuTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<T3, Long> tutuPrimaryKey = tutuTable.getPrimaryKey();
		
		DefaultEntityMapping titiMappingMock = buildMappingStrategyMock("Titi");
		T4 titiTable = (T4) titiMappingMock.getTargetTable();
		titiTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<T4, Long> titiPrimaryKey = titiTable.getPrimaryKey();
		
		EntityJoinTree<?, ?> entityJoinTree = new EntityJoinTree<>(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMappingAdapter(tataMappingMock), mock(PropertyAccessPoint.class), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), mock(PropertyAccessPoint.class), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		String titiAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMappingAdapter(titiMappingMock), mock(PropertyAccessPoint.class), totoPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());

		assertThat(entityJoinTree.giveTables())
				.usingElementComparator(Table.COMPARATOR_ON_SCHEMA_AND_NAME)
				.containsExactlyInAnyOrder(totoTable, tataTable, tutuTable, titiTable);
	}
}
