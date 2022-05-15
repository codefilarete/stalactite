package org.codefilarete.stalactite.engine.runtime.load;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.assertj.core.presentation.StandardRepresentation;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Predicates;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.INNER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntityJoinTreeTest {
	
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
	void projectTo() {
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
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		EntityJoinTree entityJoinTree1 = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree1.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree1.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		
		// and following second one:
		// Tata.id = Titi.id (Z)
		EntityJoinTree entityJoinTree2 = new EntityJoinTree(new EntityMappingAdapter(tataMappingMock), tataMappingMock.getTargetTable());
		String titiAddKey = entityJoinTree2.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());
		
		// projecting second one to first one on X
		entityJoinTree2.projectTo(entityJoinTree1, tataAddKey);
		
		// we expect to have:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		//   Tata.id = Titi.id (Z)
		JoinNode tataJoinClone = entityJoinTree1.getJoin(tataAddKey);
		assertThat(tataJoinClone).isNotNull();
		// by checking node count we ensure that node was added 
		assertThat(Iterables.stream(entityJoinTree1.joinIterator()).count()).isEqualTo(3);
		// and there was no removal
		assertThat(entityJoinTree2.giveJoin(tataPrimaryKey, titiPrimaryKey)).isNotNull();
		// we check that a copy was made, not a node move
		AbstractJoinNode abstractJoinNode = entityJoinTree1.giveJoin(tataPrimaryKey, titiPrimaryKey);
		assertThat(abstractJoinNode).isNotSameAs(entityJoinTree2.giveJoin(tataPrimaryKey, titiPrimaryKey));
		assertThat(abstractJoinNode.getColumnsToSelect()).isEqualTo(titiMappingMock.getSelectableColumns());
		// copy must be put at the right place 
		assertThat(abstractJoinNode.getParent()).isEqualTo(tataJoinClone);
		// we check that join indexes were updated: since it's difficult to check detailed content because of index naming strategy, we fallback to count them (far from perfect) 
		assertThat(entityJoinTree1.getJoinIndex().size()).isEqualTo(4);
		assertThat(entityJoinTree2.getJoinIndex().size()).isEqualTo(2);
	}
	
	@Test
	void joinIterator() {
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
		
		Table tataTableClone = new Table("tata2");
		ClassMapping tataCloneMappingMock = buildMappingStrategyMock(tataTableClone);
		Column tataClonePrimaryKey = tataTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tataCloneNameColumn = tataTableClone.addColumn("name", String.class);
		
		Table tutuTableClone = new Table("tutu2");
		ClassMapping tutuCloneMappingMock = buildMappingStrategyMock(tutuTableClone);
		Column tutuClonePrimaryKey = tutuTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuCloneNameColumn = tutuTableClone.addColumn("name", String.class);
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		//   Tata.id = Titi.id (Z)
		// Toto.id = tata2.id (X')
		//   tata2.id = tutu2.id (Y')
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		String titiAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());
		String tataAddKey2 = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tataCloneMappingMock), totoPrimaryKey, tataClonePrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey2 = entityJoinTree.addRelationJoin(tataAddKey2, new EntityMappingAdapter(tutuCloneMappingMock), tataClonePrimaryKey, tutuClonePrimaryKey, null, INNER, null, Collections.emptySet());
		
		Iterator<AbstractJoinNode<?, ?, ?, ?>> actual = entityJoinTree.joinIterator();
		org.assertj.core.api.Assertions.assertThat(Iterables.copy(actual))
				.usingElementComparator(Predicates.toComparator(Predicates.and(AbstractJoinNode::getLeftJoinColumn, AbstractJoinNode::getRightJoinColumn)))
				.withRepresentation(new Printer<>(AbstractJoinNode.class, joinNode -> joinNode.getLeftJoinColumn() + " = " + joinNode.getRightJoinColumn()))
				.isEqualTo(Arrays.asList(
						entityJoinTree.giveJoin(totoPrimaryKey, tataPrimaryKey),	// X
						entityJoinTree.giveJoin(totoPrimaryKey, tataClonePrimaryKey),	// X'
						entityJoinTree.giveJoin(tataPrimaryKey, tutuPrimaryKey),	// Y
						entityJoinTree.giveJoin(tataPrimaryKey, titiPrimaryKey),	// Z
						entityJoinTree.giveJoin(tataClonePrimaryKey, tutuClonePrimaryKey)	// Y'
				));
	}
	
	@Test
	void foreachJoinWithDepth() {
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
		
		Table tataTableClone = new Table("tata2");
		ClassMapping tataCloneMappingMock = buildMappingStrategyMock(tataTableClone);
		Column tataClonePrimaryKey = tataTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tataCloneNameColumn = tataTableClone.addColumn("name", String.class);
		
		Table tutuTableClone = new Table("tutu2");
		ClassMapping tutuCloneMappingMock = buildMappingStrategyMock(tutuTableClone);
		Column tutuClonePrimaryKey = tutuTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuCloneNameColumn = tutuTableClone.addColumn("name", String.class);
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		//   Tata.id = Titi.id (Z)
		// Toto.id = tata2.id (X')
		//   tata2.id = tutu2.id (Y')
		
		EntityJoinTree<?, ?> entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		String titiAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());
		String tataAddKey2 = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tataCloneMappingMock), totoPrimaryKey, tataClonePrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey2 = entityJoinTree.addRelationJoin(tataAddKey2, new EntityMappingAdapter(tutuCloneMappingMock), tataClonePrimaryKey, tutuClonePrimaryKey, null, INNER, null, Collections.emptySet());
		
		List<Integer> depth = new ArrayList<>();
		List<AbstractJoinNode> collectedNodes = new ArrayList<>();
		entityJoinTree.foreachJoinWithDepth(1, (o, abstractJoinNode) -> {
			depth.add(o);
			collectedNodes.add(abstractJoinNode);
			return ++o;
		});
		
		org.assertj.core.api.Assertions.assertThat(depth).isEqualTo(Arrays.asList(1, 1, 2, 2, 2));
		
		org.assertj.core.api.Assertions.assertThat(collectedNodes)
				.withRepresentation(new Printer<>(AbstractJoinNode.class, joinNode -> joinNode.getLeftJoinColumn() + " = " + joinNode.getRightJoinColumn()))
				.isEqualTo(Arrays.asList(
						entityJoinTree.giveJoin(totoPrimaryKey, tataPrimaryKey),    // X
						entityJoinTree.giveJoin(totoPrimaryKey, tataClonePrimaryKey),    // X'
						entityJoinTree.giveJoin(tataPrimaryKey, tutuPrimaryKey),    // Y
						entityJoinTree.giveJoin(tataPrimaryKey, titiPrimaryKey),    // Z
						entityJoinTree.giveJoin(tataClonePrimaryKey, tutuClonePrimaryKey)    // Y'
				));
		
	}
	
	@Test
	void giveTables() {
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
		
		Table tataTableClone = new Table("tata2");
		ClassMapping tataCloneMappingMock = buildMappingStrategyMock(tataTableClone);
		Column tataClonePrimaryKey = tataTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tataCloneNameColumn = tataTableClone.addColumn("name", String.class);
		
		Table tutuTableClone = new Table("tutu2");
		ClassMapping tutuCloneMappingMock = buildMappingStrategyMock(tutuTableClone);
		Column tutuClonePrimaryKey = tutuTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuCloneNameColumn = tutuTableClone.addColumn("name", String.class);
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		//   Tata.id = Titi.id (Z)
		// Toto.id = tata2.id (X')
		//   tata2.id = tutu2.id (Y')
		
		EntityJoinTree<?, ?> entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		String titiAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());
		String tataAddKey2 = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingAdapter(tataCloneMappingMock), totoPrimaryKey, tataClonePrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey2 = entityJoinTree.addRelationJoin(tataAddKey2, new EntityMappingAdapter(tutuCloneMappingMock), tataClonePrimaryKey, tutuClonePrimaryKey, null, INNER, null, Collections.emptySet());
		
		Set<Table> actualTables = entityJoinTree.giveTables();
		
		org.assertj.core.api.Assertions.assertThat(actualTables).isEqualTo(
				Arrays.asHashSet(totoTable, tataTable, tataTableClone, titiTable, tutuTable, tutuTableClone));
		
	}
	
	private static class Printer<E> extends StandardRepresentation {
		
		private final Class<E> elementType;
		private final Function<E, String> printingFunction;
		
		private Printer(Class<E> elementType, Function<E, String> printingFunction) {
			this.elementType = elementType;
			this.printingFunction = printingFunction;
		}
		
		@Override
		public String toStringOf(Object o) {
			if (elementType.isInstance(o)) {
				return printingFunction.apply((E) o);
			} else {
				return super.toStringOf(o);
			}
		}
	}
	
}