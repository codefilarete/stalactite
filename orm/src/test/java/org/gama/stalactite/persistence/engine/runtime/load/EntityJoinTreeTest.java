package org.gama.stalactite.persistence.engine.runtime.load;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.assertj.core.presentation.StandardRepresentation;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Predicates;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater.EntityMappingStrategyAdapter;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType.INNER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		EntityJoinTree entityJoinTree1 = new EntityJoinTree(new EntityMappingStrategyAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree1.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null);
		String tutuAddKey = entityJoinTree1.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null);
		
		// and following second one:
		// Tata.id = Titi.id (Z)
		EntityJoinTree entityJoinTree2 = new EntityJoinTree(new EntityMappingStrategyAdapter(tataMappingMock), tataMappingMock.getTargetTable());
		String titiAddKey = entityJoinTree2.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null);
		
		// projecting second one to first one on X
		entityJoinTree2.projectTo(entityJoinTree1, tataAddKey);
		
		// we expect to have:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		//   Tata.id = Titi.id (Z)
		assertNotNull(entityJoinTree1.getJoin(tataAddKey));
		// by checking node count we ensure that node was added 
		assertEquals(3, Iterables.stream(entityJoinTree1.joinIterator()).count());
		// and there was no removal
		assertNotNull(entityJoinTree2.giveJoin(tataPrimaryKey, titiPrimaryKey));
		// we check that a copy was made, not a node move
		assertNotSame(entityJoinTree2.giveJoin(tataPrimaryKey, titiPrimaryKey), entityJoinTree1.giveJoin(tataPrimaryKey, titiPrimaryKey));
		assertEquals(titiMappingMock.getSelectableColumns(), entityJoinTree1.giveJoin(tataPrimaryKey, titiPrimaryKey).getColumnsToSelect());
		assertEquals(entityJoinTree1.getJoin(tataAddKey), entityJoinTree1.giveJoin(tataPrimaryKey, titiPrimaryKey).getParent());
	}
	
	@Test
	void joinIterator() {
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
		
		Table tataTableClone = new Table("tata2");
		ClassMappingStrategy tataCloneMappingMock = buildMappingStrategyMock(tataTableClone);
		Column tataClonePrimaryKey = tataTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tataCloneNameColumn = tataTableClone.addColumn("name", String.class);
		
		Table tutuTableClone = new Table("tutu2");
		ClassMappingStrategy tutuCloneMappingMock = buildMappingStrategyMock(tutuTableClone);
		Column tutuClonePrimaryKey = tutuTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuCloneNameColumn = tutuTableClone.addColumn("name", String.class);
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		//   Tata.id = Titi.id (Z)
		// Toto.id = tata2.id (X')
		//   tata2.id = tutu2.id (Y')
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingStrategyAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null);
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null);
		String titiAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null);
		String tataAddKey2 = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataCloneMappingMock), totoPrimaryKey, tataClonePrimaryKey, null, INNER, null);
		String tutuAddKey2 = entityJoinTree.addRelationJoin(tataAddKey2, new EntityMappingStrategyAdapter(tutuCloneMappingMock), tataClonePrimaryKey, tutuClonePrimaryKey, null, INNER, null);
		
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
		
		Table tataTableClone = new Table("tata2");
		ClassMappingStrategy tataCloneMappingMock = buildMappingStrategyMock(tataTableClone);
		Column tataClonePrimaryKey = tataTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tataCloneNameColumn = tataTableClone.addColumn("name", String.class);
		
		Table tutuTableClone = new Table("tutu2");
		ClassMappingStrategy tutuCloneMappingMock = buildMappingStrategyMock(tutuTableClone);
		Column tutuClonePrimaryKey = tutuTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuCloneNameColumn = tutuTableClone.addColumn("name", String.class);
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		//   Tata.id = Titi.id (Z)
		// Toto.id = tata2.id (X')
		//   tata2.id = tutu2.id (Y')
		
		EntityJoinTree<?, ?> entityJoinTree = new EntityJoinTree(new EntityMappingStrategyAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null);
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null);
		String titiAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null);
		String tataAddKey2 = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataCloneMappingMock), totoPrimaryKey, tataClonePrimaryKey, null, INNER, null);
		String tutuAddKey2 = entityJoinTree.addRelationJoin(tataAddKey2, new EntityMappingStrategyAdapter(tutuCloneMappingMock), tataClonePrimaryKey, tutuClonePrimaryKey, null, INNER, null);
		
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
		
		Table tataTableClone = new Table("tata2");
		ClassMappingStrategy tataCloneMappingMock = buildMappingStrategyMock(tataTableClone);
		Column tataClonePrimaryKey = tataTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tataCloneNameColumn = tataTableClone.addColumn("name", String.class);
		
		Table tutuTableClone = new Table("tutu2");
		ClassMappingStrategy tutuCloneMappingMock = buildMappingStrategyMock(tutuTableClone);
		Column tutuClonePrimaryKey = tutuTableClone.addColumn("id", long.class);
		// column for "noise" in select
		Column tutuCloneNameColumn = tutuTableClone.addColumn("name", String.class);
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		//   Tata.id = Titi.id (Z)
		// Toto.id = tata2.id (X')
		//   tata2.id = tutu2.id (Y')
		
		EntityJoinTree<?, ?> entityJoinTree = new EntityJoinTree(new EntityMappingStrategyAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null);
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null);
		String titiAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingStrategyAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null);
		String tataAddKey2 = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME, new EntityMappingStrategyAdapter(tataCloneMappingMock), totoPrimaryKey, tataClonePrimaryKey, null, INNER, null);
		String tutuAddKey2 = entityJoinTree.addRelationJoin(tataAddKey2, new EntityMappingStrategyAdapter(tutuCloneMappingMock), tataClonePrimaryKey, tutuClonePrimaryKey, null, INNER, null);
		
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