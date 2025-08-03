package org.codefilarete.stalactite.engine.runtime.load;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.assertj.core.presentation.StandardRepresentation;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoColumn;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
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
	void cloneNodeForParent_relationJoinNode() {
		// Given following tree:
		// Toto.id (Root)
		// Toto.tataId = Tata.id (X)
		ClassMapping totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column<Table, Long> tataIdColumn = totoTable.addColumn("tataId", long.class);
		Table tataTable = new Table("Tata");
		Column<Table, Long> dummyColumn = tataTable.addColumn("dummyColumn", long.class);
		tataTable.addColumn("id", long.class).primaryKey();
		EntityJoinTree totoEntityJoinTree = new EntityJoinTree(totoMappingMock);
		String relationJoinName = totoEntityJoinTree.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME,
				mock(EntityInflater.class),
				Key.ofSingleColumn(tataIdColumn),
				tataTable.<Long>getPrimaryKey(),
				"dummyTableAlias",
				INNER,
				null,
				Arrays.asSet(dummyColumn));
		AbstractJoinNode tataJoinInToto = (AbstractJoinNode) totoEntityJoinTree.getJoin(relationJoinName);

		// and another given tree:
		// Tutu.id (Root)
		ClassMapping tutuMappingMock = buildMappingStrategyMock("Tutu");
		EntityJoinTree tutuEntityJoinTree = new EntityJoinTree(tutuMappingMock);
		((Table) tutuEntityJoinTree.getRoot().getTable()).addColumn("id", long.class).primaryKey();

		// When we clone:
		// Toto.tataId = Tata.id (X)
		// to
		// Tutu.id (Root)
		PrimaryKey<?, Object> targetJoinLink = ((Table<?>) tutuEntityJoinTree.getRoot().getTable()).getPrimaryKey();
		AbstractJoinNode<?, ?, ?, ?> tataJoinInTutu = EntityJoinTree.cloneNodeForParent(tataJoinInToto, tutuEntityJoinTree.getRoot(), targetJoinLink);

		// Then we should get:
		// Tutu.id (Root)
		// Tutu.id = Tata.id (X clone)
		Table tutuTable = (Table) tutuEntityJoinTree.getRoot().getTable();
		assertThat(tataJoinInTutu.getParent()).isSameAs(tutuEntityJoinTree.getRoot());
		assertThat(tataJoinInTutu.getLeftJoinLink()).isSameAs(targetJoinLink);
		assertThat(tutuEntityJoinTree.getRoot().getJoins()).hasSize(1);
		assertThat(tutuEntityJoinTree.getRoot().getJoins().get(0)).isInstanceOf(RelationJoinNode.class);
		// we check that the cloned node has the same left join link as the original one
		assertThat(tataJoinInTutu.getLeftJoinLink()).isSameAs(tutuTable.getPrimaryKey());
		// right table must be cloned ...
		assertThat(tataJoinInTutu.getTable()).isNotSameAs(tataTable);
		// ... and its columns must be the same as the original one
		Function<Selectable, String> getExpression = Selectable::getExpression;
		Function<Selectable, Class> getJavaType = Selectable::getJavaType;
		assertThat(tataJoinInTutu.getRightJoinLink().getColumns())
				.usingElementComparator(Predicates.toComparator(Predicates.and(getExpression, getExpression).and(Predicates.and(getJavaType, getJavaType))))
				.isEqualTo(tataTable.getPrimaryKey().getColumns());

		// we check that the join is well-formed
		Function<AbstractJoinNode, Collection<Column>> getLeftJoinColumns = joinNode -> joinNode.getLeftJoinLink().getColumns();
		Function<AbstractJoinNode, Collection<Column>> getRightJoinColumns = joinNode -> joinNode.getRightJoinLink().getColumns();
		
		assertThat((Iterable<AbstractJoinNode<?, ?, ?, ?>>)() -> tutuEntityJoinTree.joinIterator())
				.usingElementComparator(Predicates.toComparator(Predicates.and(getLeftJoinColumns, getLeftJoinColumns).and(Predicates.and(getRightJoinColumns, getRightJoinColumns))))
				.withRepresentation(new Printer<>(AbstractJoinNode.class, joinNode -> joinNode.getLeftJoinLink().getColumns() + " = " + joinNode.getRightJoinLink().getColumns()))
				.containsExactly(tataJoinInTutu);

		// we check that the join has the right attributes
		assertThat(tataJoinInTutu.getJoinType()).isEqualTo(tataJoinInToto.getJoinType());
		assertThat(tataJoinInTutu.getTableAlias()).isEqualTo(tataJoinInToto.getTableAlias());
		// selectable columns must be cloned ...
		assertThat(tataJoinInTutu.getColumnsToSelect())
				.usingElementComparator(Predicates.toComparator(Predicates.and(getExpression, getExpression).and(Predicates.and(getJavaType, getJavaType))))
				.isEqualTo(tataJoinInToto.getColumnsToSelect());
		// ... but different from the original one
		assertThat(tataJoinInTutu.getColumnsToSelect())
				.allSatisfy(column -> assertThat(EntityJoinTreeTest.getOwner(column)).isNotSameAs(tataTable));
	}

	@Test
	void cloneNodeForParent_mergeJoinNode() {
		// Given following tree:
		// Toto.id (Root)
		// Toto.tataId = Tata.id (X)
		ClassMapping totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		Column<Table, Long> tataIdColumn = totoTable.addColumn("tataId", long.class);
		Table tataTable = new Table("Tata");
		Column<Table, Long> dummyColumn = tataTable.addColumn("dummyColumn", long.class);
		tataTable.addColumn("id", long.class).primaryKey();
		EntityJoinTree totoEntityJoinTree = new EntityJoinTree(totoMappingMock);
		
		// Creating the merge join. We must make it have some selectable columns because we expect them to be cloned and returned by the newly created join (clone) 
		EntityMerger entityMergerMock = mock(EntityMerger.class);
		when(entityMergerMock.getSelectableColumns()).thenReturn(Arrays.asSet(dummyColumn));
		
		String relationJoinName = totoEntityJoinTree.addMergeJoin(EntityJoinTree.ROOT_JOIN_NAME,
				entityMergerMock,
				Key.ofSingleColumn(tataIdColumn),
				tataTable.<Long>getPrimaryKey());
		AbstractJoinNode tataJoinInToto = (AbstractJoinNode) totoEntityJoinTree.getJoin(relationJoinName);

		// and another given tree:
		// Tutu.id (Root)
		ClassMapping tutuMappingMock = buildMappingStrategyMock("Tutu");
		EntityJoinTree tutuEntityJoinTree = new EntityJoinTree(tutuMappingMock);
		((Table) tutuEntityJoinTree.getRoot().getTable()).addColumn("id", long.class).primaryKey();

		// When we clone:
		// Toto.tataId = Tata.id (X)
		// to
		// Tutu.id (Root)
		PrimaryKey<?, Object> targetJoinLink = ((Table<?>) tutuEntityJoinTree.getRoot().getTable()).getPrimaryKey();
		AbstractJoinNode<?, ?, ?, ?> tataJoinInTutu = EntityJoinTree.cloneNodeForParent(tataJoinInToto, tutuEntityJoinTree.getRoot(), targetJoinLink);

		// Then we should get:
		// Tutu.id (Root)
		// Tutu.id = Tata.id (X clone)
		Table tutuTable = (Table) tutuEntityJoinTree.getRoot().getTable();
		assertThat(tataJoinInTutu.getParent()).isSameAs(tutuEntityJoinTree.getRoot());
		assertThat(tataJoinInTutu.getLeftJoinLink()).isSameAs(targetJoinLink);
		assertThat(tutuEntityJoinTree.getRoot().getJoins()).hasSize(1);
		assertThat(tutuEntityJoinTree.getRoot().getJoins().get(0)).isInstanceOf(MergeJoinNode.class);
		// we check that the cloned node has the same left join link as the original one
		assertThat(tataJoinInTutu.getLeftJoinLink()).isSameAs(tutuTable.getPrimaryKey());
		// right table must be cloned ...
		assertThat(tataJoinInTutu.getTable()).isNotSameAs(tataTable);
		// ... and its columns must be the same as the original one
		Function<Selectable, String> getExpression = Selectable::getExpression;
		Function<Selectable, Class> getJavaType = Selectable::getJavaType;
		assertThat(tataJoinInTutu.getRightJoinLink().getColumns())
				.usingElementComparator(Predicates.toComparator(Predicates.and(getExpression, getExpression).and(Predicates.and(getJavaType, getJavaType))))
				.isEqualTo(tataTable.getPrimaryKey().getColumns());

		// we check that the join is well-formed
		Function<AbstractJoinNode, Collection<Column>> getLeftJoinColumns = joinNode -> joinNode.getLeftJoinLink().getColumns();
		Function<AbstractJoinNode, Collection<Column>> getRightJoinColumns = joinNode -> joinNode.getRightJoinLink().getColumns();

		assertThat((Iterable<AbstractJoinNode<?, ?, ?, ?>>)() -> tutuEntityJoinTree.joinIterator())
				.usingElementComparator(Predicates.toComparator(Predicates.and(getLeftJoinColumns, getLeftJoinColumns).and(Predicates.and(getRightJoinColumns, getRightJoinColumns))))
				.withRepresentation(new Printer<>(AbstractJoinNode.class, joinNode -> joinNode.getLeftJoinLink().getColumns() + " = " + joinNode.getRightJoinLink().getColumns()))
				.containsExactly(tataJoinInTutu);

		// we check that the join has the right attributes
		assertThat(tataJoinInTutu.getJoinType()).isEqualTo(tataJoinInToto.getJoinType());
		assertThat(tataJoinInTutu.getTableAlias()).isEqualTo(tataJoinInToto.getTableAlias());
		// selectable columns must be cloned ...
		assertThat(tataJoinInTutu.getColumnsToSelect())
				.usingElementComparator(Predicates.toComparator(Predicates.and(getExpression, getExpression).and(Predicates.and(getJavaType, getJavaType))))
				.isEqualTo(tataJoinInToto.getColumnsToSelect());
		// ... but different from the original one
		assertThat(tataJoinInTutu.getColumnsToSelect())
		    .allSatisfy(column -> assertThat(EntityJoinTreeTest.getOwner(column)).isNotSameAs(tataTable));
	}
	
	static Fromable getOwner(Selectable<?> selectable) {
		if (selectable instanceof Column) {
			return ((Column<?, ?>) selectable).getOwner();
		} else if (selectable instanceof PseudoColumn) {
			return ((PseudoColumn) selectable).getOwner();
		} else {
			throw new IllegalArgumentException("Selectable " + selectable + " is not a JoinLink or a Key");
		}
	}
	
	@Test
	void projectTo() {
		ClassMapping totoMappingMock = buildMappingStrategyMock("Toto");
		Table totoTable = totoMappingMock.getTargetTable();
		totoTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> totoPrimaryKey = totoTable.getPrimaryKey();
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		ClassMapping tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		tataTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tataPrimaryKey = tataTable.getPrimaryKey();
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMapping tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		tutuTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tutuPrimaryKey = tutuTable.getPrimaryKey();
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		ClassMapping titiMappingMock = buildMappingStrategyMock("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		titiTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> titiPrimaryKey = titiTable.getPrimaryKey();
		// column for "noise" in select
		Column titiNameColumn = titiTable.addColumn("name", String.class);
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		EntityJoinTree entityJoinTree1 = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree1.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMappingAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree1.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		
		// and following second one:
		// Tata.id = Titi.id (Z)
		EntityJoinTree entityJoinTree2 = new EntityJoinTree(new EntityMappingAdapter(tataMappingMock), tataMappingMock.getTargetTable());
		String titiAddKey = entityJoinTree2.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMappingAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());
		
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
		totoTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> totoPrimaryKey = totoTable.getPrimaryKey();
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		ClassMapping tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		tataTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tataPrimaryKey = tataTable.getPrimaryKey();
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMapping tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		tutuTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tutuPrimaryKey = tutuTable.getPrimaryKey();
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		ClassMapping titiMappingMock = buildMappingStrategyMock("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		titiTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> titiPrimaryKey = titiTable.getPrimaryKey();
		// column for "noise" in select
		Column titiNameColumn = titiTable.addColumn("name", String.class);
		
		Table tataTableClone = new Table("tata2");
		ClassMapping tataCloneMappingMock = buildMappingStrategyMock(tataTableClone);
		tataTableClone.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tataClonePrimaryKey = tataTableClone.getPrimaryKey();
		// column for "noise" in select
		Column tataCloneNameColumn = tataTableClone.addColumn("name", String.class);
		
		Table tutuTableClone = new Table("tutu2");
		ClassMapping tutuCloneMappingMock = buildMappingStrategyMock(tutuTableClone);
		tutuTableClone.addColumn("id", long.class).primaryKey();
		// column for "noise" in select
		Column tutuCloneNameColumn = tutuTableClone.addColumn("name", String.class);
		PrimaryKey<?, Long> tutuClonePrimaryKey = tutuTableClone.getPrimaryKey();
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		//   Tata.id = Titi.id (Z)
		// Toto.id = tata2.id (X')
		//   tata2.id = tutu2.id (Y')
		
		EntityJoinTree entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMappingAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		String titiAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());
		String tataAddKey2 = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMappingAdapter(tataCloneMappingMock), totoPrimaryKey, tataClonePrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey2 = entityJoinTree.addRelationJoin(tataAddKey2, new EntityMappingAdapter(tutuCloneMappingMock), tataClonePrimaryKey, tutuClonePrimaryKey, null, INNER, null, Collections.emptySet());
		
		Iterator<AbstractJoinNode<?, ?, ?, ?>> actual = entityJoinTree.joinIterator();
		assertThat(Iterables.copy(actual))
				.usingElementComparator(Predicates.toComparator(Predicates.and(AbstractJoinNode::getLeftJoinLink, AbstractJoinNode::getRightJoinLink)))
				.withRepresentation(new Printer<>(AbstractJoinNode.class, joinNode -> joinNode.getLeftJoinLink() + " = " + joinNode.getRightJoinLink()))
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
		totoTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> totoPrimaryKey = totoTable.getPrimaryKey();
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		ClassMapping tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		tataTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tataPrimaryKey = tataTable.getPrimaryKey();
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMapping tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		tutuTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tutuPrimaryKey = tutuTable.getPrimaryKey();
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		ClassMapping titiMappingMock = buildMappingStrategyMock("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		titiTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> titiPrimaryKey = titiTable.getPrimaryKey();
		// column for "noise" in select
		Column titiNameColumn = titiTable.addColumn("name", String.class);
		
		Table tataTableClone = new Table("tata2");
		ClassMapping tataCloneMappingMock = buildMappingStrategyMock(tataTableClone);
		tataTableClone.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tataClonePrimaryKey = tataTableClone.getPrimaryKey();
		// column for "noise" in select
		Column tataCloneNameColumn = tataTableClone.addColumn("name", String.class);
		
		Table tutuTableClone = new Table("tutu2");
		ClassMapping tutuCloneMappingMock = buildMappingStrategyMock(tutuTableClone);
		tutuTableClone.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tutuClonePrimaryKey = tutuTableClone.getPrimaryKey();
		// column for "noise" in select
		Column tutuCloneNameColumn = tutuTableClone.addColumn("name", String.class);
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		//   Tata.id = Titi.id (Z)
		// Toto.id = tata2.id (X')
		//   tata2.id = tutu2.id (Y')
		
		EntityJoinTree<?, ?> entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMappingAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		String titiAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());
		String tataAddKey2 = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMappingAdapter(tataCloneMappingMock), totoPrimaryKey, tataClonePrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey2 = entityJoinTree.addRelationJoin(tataAddKey2, new EntityMappingAdapter(tutuCloneMappingMock), tataClonePrimaryKey, tutuClonePrimaryKey, null, INNER, null, Collections.emptySet());
		
		List<Integer> depth = new ArrayList<>();
		List<AbstractJoinNode> collectedNodes = new ArrayList<>();
		entityJoinTree.foreachJoinWithDepth(1, (o, abstractJoinNode) -> {
			depth.add(o);
			collectedNodes.add(abstractJoinNode);
			return ++o;
		});
		
		assertThat(depth).isEqualTo(Arrays.asList(1, 1, 2, 2, 2));
		
		assertThat(collectedNodes)
				.withRepresentation(new Printer<>(AbstractJoinNode.class, joinNode -> joinNode.getLeftJoinLink() + " = " + joinNode.getRightJoinLink()))
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
		totoTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> totoPrimaryKey = totoTable.getPrimaryKey();
		// column for "noise" in select
		Column totoNameColumn = totoTable.addColumn("name", String.class);
		
		ClassMapping tataMappingMock = buildMappingStrategyMock("Tata");
		Table tataTable = tataMappingMock.getTargetTable();
		tataTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tataPrimaryKey = tataTable.getPrimaryKey();
		// column for "noise" in select
		Column tataNameColumn = tataTable.addColumn("name", String.class);
		
		ClassMapping tutuMappingMock = buildMappingStrategyMock("Tutu");
		Table tutuTable = tutuMappingMock.getTargetTable();
		tutuTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tutuPrimaryKey = tutuTable.getPrimaryKey();
		// column for "noise" in select
		Column tutuNameColumn = tutuTable.addColumn("name", String.class);
		
		ClassMapping titiMappingMock = buildMappingStrategyMock("Titi");
		Table titiTable = titiMappingMock.getTargetTable();
		titiTable.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> titiPrimaryKey = titiTable.getPrimaryKey();
		// column for "noise" in select
		Column titiNameColumn = titiTable.addColumn("name", String.class);
		
		Table tataTableClone = new Table("tata2");
		ClassMapping tataCloneMappingMock = buildMappingStrategyMock(tataTableClone);
		tataTableClone.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tataClonePrimaryKey = tataTableClone.getPrimaryKey();
		// column for "noise" in select
		Column tataCloneNameColumn = tataTableClone.addColumn("name", String.class);
		
		Table tutuTableClone = new Table("tutu2");
		ClassMapping tutuCloneMappingMock = buildMappingStrategyMock(tutuTableClone);
		tutuTableClone.addColumn("id", long.class).primaryKey();
		PrimaryKey<?, Long> tutuClonePrimaryKey = tutuTableClone.getPrimaryKey();
		// column for "noise" in select
		Column tutuCloneNameColumn = tutuTableClone.addColumn("name", String.class);
		
		// Given following tree:
		// Toto.id = Tata.id (X)
		//   Tata.id = Tutu.id (Y)
		//   Tata.id = Titi.id (Z)
		// Toto.id = tata2.id (X')
		//   tata2.id = tutu2.id (Y')
		
		EntityJoinTree<?, ?> entityJoinTree = new EntityJoinTree(new EntityMappingAdapter(totoMappingMock), totoMappingMock.getTargetTable());
		String tataAddKey = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMappingAdapter(tataMappingMock), totoPrimaryKey, tataPrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(tutuMappingMock), tataPrimaryKey, tutuPrimaryKey, null, INNER, null, Collections.emptySet());
		String titiAddKey = entityJoinTree.addRelationJoin(tataAddKey, new EntityMappingAdapter(titiMappingMock), tataPrimaryKey, titiPrimaryKey, null, INNER, null, Collections.emptySet());
		String tataAddKey2 = entityJoinTree.addRelationJoin(EntityJoinTree.ROOT_JOIN_NAME, new EntityMappingAdapter(tataCloneMappingMock), totoPrimaryKey, tataClonePrimaryKey, null, INNER, null, Collections.emptySet());
		String tutuAddKey2 = entityJoinTree.addRelationJoin(tataAddKey2, new EntityMappingAdapter(tutuCloneMappingMock), tataClonePrimaryKey, tutuClonePrimaryKey, null, INNER, null, Collections.emptySet());

		assertThat(entityJoinTree.giveTables())
				.usingElementComparator(Table.COMPARATOR_ON_SCHEMA_AND_NAME)
				.containsExactlyInAnyOrder(
				totoTable, tataTable, tataTableClone, titiTable, tutuTable, tutuTableClone);
		
	}
	
	@Test
	void lookupTable() {
		EntityJoinTree<?, ?> testInstance = new EntityJoinTree<Object, Object>(mock(EntityInflater.class), new Table<>("root Table"));
		Table table1 = new Table("table1");
		Column table1A = table1.addColumn("table1_a", String.class);
		Table table2 = new Table("table2");
		Column table2A = table1.addColumn("table2_a", String.class);
		Table table3 = new Table("table3");
		Column table3A = table1.addColumn("table3_a", String.class);
		// we create a union that will be joined to a query of a union to check that the algorithm goes in depth 
		Table table4 = new Table("table4");
		Column table4A = table1.addColumn("table4_a", String.class);
		Union table5 = new Union(
				QueryEase.select(table4A, "a").from(table4).innerJoin(table3, "whatever join").getQuery()
		);
		Union union = new Union(
				QueryEase.select(table1A, "a").from(table1).getQuery(),
				QueryEase.select(table2A, "a").from(table2)
						.innerJoin(table3, "whatever join 1")
						.leftOuterJoin(table5.asPseudoTable("a sub pseudo table"), "whatever join 2")
						.getQuery()
		);
		Set<Table<?>> foundTables = testInstance.lookupTable(union.asPseudoTable("a pseudo table"));
		assertThat(foundTables).containsExactlyInAnyOrder(table1, table2, table3, table4);
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