package org.gama.stalactite.persistence.engine.runtime.load;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.runtime.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.engine.runtime.load.EntityTreeInflater.ConsumerNode;
import org.gama.stalactite.persistence.engine.runtime.load.EntityTreeInflater.NodeVisitor;
import org.gama.stalactite.persistence.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.gama.stalactite.persistence.engine.runtime.load.RelationJoinNode.BasicEntityCache;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IRowTransformer;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntityTreeInflaterTest {
	
	@Test
	void transform_doesntGoDeeperIfRelatedBeanIdIsNull() {
		
		Table leftTable = new Table("leftTable");
		Column leftTablePk = leftTable.addColumn("pk", long.class);
		
		Table rightTable = new Table("rightTable");
		Column rightTablePk = leftTable.addColumn("pk", long.class);
		Column rightTableFkToLeftTable = leftTable.addColumn("fkToLeftTable", long.class);
		
		Table rightMostTable = new Table("rightMostTable");
		Column rightMostTablePk = leftTable.addColumn("pk", long.class);
		Column rightMostTableFkToRightTable = leftTable.addColumn("fkToRightTable", long.class);
		
		EntityInflater leftEntityInflater = Mockito.mock(EntityInflater.class);
		// these lines are needed to trigger "main bean instance" creation
		when(leftEntityInflater.giveIdentifier(any(), any())).thenReturn(42);
		when(leftEntityInflater.getEntityType()).thenReturn(Object.class);
		IRowTransformer rightEntityBuilder = mock(IRowTransformer.class);
		when(rightEntityBuilder.transform(any())).thenReturn(new Object());
		when(leftEntityInflater.copyTransformerWithAliases(any())).thenReturn(rightEntityBuilder);
		
		// we create a second inflater to be related with first one, but we'll expect its transform() method not to be invoked
		EntityInflater rightEntityInflater = Mockito.mock(EntityInflater.class);
		when(rightEntityInflater.giveIdentifier(any(), any())).thenReturn(null);
		
		// relation fixer isn't expected to do anything because it won't be called (goal of the test)
		BeanRelationFixer relationFixer = Mockito.mock(BeanRelationFixer.class);
		
		// we create a last inflater to be related with the intermediary one (the right one),
		// but we'll expect none of its method to be invoked
		EntityInflater rightMostEntityInflater = Mockito.mock(EntityInflater.class);
		IRowTransformer rightMostRowTransformerMock = mock(IRowTransformer.class);
		when(rightMostEntityInflater.copyTransformerWithAliases(any())).thenReturn(rightMostRowTransformerMock);
		
		// composing entity tree : leftInflater gets a relation on rightInflater
		EntityJoinTree entityJoinTree = new EntityJoinTree<>(new JoinRoot<>(leftEntityInflater, leftTable));
		String joinName = entityJoinTree.addRelationJoin(
				EntityJoinTree.ROOT_STRATEGY_NAME,
				rightEntityInflater,
				leftTablePk,
				rightTableFkToLeftTable,
				JoinType.OUTER,
				relationFixer);
		entityJoinTree.addRelationJoin(
				joinName,
				rightMostEntityInflater,
				rightTablePk,
				rightMostTableFkToRightTable,
				JoinType.OUTER,
				Mockito.mock(BeanRelationFixer.class));
		
		EntityTreeQuery<Country> entityTreeQuery = new EntityTreeQueryBuilder<>(entityJoinTree).buildSelectQuery(new Dialect().getColumnBinderRegistry());
		EntityTreeInflater testInstance = new EntityTreeInflater<>(entityJoinTree, new ColumnedRow(entityTreeQuery.getColumnAliases()::get));
		
		
		Row databaseData = new Row()
				.add("leftTable_pk", 1)
				.add("rightTable_fkToLeftTable", null)
				.add("rightTable_pk", null)
				;
		testInstance.transform(databaseData, new BasicEntityCache());
		
		verify(rightEntityInflater, times(1)).giveIdentifier(any(), any());
		// because we returned null on giveIdentifier, the transformation algorithm shouldn't ask for relation appliance
		verify(rightMostEntityInflater, times(0)).giveIdentifier(any(), any());
		verify(rightMostRowTransformerMock, times(0)).transform(any());
		verify(relationFixer, times(0)).apply(any(), any());
	}
	
	@Test
	void foreachNode_iteratesInstancesInSameOrderAsJoins_fullTree() {
		Root root = new Root();
		Entity1 entity1 = new Entity1();
		Entity11 entity11 = new Entity11();
		Entity111 entity111 = new Entity111();
		Entity1111 entity1111 = new Entity1111();
		Entity12 entity12 = new Entity12();
		Entity2 entity2 = new Entity2();
		Entity3 entity3 = new Entity3();
		ConsumerNode consumerRoot = new ConsumerNode((DummyJoinRowConsumer) () -> root);
		ConsumerNode consumerNode1 = new ConsumerNode((DummyJoinRowConsumer) () -> entity1);
		ConsumerNode consumerNode11 = new ConsumerNode((DummyJoinRowConsumer) () -> entity11);
		ConsumerNode consumerNode111 = new ConsumerNode((DummyJoinRowConsumer) () -> entity111);
		ConsumerNode consumerNode1111 = new ConsumerNode((DummyJoinRowConsumer) () -> entity1111);
		ConsumerNode consumerNode12 = new ConsumerNode((DummyJoinRowConsumer) () -> entity12);
		ConsumerNode consumerNode2 = new ConsumerNode((DummyJoinRowConsumer) () -> entity2);
		ConsumerNode consumerNode3 = new ConsumerNode((DummyJoinRowConsumer) () -> entity3);
		
		consumerRoot.addConsumer(consumerNode2);
		consumerRoot.addConsumer(consumerNode3);
		consumerRoot.addConsumer(consumerNode1);
		consumerNode1.addConsumer(consumerNode12);
		consumerNode1.addConsumer(consumerNode11);
		consumerNode11.addConsumer(consumerNode111);
		consumerNode111.addConsumer(consumerNode1111);
		
		
		EntityTreeInflater<Root> testInstance = new EntityTreeInflater<>(consumerRoot);
		List<List<Object>> entityStackHistory = new ArrayList<>();
		testInstance.foreachNode(new NodeVisitor(root) {
			@Override
			Object apply(JoinRowConsumer joinRowConsumer, Object parentEntity) {
				List<Object> newStack = new ArrayList<>(Iterables.last(entityStackHistory, new ArrayList<>()));
				newStack.add(parentEntity);
				entityStackHistory.add(newStack);
				if (joinRowConsumer instanceof DummyJoinRowConsumer) {
					return ((DummyJoinRowConsumer) joinRowConsumer).get();
				} else {
					throw new UnsupportedOperationException("Something has changed in test data");
				}
			}
		});
		List<List<Object>> expectedEntityStackHistory = new ArrayList<>();
		expectedEntityStackHistory.add(Arrays.asList(root));
		expectedEntityStackHistory.add(Arrays.asList(root, entity1));
		expectedEntityStackHistory.add(Arrays.asList(root, entity1, entity11));
		expectedEntityStackHistory.add(Arrays.asList(root, entity1, entity11, entity111));
		expectedEntityStackHistory.add(Arrays.asList(root, entity1, entity11, entity111, entity1));
		expectedEntityStackHistory.add(Arrays.asList(root, entity1, entity11, entity111, entity1, root));
		expectedEntityStackHistory.add(Arrays.asList(root, entity1, entity11, entity111, entity1, root, root));
		assertEquals(expectedEntityStackHistory, entityStackHistory);
	}
	
	@Test
	void foreachNode_iteratesInstancesInSameOrderAsJoins_partialTree() {
		Root root = new Root();
		Entity1 entity1 = new Entity1();
		Entity11 entity11 = new Entity11();
		Entity111 entity111 = new Entity111();
		Entity1111 entity1111 = new Entity1111();
		Entity12 entity12 = new Entity12();
		Entity2 entity2 = new Entity2();
		Entity3 entity3 = new Entity3();
		ConsumerNode consumerRoot = new ConsumerNode((DummyJoinRowConsumer) () -> root);
		ConsumerNode consumerNode1 = new ConsumerNode((DummyJoinRowConsumer) () -> entity1);
		ConsumerNode consumerNode11 = new ConsumerNode((DummyJoinRowConsumer) () -> entity11);
		ConsumerNode consumerNode111 = new ConsumerNode((DummyJoinRowConsumer) () -> null);
		ConsumerNode consumerNode1111 = new ConsumerNode((DummyJoinRowConsumer) () -> entity1111);
		ConsumerNode consumerNode12 = new ConsumerNode((DummyJoinRowConsumer) () -> entity12);
		ConsumerNode consumerNode2 = new ConsumerNode((DummyJoinRowConsumer) () -> entity2);
		ConsumerNode consumerNode3 = new ConsumerNode((DummyJoinRowConsumer) () -> entity3);
		
		consumerRoot.addConsumer(consumerNode2);
		consumerRoot.addConsumer(consumerNode3);
		consumerRoot.addConsumer(consumerNode1);
		consumerNode1.addConsumer(consumerNode12);
		consumerNode1.addConsumer(consumerNode11);
		consumerNode11.addConsumer(consumerNode111);
		consumerNode111.addConsumer(consumerNode1111);
		
		
		EntityTreeInflater<Root> testInstance = new EntityTreeInflater<>(consumerRoot);
		List<List<Object>> entityStackHistory = new ArrayList<>();
		testInstance.foreachNode(new NodeVisitor(root) {
			@Override
			Object apply(JoinRowConsumer joinRowConsumer, Object parentEntity) {
				List<Object> newStack = new ArrayList<>(Iterables.last(entityStackHistory, new ArrayList<>()));
				newStack.add(parentEntity);
				entityStackHistory.add(newStack);
				if (joinRowConsumer instanceof DummyJoinRowConsumer) {
					return ((DummyJoinRowConsumer) joinRowConsumer).get();
				} else {
					throw new UnsupportedOperationException("Something has changed in test data");
				}
			}
		});
		List<List<Object>> expectedEntityStackHistory = new ArrayList<>();
		expectedEntityStackHistory.add(Arrays.asList(root));
		expectedEntityStackHistory.add(Arrays.asList(root, entity1));
		expectedEntityStackHistory.add(Arrays.asList(root, entity1, entity11));
		expectedEntityStackHistory.add(Arrays.asList(root, entity1, entity11, entity1));
		expectedEntityStackHistory.add(Arrays.asList(root, entity1, entity11, entity1, root));
		expectedEntityStackHistory.add(Arrays.asList(root, entity1, entity11, entity1, root, root));
		assertEquals(expectedEntityStackHistory, entityStackHistory);
	}
	
	static class Root {
		
	}
	
	static class Entity1 {
		
	}
	
	static class Entity11 {
		
	}
	
	static class Entity111 {
		
	}
	
	static class Entity1111 {
		
	}
	
	static class Entity12 {
		
	}
	
	static class Entity2 {
		
	}
	
	static class Entity3 {
		
	}
	
	private interface DummyJoinRowConsumer extends JoinRowConsumer, Supplier {
		
	}
}