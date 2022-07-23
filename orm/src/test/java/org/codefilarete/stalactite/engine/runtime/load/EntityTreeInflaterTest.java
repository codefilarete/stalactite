package org.codefilarete.stalactite.engine.runtime.load;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.ConsumerNode;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.NodeVisitor;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.RelationIdentifier;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.engine.runtime.load.RelationJoinNode.RelationJoinRowConsumer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
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
		RowTransformer rightEntityBuilder = mock(RowTransformer.class);
		when(rightEntityBuilder.transform(any())).thenReturn(new Object());
		when(leftEntityInflater.copyTransformerWithAliases(any(ColumnedRow.class))).thenReturn(rightEntityBuilder);
		
		// we create a second inflater to be related with first one, but we'll expect its transform() method not to be invoked
		EntityInflater rightEntityInflater = Mockito.mock(EntityInflater.class);
		when(rightEntityInflater.giveIdentifier(any(), any())).thenReturn(null);
		
		// relation fixer isn't expected to do anything because it won't be called (goal of the test)
		BeanRelationFixer relationFixer = Mockito.mock(BeanRelationFixer.class);
		
		// we create a last inflater to be related with the intermediary one (the right one),
		// but we'll expect none of its method to be invoked
		EntityInflater rightMostEntityInflater = Mockito.mock(EntityInflater.class);
		RowTransformer rightMostRowTransformerMock = mock(RowTransformer.class);
		when(rightMostEntityInflater.copyTransformerWithAliases(any(ColumnedRow.class))).thenReturn(rightMostRowTransformerMock);
		
		// composing entity tree : leftInflater gets a relation on rightInflater
		EntityJoinTree entityJoinTree = new EntityJoinTree<>(leftEntityInflater, leftTable);
		String joinName = entityJoinTree.addRelationJoin(
				EntityJoinTree.ROOT_STRATEGY_NAME,
				rightEntityInflater,
				leftTablePk,
				rightTableFkToLeftTable,
				null,
				JoinType.OUTER,
				relationFixer, Collections.emptySet());
		entityJoinTree.addRelationJoin(
				joinName,
				rightMostEntityInflater,
				rightTablePk,
				rightMostTableFkToRightTable,
				null,
				JoinType.OUTER,
				Mockito.mock(BeanRelationFixer.class), Collections.emptySet());
		
		EntityTreeQuery<Country> entityTreeQuery = new EntityTreeQueryBuilder<>(entityJoinTree, new Dialect().getColumnBinderRegistry()).buildSelectQuery();
		EntityTreeInflater testInstance = entityTreeQuery.getInflater();
		
		
		Row databaseData = new Row()
				.add("leftTable_pk", 1)
				.add("rightTable_fkToLeftTable", null)
				.add("rightTable_pk", null)
				;
		testInstance.transform(databaseData, testInstance.new TreeInflationContext());
		
		// giveIdentifier is invoked twice because of identifier computation and relation identifier computation
		verify(rightEntityInflater, times(2)).giveIdentifier(any(), any());
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
		
		
		EntityTreeInflater<Root> testInstance = new EntityTreeInflater<>(consumerRoot, null, null);
		List<List<Object>> entityStackHistory = new ArrayList<>();
		testInstance.foreachNode(new NodeVisitor(root) {
			@Override
			EntityCreationResult apply(ConsumerNode joinRowConsumer, Object parentEntity) {
				List<Object> newStack = new ArrayList<>(Iterables.last(entityStackHistory, new ArrayList<>()));
				newStack.add(parentEntity);
				entityStackHistory.add(newStack);
				if (joinRowConsumer.getConsumer() instanceof DummyJoinRowConsumer) {
					return new EntityCreationResult(((DummyJoinRowConsumer) joinRowConsumer.getConsumer()).get(), joinRowConsumer);
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
		assertThat(entityStackHistory).isEqualTo(expectedEntityStackHistory);
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
		
		
		EntityTreeInflater<Root> testInstance = new EntityTreeInflater<>(consumerRoot, null, null);
		List<List<Object>> entityStackHistory = new ArrayList<>();
		testInstance.foreachNode(new NodeVisitor(root) {
			@Override
			EntityCreationResult apply(ConsumerNode joinRowConsumer, Object parentEntity) {
				List<Object> newStack = new ArrayList<>(Iterables.last(entityStackHistory, new ArrayList<>()));
				newStack.add(parentEntity);
				entityStackHistory.add(newStack);
				if (joinRowConsumer.getConsumer() instanceof DummyJoinRowConsumer) {
					return new EntityCreationResult(((DummyJoinRowConsumer) joinRowConsumer.getConsumer()).get(), joinRowConsumer);
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
		assertThat(entityStackHistory).isEqualTo(expectedEntityStackHistory);
	}
	
	/**
	 * Those tests are made as anti-regression ones : if RelationIdentifier equals/hashCode change it would break some hard to debug case 
	 */
	@Nested
	class RelationIdentifierTest {
		
		private Entity1 rootEntityReference;
		private RelationIdentifier reference;
		private RelationJoinRowConsumer rowConsumerReference;
		
		@BeforeEach
		void init() {
			rowConsumerReference = mock(RelationJoinRowConsumer.class);
			
			rootEntityReference = new Entity1();
			reference = new RelationIdentifier(rootEntityReference, Entity1.class, 1, rowConsumerReference);
		}			
		
		@Test
		void equals_verySameInstance() {
			assertThat(reference).isEqualTo(reference);
		}
		
		@Test
		void equals_instanceWithSameValues() {
			// all informations are the same => equality is true
			RelationIdentifier id2 = new RelationIdentifier(rootEntityReference, Entity1.class, 1, rowConsumerReference);
			assertThat(reference).isEqualTo(id2);
		}
		
		@Test
		void equals_instancesDifferOnIdentifier() {
			// instances differ on identifier => equality is false
			RelationIdentifier id3 = new RelationIdentifier(rootEntityReference, Entity1.class, 2, rowConsumerReference);
			assertThat(reference).isNotEqualTo(id3);
		}
		
		@Test
		void equals_instancesDifferIdentifierWithOverriddenEquals() {
			// all information are the same => equality is true
			RelationIdentifier id7 = new RelationIdentifier(rootEntityReference, Entity1.class, new Object() {
				@Override
				public boolean equals(Object obj) {
					return true;
				}
			}, rowConsumerReference);
			assertThat(id7).isEqualTo(reference);
		}
		
		@Test
		void equals_instancesDifferOnRoot() {
			// instances differ on root => equality is false
			RelationIdentifier id4 = new RelationIdentifier(new Object(), Entity1.class, 1, rowConsumerReference);
			assertThat(reference).isNotEqualTo(id4);
		}
		
		@Test
		void equals_instancesDifferOnEntityType() {
			// instances differ on entity type => equality is false
			RelationIdentifier id5 = new RelationIdentifier(rootEntityReference, Entity2.class, 1, rowConsumerReference);
			assertThat(reference).isNotEqualTo(id5);
		}
		
		@Test
		void equals_instancesDifferOnNode() {
			// instances differ on consumer type => equality is false
			RelationJoinRowConsumer rowConsumer2 = mock(RelationJoinRowConsumer.class);
			RelationIdentifier id6 = new RelationIdentifier(rootEntityReference, Entity1.class, 1, rowConsumer2);
			assertThat(reference).isNotEqualTo(id6);
		}
		
		@Test
		void hashCode_verySameInstance() {
			assertThat(reference.hashCode()).isEqualTo(reference.hashCode());
		}
		
		@Test
		void hashCode_instanceWithSameValues() {
			// all informations are the same => equality is true
			RelationIdentifier id2 = new RelationIdentifier(rootEntityReference, Entity1.class, 1, rowConsumerReference);
			assertThat(reference.hashCode()).isEqualTo(id2.hashCode());
		}
		
		@Test
		void hashCode_instancesDifferOnIdentifier() {
			// instances differ on identifier => equality is false
			RelationIdentifier id3 = new RelationIdentifier(rootEntityReference, Entity1.class, 2, rowConsumerReference);
			assertThat(reference.hashCode()).isNotEqualTo(id3.hashCode());
		}
		
		@Test
		void hashCode_instancesDifferOnRoot() {
			// instances differ on root => equality is false
			RelationIdentifier id4 = new RelationIdentifier(new Object(), Entity1.class, 1, rowConsumerReference);
			assertThat(reference.hashCode()).isNotEqualTo(id4.hashCode());
		}
		
		@Test
		void hashCode_instancesDifferOnEntityType() {
			// instances differ on entity type => equality is false
			RelationIdentifier id5 = new RelationIdentifier(rootEntityReference, Entity2.class, 1, rowConsumerReference);
			assertThat(reference.hashCode()).isNotEqualTo(id5.hashCode());
		}
		
		@Test
		void hashCode_instancesDifferOnNode() {
			// instances differ on consumer type => equality is false
			RelationJoinRowConsumer rowConsumer2 = mock(RelationJoinRowConsumer.class);
			RelationIdentifier id6 = new RelationIdentifier(rootEntityReference, Entity1.class, 1, rowConsumer2);
			assertThat(reference.hashCode()).isNotEqualTo(id6.hashCode());
		}
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