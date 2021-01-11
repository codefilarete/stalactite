package org.gama.stalactite.persistence.engine.runtime.load;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.ThreadLocals;
import org.gama.stalactite.persistence.engine.runtime.load.JoinRoot.JoinRootRowConsumer;
import org.gama.stalactite.persistence.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.gama.stalactite.persistence.engine.runtime.load.PassiveJoinNode.PassiveJoinRowConsumer;
import org.gama.stalactite.persistence.engine.runtime.load.RelationJoinNode.BasicEntityCache;
import org.gama.stalactite.persistence.engine.runtime.load.RelationJoinNode.EntityCache;
import org.gama.stalactite.persistence.engine.runtime.load.RelationJoinNode.RelationJoinRowConsumer;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.sql.result.Row;

/**
 * Bean graph creator from database rows. Based on a tree of {@link ConsumerNode}s which wraps some {@link JoinRowConsumer}.
 * 
 * @param <C> main entity type
 * @author Guillaume Mary
 */
public class EntityTreeInflater<C> {
	
	private static final ThreadLocal<EntityCache> CURRENT_ENTITY_CACHE = new ThreadLocal<>();
	
	/**
	 * All inflaters, mergers and so on, in a tree structure that reflects {@link EntityJoinTree}.
	 * Made as such to benefit from the possbility to cancel in-depth iteration in {@link #transform(Row, EntityCache)} method during relation
	 * building
	 */
	private final ConsumerNode consumerRoot;
	
	
	EntityTreeInflater(ConsumerNode consumerRoot) {
		this.consumerRoot = consumerRoot;
	}
	
	public EntityTreeInflater(EntityJoinTree<C, ?> entityJoinTree, ColumnedRow columnedRow) {
		this.consumerRoot = new ConsumerNode(entityJoinTree.getRoot().toConsumer(columnedRow));
		buildConsumerTree(entityJoinTree, columnedRow);
	}
	
	private void buildConsumerTree(EntityJoinTree<C, ?> entityJoinTree, ColumnedRow columnedRow) {
		entityJoinTree.foreachJoinWithDepth(this.consumerRoot, (targetOwner, currentNode) -> {
			ConsumerNode consumerNode = new ConsumerNode(currentNode.toConsumer(columnedRow));
			targetOwner.addConsumer(consumerNode);
			return consumerNode;
		});
	}
	
	/**
	 *
	 * @param rows rows (coming from database select) to be read to build beans graph
	 * @param resultSize expected reuslt size, only for resulting list optimization
	 * @return a list of root beans, built from given rows by asking internal strategy joins to instanciate and complete them
	 */
	public List<C> transform(Iterable<Row> rows, int resultSize) {
		return ThreadLocals.doWithThreadLocal(CURRENT_ENTITY_CACHE, BasicEntityCache::new, (Function<EntityCache, List<C>>) entityCache ->
				new ArrayList<>(transform(rows, resultSize, entityCache))
		);
	}
	
	private Set<C> transform(Iterable<Row> rows, int resultSize, EntityCache entityCache) {
		// we use an "IdentitySet" (doesn't exist directly, but can be done through IdentityHashMap) to avoid duplicate entity : with a HashSet
		// duplicate can happen if equals/hashCode depends on relation, in particular Collection ones, because they are filled from row to row
		// making hashCode value change
		Set<C> result = Collections.newSetFromMap(new IdentityHashMap<>(resultSize));
		for (Row row : rows) {
			Nullable<C> newInstance = transform(row, entityCache);
			newInstance.invoke(result::add);
		}
		return result;
	}
	
	Nullable<C> transform(Row row, EntityCache entityCache) {
		// Algorithm : we iterate depth by depth the tree structure of the joins
		// We start by the root of the hierarchy.
		// We process the entity of the current depth, then process the direct relations, add those relations to the depth iterator
		Nullable<C> result = Nullable.nullable(((JoinRootRowConsumer<C, ?>) this.consumerRoot.consumer).createRootInstance(row, entityCache));
		
		if (result.isPresent()) {
			foreachNode(new NodeVisitor(result.get()) {
				
				@Override
				public Object apply(JoinRowConsumer join, Object entity) {
					// processing current depth
					if (join instanceof PassiveJoinNode.PassiveJoinRowConsumer) {
						((PassiveJoinRowConsumer) join).consume(entity, row);
						return entity;
					} else if (join instanceof MergeJoinNode.MergeJoinRowConsumer) {
						((MergeJoinRowConsumer) join).mergeProperties(entity, row);
						return entity;
					} else if (join instanceof RelationJoinNode.RelationJoinRowConsumer) {
						return ((RelationJoinRowConsumer) join).applyRelatedEntity(entity, row, entityCache);
					} else {
						// Developer made something wrong because other types than MergeJoin and RelationJoin are not expected
						throw new IllegalArgumentException("Unexpected join type, only "
								+ Reflections.toString(PassiveJoinRowConsumer.class)
								+ ", " + Reflections.toString(MergeJoinRowConsumer.class)
								+ " and " + Reflections.toString(RelationJoinRowConsumer.class) + " are handled"
								+ ", not " + Reflections.toString(join.getClass()));
					}
				}
			});
		}
		return result;
	}
	
	@VisibleForTesting
	void foreachNode(NodeVisitor consumer) {
		Queue<ConsumerNode> joinNodeStack = Collections.asLifoQueue(new ArrayDeque<>());
		joinNodeStack.addAll(this.consumerRoot.consumers);
		// Maintaining entities that will be given to each node : they are entities produced by parent node
		Map<ConsumerNode, Object> entityPerNode = new HashMap<>(10);
		joinNodeStack.forEach(node -> entityPerNode.put(node, consumer.entityRoot));
		
		while (!joinNodeStack.isEmpty()) {
			ConsumerNode joinNode = joinNodeStack.poll();
			Object entity = consumer.apply(joinNode.consumer, entityPerNode.get(joinNode));
			if (entity != null) {
				joinNodeStack.addAll(joinNode.consumers);
				joinNode.consumers.forEach(node -> entityPerNode.put(node, entity));
			}
		}
	}
	
	/**
	 * Small structure to store {@link JoinRootRowConsumer} as a tree that reflects {@link EntityJoinTree} input.
	 */
	static class ConsumerNode {
		
		private final JoinRowConsumer consumer;
		
		private final List<ConsumerNode> consumers = new ArrayList<>();
		
		ConsumerNode(JoinRowConsumer consumer) {
			this.consumer = consumer;
		}
		
		void addConsumer(ConsumerNode consumer) {
			this.consumers.add(consumer);
		}
	}
	
	@VisibleForTesting
	static abstract class NodeVisitor {
		
		private final Object entityRoot;
		
		NodeVisitor(Object entityRoot) {
			this.entityRoot = entityRoot;
		}
		
		/**
		 * Asks for parentEntity consumption by {@link JoinRootRowConsumer}
		 * @param joinRowConsumer consumer expected to use given entity to constructs, fills, does whatever, with given entity
		 * @param parentEntity entity on which consumer mecanism may apply
		 * @return the optional entity created by consumer (as in one-to-one or one-to-many relation), else given parentEntity (not null)
		 */
		abstract Object apply(JoinRowConsumer joinRowConsumer, Object parentEntity);
	}
}
