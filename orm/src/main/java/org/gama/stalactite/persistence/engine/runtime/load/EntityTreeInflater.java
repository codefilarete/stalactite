package org.gama.stalactite.persistence.engine.runtime.load;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;

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

import static org.gama.stalactite.persistence.engine.runtime.load.EntityTreeInflater.TreeIterationPursue.CONTINUE;
import static org.gama.stalactite.persistence.engine.runtime.load.EntityTreeInflater.TreeIterationPursue.DONT_GO_DEEPER;

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
				transform(rows, resultSize, entityCache)
		);
	}
	
	private List<C> transform(Iterable<Row> rows, int resultSize, EntityCache entityCache) {
		List<C> result = new ArrayList<>(resultSize);
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
			foreachNode(join -> {
				Object rowInstance = result.get();
				// processing current depth
				if (join instanceof PassiveJoinNode.PassiveJoinRowConsumer) {
					((PassiveJoinRowConsumer) join).consume(rowInstance, row);
					return CONTINUE;
				} else if (join instanceof MergeJoinNode.MergeJoinRowConsumer) {
					((MergeJoinRowConsumer) join).mergeProperties(rowInstance, row);
					return CONTINUE;
				} else if (join instanceof RelationJoinNode.RelationJoinRowConsumer) {
					boolean relationApplied = ((RelationJoinRowConsumer) join).applyRelatedEntity(rowInstance, row, entityCache);
					return relationApplied ? CONTINUE : DONT_GO_DEEPER;
				} else {
					// Developer made something wrong because other types than MergeJoin and RelationJoin are not expected
					throw new IllegalArgumentException("Unexpected join type, only "
							+ Reflections.toString(PassiveJoinRowConsumer.class)
							+ ", " + Reflections.toString(MergeJoinRowConsumer.class)
							+ " and " + Reflections.toString(RelationJoinRowConsumer.class) + " are handled"
							+ ", not " + Reflections.toString(join.getClass()));
				}
			});
		}
		return result;
	}
	
	private void foreachNode(Function<JoinRowConsumer, TreeIterationPursue> consumer) {
		Queue<ConsumerNode> stack = new ArrayDeque<>(this.consumerRoot.consumers);
		while (!stack.isEmpty()) {
			ConsumerNode joinNode = stack.poll();
			TreeIterationPursue pursue = consumer.apply(joinNode.consumer);
			if (pursue == CONTINUE) {
				stack.addAll(joinNode.consumers);
			} // else DONT_GO_DEEPER => nothing to do
		}
	}
	
	/**
	 * Small structure to store {@link JoinRootRowConsumer} as a tree that reflects {@link EntityJoinTree} input.
	 */
	private static class ConsumerNode {
		
		private final JoinRowConsumer consumer;
		
		private final List<ConsumerNode> consumers = new ArrayList<>();
		
		private ConsumerNode(JoinRowConsumer consumer) {
			this.consumer = consumer;
		}
		
		private void addConsumer(ConsumerNode consumer) {
			this.consumers.add(consumer);
		}
	}
	
	enum TreeIterationPursue {
		CONTINUE,
		DONT_GO_DEEPER;
	}
}
