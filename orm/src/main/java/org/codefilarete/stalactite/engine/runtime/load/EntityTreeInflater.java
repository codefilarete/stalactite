package org.codefilarete.stalactite.engine.runtime.load;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.collections4.map.LinkedMap;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.NodeVisitor.EntityCreationResult;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.EntityReference;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.ExcludingJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.ForkJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.RootJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.PassiveJoinNode.PassiveJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.RelationJoinNode.BasicEntityCache;
import org.codefilarete.stalactite.engine.runtime.load.RelationJoinNode.EntityCache;
import org.codefilarete.stalactite.engine.runtime.load.RelationJoinNode.RelationJoinRowConsumer;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.ThreadLocals;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Bean graph creator from database rows. Based on a tree of {@link ConsumerNode}s which wraps some {@link JoinRowConsumer}.
 * 
 * @param <C> main entity type
 * @author Guillaume Mary
 */
public class EntityTreeInflater<C> {
	
	public static final Logger LOGGER = LoggerFactory.getLogger(EntityTreeInflater.class);
	
	@SuppressWarnings("java:S5164" /* remove() is called by ThreadLocals.AutoRemoveThreadLocal */)
	private static final ThreadLocal<EntityTreeInflater.TreeInflationContext> CURRENT_CONTEXT = new ThreadLocal<>();
	
	/**
	 * Gives current {@link TreeInflationContext}. A current one is available during {@link #transform(Iterable, int)} invocation.
	 * 
	 * @return current {@link TreeInflationContext}, null if you're not invoking it during its lifecycle
	 */
	public static EntityTreeInflater.TreeInflationContext currentContext() {
		return CURRENT_CONTEXT.get();
	}
	
	/**
	 * All inflaters, mergers and so on, in a tree structure that reflects {@link EntityJoinTree}.
	 * Made as such to benefit from the possibility to cancel in-depth iteration in {@link #transform(ColumnedRow, TreeInflationContext)} method during relation
	 * building
	 */
	private final ConsumerNode consumerRoot;
	
	/**
	 * Constructor with necessary elements
	 * @param consumerRoot top level row consumer, the one that will compute root instances
	 */
	EntityTreeInflater(ConsumerNode consumerRoot) {
		this.consumerRoot = consumerRoot;
	}
	
	/**
	 *
	 * @param rows rows (coming from database select) to be read to build beans graph
	 * @param resultSize expected result size, only for resulting list optimization
	 * @return a set of root beans, built from given rows by asking internal strategy joins to instantiate and complete them
	 */
	public Set<C> transform(Iterable<? extends ColumnedRow> rows, int resultSize) {
		return ThreadLocals.doWithThreadLocal(CURRENT_CONTEXT, TreeInflationContext::new, (Function<EntityTreeInflater.TreeInflationContext, Set<C>>) context ->
						transform(rows, resultSize, context));
	}
	
	private Set<C> transform(Iterable<? extends ColumnedRow> rows, int resultSize, EntityTreeInflater.TreeInflationContext context) {
		// we use an "IdentitySet" (doesn't exist directly, but can be done through IdentityLinkedMap) to avoid duplicate entity : with a HashSet
		// duplicate can happen if equals/hashCode depends on relation, in particular Collection ones, because they are filled from row to row
		// making hashCode value change. Moreover we need to keep track of the order because query might be sorted through "order by" clause.
		Set<C> result = java.util.Collections.newSetFromMap(new IdentityLinkedMap<>(resultSize));
		for (ColumnedRow row : rows) {
			Nullable<C> newInstance = transform(row, context);
			newInstance.invoke(result::add);
		}
		return result;
	}
	
	Nullable<C> transform(ColumnedRow row, EntityTreeInflater.TreeInflationContext context) {
		context.setCurrentRow(row);
		// Algorithm : we iterate depth by depth the tree structure of the joins
		// We start by hierarchy root.
		// We process entity of current depth, process the direct relations, then add those relations to depth iterator
		LOGGER.debug("Creating instance with {}", this.consumerRoot.consumer);

		ColumnedRow rootRow = currentContext().getDecoder(consumerRoot.consumer.getNode());
		EntityCreationResult rootEntityCreationResult = getRootEntityCreationResult(rootRow, context);

		if (rootEntityCreationResult != null) {
			foreachNode(rootEntityCreationResult.consumers, new NodeVisitor(rootEntityCreationResult.entity) {
				
				@Override
				public EntityCreationResult apply(ConsumerNode join, EntityReference<?, ?> entityReference) {
					// processing current depth
					JoinRowConsumer consumer = join.getConsumer();
					LOGGER.debug("Consuming {} on object {}#{}", consumer, Reflections.toString(entityReference.getEntity().getClass()), entityReference.getIdentifier());
					consumer.beforeRowConsumption(context);
					ColumnedRow nodeRow = currentContext().getDecoder(consumer.getNode());
					if (consumer instanceof PassiveJoinNode.PassiveJoinRowConsumer) {
						((PassiveJoinRowConsumer) consumer).consume(entityReference, nodeRow);
						return new EntityCreationResult(entityReference, join);
					} else if (consumer instanceof MergeJoinNode.MergeJoinRowConsumer) {
						((MergeJoinRowConsumer) consumer).mergeProperties(entityReference.getEntity(), nodeRow);
						return new EntityCreationResult(entityReference, join);
					} else if (consumer instanceof RelationJoinNode.RelationJoinRowConsumer) {
						EntityReference<?, ?> relatedEntityReference = ((RelationJoinRowConsumer) consumer).applyRelatedEntity(entityReference, nodeRow, context);
						EntityCreationResult result;
						if (consumer instanceof ForkJoinRowConsumer) {
							// In case of join-table polymorphism we have to provide the tree branch on which id was found
							// in order to let created entity filled with right consumers. "Wrong" branches serve no purpose. 
							JoinRowConsumer nextRowConsumer = ((ForkJoinRowConsumer) consumer).giveNextConsumer();
							if (nextRowConsumer == null) {
								// means no identifier of polymorphic entity
								result = new EntityCreationResult(null, (List<ConsumerNode>) null);
							} else {
								Optional<ConsumerNode> consumerNode = join.consumers.stream().filter(c -> nextRowConsumer == c.consumer).findFirst();
								if (!consumerNode.isPresent()) {
									throw new IllegalStateException("Can't find consumer node for " + nextRowConsumer + " in " + join.consumers);
								} else {
									result = new EntityCreationResult(relatedEntityReference, Arrays.asList(consumerNode.get()));
								}
							}
						} else {
							result = new EntityCreationResult(relatedEntityReference, join);
						}
						consumer.afterRowConsumption(context);
						return result;
					} else {
						// Developer made something wrong because other types than MergeJoin and RelationJoin are not expected
						throw new IllegalArgumentException("Unexpected join type, only "
								+ Reflections.toString(PassiveJoinRowConsumer.class)
								+ ", " + Reflections.toString(MergeJoinRowConsumer.class)
								+ " and " + Reflections.toString(RelationJoinRowConsumer.class) + " are handled"
								+ ", not " + (consumer == null ? "null" : Reflections.toString(consumer.getClass())));
					}
				}
			});
		}
		return nullable(rootEntityCreationResult).map(c -> (C) c.entity.getEntity());
	}
	
	private EntityCreationResult getRootEntityCreationResult(ColumnedRow row, EntityTreeInflater.TreeInflationContext context) {
		EntityReference<C, ?> rootInstance = ((RootJoinRowConsumer<C, ?>) this.consumerRoot.consumer).createRootInstance(row, context);
		if (rootInstance != null) {
			if (consumerRoot.consumer instanceof ExcludingJoinRowConsumer) {
				// In case of polymorphism we have to provide the tree branch on which instance was found
				// in order to let created entity filled with right consumers. "Wrong" branches serve no purpose. 
				Set<JoinRowConsumer> deadBranches = ((ExcludingJoinRowConsumer) consumerRoot.consumer).giveExcludedConsumers();
				ArrayList<ConsumerNode> consumerNodes = new ArrayList<>(consumerRoot.consumers);
				consumerNodes.removeIf(consumer -> deadBranches.contains(consumer.consumer));
				return new EntityCreationResult(rootInstance, consumerNodes);
			} else {
				return new EntityCreationResult(rootInstance, consumerRoot.consumers);
			}
		} else {
			return null;
		}
	}
	
	@VisibleForTesting
	void foreachNode(Collection<ConsumerNode> seeds, NodeVisitor nodeVisitor) {
		Queue<ConsumerNode> joinNodeStack = Collections.newLifoQueue();
		joinNodeStack.addAll(seeds);
		// Maintaining entities that will be given to each node : they are entities produced by parent node
		Map<ConsumerNode, EntityReference<?, ?>> entityPerConsumer = new HashMap<>(10);
		joinNodeStack.forEach(node -> entityPerConsumer.put(node, nodeVisitor.entityRoot));

		while (!joinNodeStack.isEmpty()) {
			ConsumerNode joinNode = joinNodeStack.poll();
			EntityCreationResult result = nodeVisitor.apply(joinNode, entityPerConsumer.get(joinNode));
			if (result.getEntity() != null) {
				List<ConsumerNode> nextConsumers = result.nextConsumers();
				joinNodeStack.addAll(nextConsumers);
				nextConsumers.forEach(node -> entityPerConsumer.put(node, result.getEntity()));
			}
		}
	}

	/**
	 * Small structure to store {@link JoinRowConsumer} as a tree that reflects {@link EntityJoinTree} input.
	 */
	static class ConsumerNode {
		
		private final JoinRowConsumer consumer;
		
		private final List<ConsumerNode> consumers = new ArrayList<>();
		
		ConsumerNode(JoinRowConsumer consumer) {
			this.consumer = consumer;
		}
		
		public JoinRowConsumer getConsumer() {
			return consumer;
		}
		
		void addConsumer(ConsumerNode consumer) {
			this.consumers.add(consumer);
		}
	}
	
	static abstract class NodeVisitor {
		
		private final EntityReference<?, ?> entityRoot;
		
		NodeVisitor(EntityReference<?, ?> entityRoot) {
			this.entityRoot = entityRoot;
		}
		
		/**
		 * Asks for parentEntity consumption by {@link ConsumerNode}
		 * @param consumerNode consumer expected to use the given entity to construct, fill, do whatever, with it
		 * @param parentEntity entity on which the consumer mechanism may apply
		 * @return the optional entity created by consumer (as in one-to-one or one-to-many relation), else given parentEntity (not null)
		 */
		abstract EntityCreationResult apply(ConsumerNode consumerNode, EntityReference<?, ?> parentEntity);
		
		static class EntityCreationResult {
			
			private final EntityReference<?, ?> entity;
			
			private final List<ConsumerNode> consumers;
			
			EntityCreationResult(EntityReference<?, ?> entity, ConsumerNode entityCreator) {
				this(entity, entityCreator.consumers);
			}
			
			EntityCreationResult(EntityReference<?, ?> entity, List<ConsumerNode> consumers) {
				this.entity = entity;
				this.consumers = consumers;
			}
			
			public EntityReference<?, ?> getEntity() {
				return entity;
			}
			
			List<ConsumerNode> nextConsumers() {
				return consumers;
			}
		}
	}
	
	/**
	 * Represents a relation between an entity and another one through its join node 
	 */
	static class RelationIdentifier {
		
		protected final Object rootEntity;
		protected final Class relatedEntityType;
		protected final Object relatedBeanIdentifier;
		protected final RelationJoinRowConsumer joinNode;
		
		RelationIdentifier(Object rootEntity, Class<?> relatedEntityType, Object relatedBeanIdentifier, RelationJoinRowConsumer joinNode) {
			this.rootEntity = rootEntity;
			this.relatedEntityType = relatedEntityType;
			this.relatedBeanIdentifier = relatedBeanIdentifier;
			this.joinNode = joinNode;
		}
		
		/**
		 * Implemented so inflater algorithm can check for already treated relation
		 * @param o another RelationIdentifier
		 * @return true if given instance equals this one
		 */
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof EntityTreeInflater.RelationIdentifier)) return false;
			
			RelationIdentifier other = (RelationIdentifier) o;
			
			// WARN : this is finely defined according to :
			// - comparison of root entity is based on instance comparison to avoid being dependent of equals() implementation which may vary during inflation process
			// - comparison with related bean is based on its identifier with Object equality because it is expected to be simple and comparable type
			// - comparison of Join Node which stores kind of "relation name" that links current beans (Object equality could be used but using instance matched more our expectation)
			if (rootEntity != other.rootEntity) return false;
			if (!relatedEntityType.equals(other.relatedEntityType)) return false;
			if (!relatedBeanIdentifier.equals(other.relatedBeanIdentifier)) return false;
			return joinNode == other.joinNode;
		}
		
		@Override
		public int hashCode() {
			int result = rootEntity.hashCode();
			result = 31 * result + relatedEntityType.hashCode();
			result = 31 * result + relatedBeanIdentifier.hashCode();
			result = 31 * result + joinNode.hashCode();
			return result;
		}
	}
	
	/**
	 * Container for information used during {@link Row} transformation as a bean graph.
	 * Accessible from {@link EntityTreeInflater#currentContext()} during its lifecycle:
	 * - instanced at beginning of {@link Row} transformation ({@link EntityTreeInflater#transform(Iterable, int)}
	 * - dropped at the end of the method
	 * 
	 * @implNote made non-static to ease access to the surrounding {@link EntityTreeInflater} instance
	 */
	public static class TreeInflationContext {
		
		/** Entity cache */
		private final EntityCache entityCache;
		
		/** Storage for treated relations */
		private final Set<RelationIdentifier> treatedRelations = new HashSet<>();
		
		/**
		 * Currently (Thread-linked) row read by inflater.
		 * This row is not linked to any node and can be used has a base to construct some other {@link ColumnedRow}
		 * based on some {@link JoinNode}.
		 * @see #getDecoder
		 */
		private ColumnedRow currentRow;
		
		private TreeInflationContext() {
			this(new BasicEntityCache());
		}
		
		public TreeInflationContext(EntityCache entityCache) {
			this.entityCache = entityCache;
		}
		
		private TreeInflationContext setCurrentRow(ColumnedRow currentRow) {
			this.currentRow = currentRow;
			return this;
		}
		
		/**
		 * Gives the value provider of current {@link java.sql.ResultSet} row for given {@link JoinNode}.
		 * 
		 *
		 * @param joinNode relation node identifier 
		 * @return null if data is null
		 * @see EntityTreeQueryBuilder#cloneTable(JoinNode)  table clone mechanism.
		 */
		 public ColumnedRow getDecoder(JoinNode<?, ?> joinNode) {
			Fromable table = joinNode.getTable();
			return new ColumnedRow() {
				@Override
				public <E> E get(Selectable<E> key) {
					Selectable<E> column = table.findColumn(key.getExpression());
					if (column == null) {
						// This is more for debugging purpose than for a real production goal, may be removed later
						throw new MappingConfigurationException("Can't find column for " + key.getExpression() + " in table : " + joinNode.getTable().getName());
					}
					return currentRow.get(column);
				}
			};
		}
		
		public boolean isTreatedOrAppend(RelationIdentifier relationIdentifier) {
			boolean alreadyTreated = treatedRelations.contains(relationIdentifier);
			if (!alreadyTreated) {
				treatedRelations.add(relationIdentifier);
			}
			return !alreadyTreated;
		}
		
		/**
		 * Expected to retrieve an entity by its class and identifier from cache or instantiates it and put it into the cache
		 *
		 * @param clazz the type of the entity
		 * @param identifier the identifier of the entity (Long, String, ...)
		 * @param fallbackFactory the "method" that will be called to create the entity when the entity is not in the cache
		 * @return the existing instance in the cache or a new object
		 */
		public <E> E giveEntityFromCache(Class<E> clazz, Object identifier, Supplier<E> fallbackFactory) {
			return this.entityCache.computeIfAbsent(clazz, identifier, fallbackFactory);
		}
	}
	
	/**
	 * A {@link LinkedMap} basing its hash on {@link System#identityHashCode(Object)} one, and comparing key on their reference.
	 * Made to have a {@link Map} that hashes on identity while keeping insertion order.
	 *
	 * @param <K>
	 * @param <V>
	 * @author Guillaume Mary
	 */
	public static class IdentityLinkedMap<K, V> extends LinkedMap<K, V> {
		
		public IdentityLinkedMap(int initialCapacity) {
			super(initialCapacity);
		}
		
		@Override
		protected int hash(Object key) {
			return System.identityHashCode(key);
		}
		
		@Override
		protected boolean isEqualKey(Object key1, Object key2) {
			return key1 == key2;
		}
	}
}
