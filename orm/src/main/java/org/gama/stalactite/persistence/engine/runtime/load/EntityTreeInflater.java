package org.gama.stalactite.persistence.engine.runtime.load;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.ThreadLocals;
import org.gama.lang.VisibleForTesting;
import org.gama.lang.collection.Collections;
import org.gama.stalactite.persistence.engine.MappingConfigurationException;
import org.gama.stalactite.persistence.engine.runtime.load.JoinRoot.JoinRootRowConsumer;
import org.gama.stalactite.persistence.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.gama.stalactite.persistence.engine.runtime.load.PassiveJoinNode.PassiveJoinRowConsumer;
import org.gama.stalactite.persistence.engine.runtime.load.RelationJoinNode.BasicEntityCache;
import org.gama.stalactite.persistence.engine.runtime.load.RelationJoinNode.EntityCache;
import org.gama.stalactite.persistence.engine.runtime.load.RelationJoinNode.RelationJoinRowConsumer;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.IdentityMap;
import org.gama.stalactite.sql.result.Row;

/**
 * Bean graph creator from database rows. Based on a tree of {@link ConsumerNode}s which wraps some {@link JoinRowConsumer}.
 * 
 * @param <C> main entity type
 * @author Guillaume Mary
 */
public class EntityTreeInflater<C> {
	
	private static final ThreadLocal<EntityTreeInflater<?>.TreeInflationContext> CURRENT_CONTEXT = new ThreadLocal<>();
	
	/**
	 * Gives current {@link TreeInflationContext}. A current one is available during {@link #transform(Iterable, int)} invokation.
	 * 
	 * @return current {@link TreeInflationContext}, null if you're not invoking it during its lifecycle
	 */
	public static EntityTreeInflater<?>.TreeInflationContext currentContext() {
		return CURRENT_CONTEXT.get();
	}
	
	/**
	 * All inflaters, mergers and so on, in a tree structure that reflects {@link EntityJoinTree}.
	 * Made as such to benefit from the possbility to cancel in-depth iteration in {@link #transform(Row, TreeInflationContext)} method during relation
	 * building
	 */
	private final ConsumerNode consumerRoot;
	
	/**
	 * Query row decoder, overall used to be passed to current {@link TreeInflationContext}
 	 */
	private final EntityTreeQueryRowDecoder rowDecoder;
	
	/**
	 * Constructor with necessary elements
	 * @param consumerRoot top level row consumer, the one that will computes root instances
	 * @param columnAliases query column aliases
	 * @param tablePerJoinNodeName mapping between join nodes and their names
	 */
	EntityTreeInflater(ConsumerNode consumerRoot, IdentityMap<Column, String> columnAliases, Map<String, Table> tablePerJoinNodeName) {
		this.consumerRoot = consumerRoot;
		this.rowDecoder = new EntityTreeQueryRowDecoder(columnAliases, tablePerJoinNodeName);
	}
	
	/**
	 *
	 * @param rows rows (coming from database select) to be read to build beans graph
	 * @param resultSize expected reuslt size, only for resulting list optimization
	 * @return a list of root beans, built from given rows by asking internal strategy joins to instanciate and complete them
	 */
	public List<C> transform(Iterable<Row> rows, int resultSize) {
		return ThreadLocals.doWithThreadLocal(CURRENT_CONTEXT, () -> this.new TreeInflationContext(), (Function<EntityTreeInflater<?>.TreeInflationContext, List<C>>) context ->
						new ArrayList<>(transform(rows, resultSize, context)));
	}
	
	private Set<C> transform(Iterable<Row> rows, int resultSize, EntityTreeInflater<?>.TreeInflationContext context) {
		// we use an "IdentitySet" (doesn't exist directly, but can be done through IdentityHashMap) to avoid duplicate entity : with a HashSet
		// duplicate can happen if equals/hashCode depends on relation, in particular Collection ones, because they are filled from row to row
		// making hashCode value change
		Set<C> result = Collections.newIdentitySet(resultSize);
		for (Row row : rows) {
			Nullable<C> newInstance = transform(row, context);
			newInstance.invoke(result::add);
		}
		return result;
	}
	
	Nullable<C> transform(Row row, EntityTreeInflater<?>.TreeInflationContext context) {
		context.setCurrentRow(row);
		// Algorithm : we iterate depth by depth the tree structure of the joins
		// We start by the root of the hierarchy.
		// We process the entity of the current depth, then process the direct relations, add those relations to the depth iterator
		Nullable<C> result = Nullable.nullable(((JoinRootRowConsumer<C, ?>) this.consumerRoot.consumer).createRootInstance(row, context));
		
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
						return ((RelationJoinRowConsumer) join).applyRelatedEntity(entity, row, context);
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
		Queue<ConsumerNode> joinNodeStack = Collections.newLifoQueue();
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
	
	/**
	 * Represents a relation between an entity and another one through its join node 
	 */
	static class RelationIdentifier {
		
		private final Object rootEntity;
		private final Class relatedEntityType;
		private final Object relatedBeanIdentifier;
		private final RelationJoinRowConsumer joinNode;
		
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
	 * Container for informations used during {@link Row} transformation as a bean graph.
	 * Accessible from {@link EntityTreeInflater#currentContext()} during its lifecycle :
	 * - instanciated at beginning of {@link Row} transformation ({@link EntityTreeInflater#transform(Iterable, int)}
	 * - dropped at the end of the method
	 * 
	 * @implNote made non-static to ease access to the surrouding {@link EntityTreeInflater} instance
	 */
	public class TreeInflationContext {
		
		/** Entity cache */
		private final EntityCache entityCache;
		
		/** Storage for treated relations */
		private final Set<RelationIdentifier> treatedRelations = new HashSet<>();
		
		private Row currentRow;
		
		@VisibleForTesting
		TreeInflationContext() {
			this(new BasicEntityCache());
		}
		
		public TreeInflationContext(EntityCache entityCache) {
			this.entityCache = entityCache;
		}
		
		private TreeInflationContext setCurrentRow(Row currentRow) {
			this.currentRow = currentRow;
			return this;
		}
		
		
		@javax.annotation.Nullable
		public <T extends Table<T>, O> O giveValue(String joinNodeName, Column<T, O> column) {
			return getRowDecoder().giveValue(joinNodeName, column, currentRow);
		}
		
		/**
		 * Returns the row decoder of the executed query 
		 * @return never null
		 */
		public EntityTreeQueryRowDecoder getRowDecoder() {
			return EntityTreeInflater.this.rowDecoder;
		}
		
		public boolean isTreatedOrAppend(RelationIdentifier relationIdentifier) {
			boolean alreadyTreated = treatedRelations.contains(relationIdentifier);
			if (!alreadyTreated) {
				treatedRelations.add(relationIdentifier);
			}
			return !alreadyTreated;
		}
		
		/**
		 * Expected to retrieve an entity by its class and identifier from cache or instanciates it and put it into the cache
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
	 * Gives access to a {@link Row} value by a {@link Column} and the join node identifier that "owns" the column.
	 * Made for nodes that need to read data from row but don't own the column table under which the data are, in particular indexing column of a
	 * {@link java.util.Collection}.
	 * 
	 * See {@link EntityTreeQueryBuilder} for table clone mecanism.
	 * 
	 * @see #giveValue(String, Column, Row)
	 */
	public static class EntityTreeQueryRowDecoder {
		
		private final IdentityMap<Column, String> columnAliases;
		
		private final Map<String, Table> tablePerJoinNodeName;
		
		EntityTreeQueryRowDecoder(IdentityMap<Column, String> columnAliases,
								  Map<String, Table> tablePerJoinNodeName) {
			this.columnAliases = columnAliases;
			this.tablePerJoinNodeName = tablePerJoinNodeName;
		}
		
		/**
		 * Gives the value of given {@link Column} in given {@link Row}.
		 * Join node name is the one for node that owns the column
		 * 
		 * @param joinNodeName relation node identifier 
		 * @param column a column of the relation node
		 * @param row data read from database after query execution
		 * @param <T> column table type
		 * @param <O> column data type
		 * @return null if data is null
		 */
		@javax.annotation.Nullable
		public <T extends Table<T>, O> O giveValue(String joinNodeName, Column<T, O> column, Row row) {
			Table table = tablePerJoinNodeName.get(joinNodeName);
			if (table == null) {
				// This is more for debugging purpose than for a real production goal, may be removed later
				throw new MappingConfigurationException("Can't find node named " + joinNodeName + " in joins : " + this.tablePerJoinNodeName);
			}
			Column<T, O> columnClone = table.getColumn(column.getName());
			return (O) row.get(columnAliases.get(columnClone));
		}
	}
}
