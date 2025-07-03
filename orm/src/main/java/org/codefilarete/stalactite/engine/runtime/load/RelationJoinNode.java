package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.RelationIdentifier;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.bean.Objects;

/**
 * Join node for filling a relation between beans such as one-to-one or one-to-many
 * 
 * @author Guillaume Mary
 */
public class RelationJoinNode<C, T1 extends Fromable, T2 extends Fromable, JOINTYPE, I> extends AbstractJoinNode<C, T1, T2, JOINTYPE> {
	
	/** The right part of the join */
	private final EntityInflater<C, I> entityInflater;
	
	/** Relation fixer for instances of this strategy on owning strategy entities */
	private final BeanRelationFixer<Object, C> beanRelationFixer;
	
	/** Available only in List cases : gives the identifier of an entity in the List to avoid duplicate mix (typically : concatenates list index to entity id)*/
	private final Function<ColumnedRow, ?> relationIdentifierProvider;
	
	RelationJoinNode(JoinNode<T1> parent,
					 JoinLink<T1, JOINTYPE> leftJoinColumn,
					 JoinLink<T2, JOINTYPE> rightJoinColumn,
					 JoinType joinType,
					 Set<? extends Selectable<?>> columnsToSelect,	// Of T2
					 @Nullable String tableAlias,
					 EntityInflater<C, I> entityInflater,
					 BeanRelationFixer<?, C> beanRelationFixer,
					 @Nullable Function<ColumnedRow, ?> relationIdentifierProvider) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias);
		this.entityInflater = entityInflater;
		this.beanRelationFixer = (BeanRelationFixer<Object, C>) beanRelationFixer;
		this.relationIdentifierProvider = relationIdentifierProvider;
	}
	
	RelationJoinNode(JoinNode<T1> parent,
					 Key<T1, JOINTYPE> leftJoinColumn,
					 Key<T2, JOINTYPE> rightJoinColumn,
					 JoinType joinType,
					 Set<? extends Selectable<?>> columnsToSelect,	// Of T2
					 @Nullable String tableAlias,
					 EntityInflater<C, I> entityInflater,
					 BeanRelationFixer<?, C> beanRelationFixer,
					 @Nullable Function<ColumnedRow, ?> relationIdentifierProvider) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias);
		this.entityInflater = entityInflater;
		this.beanRelationFixer = (BeanRelationFixer<Object, C>) beanRelationFixer;
		this.relationIdentifierProvider = relationIdentifierProvider;
	}
	
	public EntityInflater<C, ?> getEntityInflater() {
		return entityInflater;
	}
	
	public Class<C> getEntityType() {
		return entityInflater.getEntityType();
	}
	
	BeanRelationFixer<Object, C> getBeanRelationFixer() {
		return beanRelationFixer;
	}
	
	public Function<ColumnedRow, ?> getRelationIdentifierProvider() {
		return relationIdentifierProvider;
	}
	
	@Override
	public RelationJoinRowConsumer<C, I> toConsumer(JoinNode<T2> joinNode) {
		return new DefaultRelationJoinRowConsumer<>(joinNode, entityInflater, beanRelationFixer, relationIdentifierProvider, getConsumptionListener());
	}
	
	interface RelationJoinRowConsumer<C, I> extends JoinRowConsumer {
		
		C applyRelatedEntity(Object parentJoinEntity, ColumnedRow row, TreeInflationContext context);
	}
	
	static class DefaultRelationJoinRowConsumer<C, I> implements RelationJoinRowConsumer<C, I> {

		private final JoinNode<?> joinNode;
		
		private final Class<C> entityType;
		
		private final Function<ColumnedRow, I> identifierProvider;
		
		/** Relation fixer for instances of this strategy on owning strategy entities */
		private final BeanRelationFixer<Object, C> beanRelationFixer;
		
		private final Function<ColumnedRow, ?> relationIdentifierComputer;
		
		private final RowTransformer<C> rowTransformer;
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final EntityTreeJoinNodeConsumptionListener<C> consumptionListener;
		
		DefaultRelationJoinRowConsumer(JoinNode<?> joinNode,
									   EntityInflater<C, I> entityInflater,
									   BeanRelationFixer<Object, C> beanRelationFixer,
									   @Nullable Function<ColumnedRow, ?> relationIdentifierComputer,
									   @Nullable EntityTreeJoinNodeConsumptionListener<C> consumptionListener) {
			this.joinNode = joinNode;
			this.entityType = entityInflater.getEntityType();
			this.identifierProvider = entityInflater::giveIdentifier;
			this.beanRelationFixer = beanRelationFixer;
			this.relationIdentifierComputer = Objects.preventNull(relationIdentifierComputer, this.identifierProvider);
			this.rowTransformer = entityInflater.getRowTransformer();
			this.consumptionListener = consumptionListener;
		}
		
		@Override
		public JoinNode<?> getNode() {
			return joinNode;
		}
		
		RowTransformer<C> getRowTransformer() {
			return rowTransformer;
		}
		
		@Override
		public C applyRelatedEntity(Object parentJoinEntity, ColumnedRow row, TreeInflationContext context) {
			I rightIdentifier = identifierProvider.apply(row);
			// we avoid treating twice same relation, overall to avoid adding twice same instance to a collection (one-to-many list cases)
			// in case of multiple collections in ResultSet because it creates similar data (through cross join) which are treated as many as
			// collections cross with each other. This also works for one-to-one relations but produces no bugs. It can also be seen as a performance
			// enhancement even if it hasn't been measured.
			RelationIdentifier eventuallyApplied = new RelationIdentifier(parentJoinEntity, this.entityType, relationIdentifierComputer.apply(row), this);
			// primary key null means no entity => nothing to do
			if (rightIdentifier != null) {
				C rightEntity = context.giveEntityFromCache(entityType, rightIdentifier, () -> rowTransformer.transform(row));
				if (context.isTreatedOrAppend(eventuallyApplied)) {
					beanRelationFixer.apply(parentJoinEntity, rightEntity);
					if (this.consumptionListener != null) {
						this.consumptionListener.onNodeConsumption(rightEntity, row);
					}
				}
				// we return the entity found for the row to let caller go deeper in the hierarchy
				return rightEntity;
			}
			// null is a marker for caller to not go deeper in the hierarchy : no entity was found on row, we can't go deeper
			return null;
		}
		
		/**
		 * Implemented for debug. DO NOT RELY ON IT for anything else.
		 */
		@Override
		public String toString() {
			return Reflections.toString(this.getClass());
		}
	}
	
	@FunctionalInterface
	public interface EntityCache {
		
		/**
		 * Expected to retrieve an entity by its class and identifier from cache or instantiates it and put it into the cache
		 *
		 * @param clazz the type of the entity
		 * @param identifier the identifier of the entity (Long, String, ...)
		 * @param factory the "method" that will be called to create the entity when the entity is not in the cache
		 * @return the existing instance in the cache or a new object
		 */
		<C> C computeIfAbsent(Class<C> clazz, Object identifier, Supplier<C> factory);
		
	}
	
	/**
	 * Simple class to ease access or creation to entity from the cache
	 * @see #computeIfAbsent(Class, Object, Supplier)
	 */
	static final class BasicEntityCache implements EntityCache {
		
		private final Map<Class, Map<Object, Object>> entityCache = new HashMap<>();
		
		public <C> C computeIfAbsent(Class<C> clazz, Object identifier, Supplier<C> factory) {
			Map<Object, Object> classInstanceCacheByIdentifier = entityCache.computeIfAbsent(clazz, k -> new HashMap<>());
			return (C) classInstanceCacheByIdentifier.computeIfAbsent(identifier, k -> factory.get());
		}
	}
}
