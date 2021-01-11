package org.gama.stalactite.persistence.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.gama.stalactite.persistence.engine.runtime.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IRowTransformer;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * Join node for filling a relation between beans such as one-to-one or one-to-many
 * 
 * @author Guillaume Mary
 */
public class RelationJoinNode<C, T1 extends Table, T2 extends Table, I> extends AbstractJoinNode<C, T1, T2, I> {
	
	/** The right part of the join */
	private final EntityInflater<C, ?, T2> entityInflater;
	
	/** Relation fixer for instances of this strategy on owning strategy entities */
	private final BeanRelationFixer<Object, C> beanRelationFixer;
	
	RelationJoinNode(JoinNode<T1> parent,
							Column<T1, I> leftJoinColumn,
							Column<T2, I> rightJoinColumn,
							JoinType joinType,
							Set<Column<T2, Object>> columnsToSelect,
							@Nullable String tableAlias,
							EntityInflater<C, ?, T2> entityInflater,
							BeanRelationFixer<Object, C> beanRelationFixer) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias);
		this.entityInflater = entityInflater;
		this.beanRelationFixer = beanRelationFixer;
	}
	
	public EntityInflater<C, ?, T2> getEntityInflater() {
		return entityInflater;
	}
	
	BeanRelationFixer<Object, C> getBeanRelationFixer() {
		return beanRelationFixer;
	}
	
	@Override
	RelationJoinRowConsumer<C, ?> toConsumer(ColumnedRow columnedRow) {
		return new RelationJoinRowConsumer<>(entityInflater, beanRelationFixer, columnedRow);
	}
	
	static class RelationJoinRowConsumer<C, I> implements JoinRowConsumer {
		
		/** The right part of the join */
		private final EntityInflater<C, I, ?> entityInflater;
		
		/** Relation fixer for instances of this strategy on owning strategy entities */
		private final BeanRelationFixer<Object, C> beanRelationFixer;
		
		private final IRowTransformer<C> rowTransformer;
		
		private final ColumnedRow columnedRow;
		
		RelationJoinRowConsumer(EntityInflater<C, I, ?> entityInflater, BeanRelationFixer<Object, C> beanRelationFixer, ColumnedRow columnedRow) {
			this.entityInflater = entityInflater;
			this.beanRelationFixer = beanRelationFixer;
			this.columnedRow = columnedRow;
			this.rowTransformer = entityInflater.copyTransformerWithAliases(columnedRow);
		}
		
		C applyRelatedEntity(Object parentJoinEntity,
										   Row row,
										   EntityCache entityCache) {
			I rightIdentifier = entityInflater.giveIdentifier(row, columnedRow);
			// primary key null means no entity => nothing to do
			if (rightIdentifier != null) {
				C rightEntity = entityCache.computeIfAbsent(entityInflater.getEntityType(), rightIdentifier,
						() -> rowTransformer.transform(row));
				beanRelationFixer.apply(parentJoinEntity, rightEntity);
				return rightEntity;
			}
			return null;
		}
	}
	
	@FunctionalInterface
	public interface EntityCache {
		
		/**
		 * Expected to retrieve an entity by its class and identifier from cache or instanciates it and put it into the cache
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
	@VisibleForTesting
	static final class BasicEntityCache implements EntityCache {
		
		private final Map<Class, Map<Object, Object>> entityCache = new HashMap<>();
		
		public <C> C computeIfAbsent(Class<C> clazz, Object identifier, Supplier<C> factory) {
			Map<Object, Object> classInstanceCacheByIdentifier = entityCache.computeIfAbsent(clazz, k -> new HashMap<>());
			return (C) classInstanceCacheByIdentifier.computeIfAbsent(identifier, k -> factory.get());
		}
	}
}
