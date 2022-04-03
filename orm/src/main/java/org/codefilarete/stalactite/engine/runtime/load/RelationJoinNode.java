package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.RelationIdentifier;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * Join node for filling a relation between beans such as one-to-one or one-to-many
 * 
 * @author Guillaume Mary
 */
public class RelationJoinNode<C, T1 extends Table, T2 extends Table, I> extends AbstractJoinNode<C, T1, T2, I> {
	
	/** The right part of the join */
	private final EntityInflater<C, I, T2> entityInflater;
	
	/** Relation fixer for instances of this strategy on owning strategy entities */
	private final BeanRelationFixer<Object, C> beanRelationFixer;
	
	private final BiFunction<Row, ColumnedRow, Object> duplicateIdentifierProvider;
	
	RelationJoinNode(JoinNode<T1> parent,
					 Column<T1, I> leftJoinColumn,
					 Column<T2, I> rightJoinColumn,
					 JoinType joinType,
					 Set<Column<T2, ?>> columnsToSelect,
					 @Nullable String tableAlias,
					 EntityInflater<C, I, T2> entityInflater,
					 BeanRelationFixer<Object, C> beanRelationFixer,
					 @Nullable BiFunction<Row, ColumnedRow, ?> duplicateIdentifierProvider) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias);
		this.entityInflater = entityInflater;
		this.beanRelationFixer = beanRelationFixer;
		this.duplicateIdentifierProvider = (BiFunction<Row, ColumnedRow, Object>) duplicateIdentifierProvider;
	}
	
	public EntityInflater<C, ?, T2> getEntityInflater() {
		return entityInflater;
	}
	
	BeanRelationFixer<Object, C> getBeanRelationFixer() {
		return beanRelationFixer;
	}
	
	public BiFunction<Row, ColumnedRow, Object> getDuplicateIdentifierProvider() {
		return duplicateIdentifierProvider;
	}
	
	@Override
	public RelationJoinRowConsumer<C, I> toConsumer(ColumnedRow columnedRow) {
		return new RelationJoinRowConsumer<>(entityInflater, beanRelationFixer, columnedRow, duplicateIdentifierProvider, getTransformerListener());
	}
	
	static class RelationJoinRowConsumer<C, I> implements JoinRowConsumer {
		
		private final Class<C> entityType;
		
		private final BiFunction<Row, ColumnedRow, I> identifierProvider;
		
		/** Relation fixer for instances of this strategy on owning strategy entities */
		private final BeanRelationFixer<Object, C> beanRelationFixer;
		
		private final ColumnedRow columnedRow;
		
		private final BiFunction<Row, ColumnedRow, Object> relationIdentifierComputer;
		
		private final RowTransformer<C> rowTransformer;
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final TransformerListener<C> transformerListener;
		
		RelationJoinRowConsumer(EntityInflater<C, I, ?> entityInflater,
								BeanRelationFixer<Object, C> beanRelationFixer,
								ColumnedRow columnedRow,
								@Nullable BiFunction<Row, ColumnedRow, Object> relationIdentifierComputer,
								@Nullable TransformerListener<C> transformerListener) {
			this.entityType = entityInflater.getEntityType();
			this.identifierProvider = entityInflater::giveIdentifier;
			this.beanRelationFixer = beanRelationFixer;
			this.columnedRow = columnedRow;
			this.relationIdentifierComputer = (BiFunction<Row, ColumnedRow, Object>) Objects.preventNull(relationIdentifierComputer, this.identifierProvider);
			this.rowTransformer = entityInflater.copyTransformerWithAliases(columnedRow);
			this.transformerListener = transformerListener;
		}
		
		C applyRelatedEntity(Object parentJoinEntity, Row row, TreeInflationContext context) {
			I rightIdentifier = identifierProvider.apply(row, columnedRow);
			// we avoid treating twice same relation, overall to avoid adding twice same instance to a collection (one-to-many list cases)
			// in case of multiple collections in ResultSet because it creates similar data (through cross join) which are treated as many as
			// collections cross with each other. This also works for one-to-one relations but produces no bugs. It can also be seen as a performance
			// enhancement even if it hasn't been measured.
			RelationIdentifier eventuallyApplied = new RelationIdentifier(parentJoinEntity, this.entityType, relationIdentifierComputer.apply(row, columnedRow), this);
			// primary key null means no entity => nothing to do
			if (rightIdentifier != null && context.isTreatedOrAppend(eventuallyApplied)) {
				C rightEntity = (C) context.giveEntityFromCache(entityType, rightIdentifier, () -> rowTransformer.transform(row));
				beanRelationFixer.apply(parentJoinEntity, rightEntity);
				if (this.transformerListener != null) {
					this.transformerListener.onTransform(rightEntity, column -> columnedRow.getValue(column, row));
				}
				return rightEntity;
			}
			return null;
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
