package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.RelationIdentifier;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.ForkJoinRowConsumer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;

/**
 * Particular {@link JoinNode} made to handle relation from an entity to a collection of some another polymorphic one. Actually relation doesn't
 * make the complexity of that class: polymorphic entity instantiation is the core focus of it. Here are the hot spots:
 * identifier is given by the subclass which find its id in the row (see {@link JoinTablePolymorphicRelationJoinRowConsumer#giveIdentifier}),
 * and while doing it, it remembers which consumer made it. Then while instantiating entity we invoke it to get right entity type (parent mapping
 * would only give parent entity, which, out of being a wrong approach, can be an abstract type). Then instance is filled up with parent properties
 * by calling merging method (see line 98).
 * Finally, {@link JoinTablePolymorphicRelationJoinRowConsumer} must extend {@link ForkJoinRowConsumer} to give next branch to be consumed by
 * {@link EntityTreeInflater} to avoid "dead" branch to be read : we give according to consumer that found the identifier. 
 * 
 * @author Guillaume Mary
 */
public class PolymorphicRelationJoinNode<C, T1 extends Table, T2 extends Table, I> extends RelationJoinNode<C, T1, T2, I> {
	
	private final Set<Duo<EntityConfiguredJoinedTablesPersister<? extends C, I>, PolymorphicMergeJoinRowConsumer<C, ? extends C, I>>> subPersisters = new HashSet<>();
	
	PolymorphicRelationJoinNode(JoinNode<T1> parent,
								Column<T1, I> leftJoinColumn,
								Column<T2, I> rightJoinColumn,
								JoinType joinType,
								Set<Column<T2, ?>> columnsToSelect,
								@Nullable String tableAlias,
								EntityInflater<C, I, T2> entityInflater,
								BeanRelationFixer<Object, C> beanRelationFixer,
								@Nullable BiFunction<Row, ColumnedRow, ?> relationIdentifierProvider) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias, entityInflater, beanRelationFixer, relationIdentifierProvider);
	}
	
	@Override
	public JoinTablePolymorphicRelationJoinRowConsumer toConsumer(ColumnedRow columnedRow) {
		DefaultRelationJoinRowConsumer<C, I> parentRowConsumer = (DefaultRelationJoinRowConsumer<C, I>) super.toConsumer(columnedRow);
		return new JoinTablePolymorphicRelationJoinRowConsumer(columnedRow, parentRowConsumer, getTransformerListener());
	}
	
	public <D extends C> void addSubPersisterJoin(EntityConfiguredJoinedTablesPersister<D, I> subPersister, PolymorphicMergeJoinRowConsumer<C, D, I> subPersisterJoin) {
		this.subPersisters.add(new Duo<>(subPersister, subPersisterJoin));
	}
	
	private class JoinTablePolymorphicRelationJoinRowConsumer implements RelationJoinRowConsumer<C, I>, ForkJoinRowConsumer {
		
		private final ThreadLocal<Duo<I, ? extends PolymorphicMergeJoinRowConsumer<C, ? extends C, I>>> currentlyFoundConsumer = new ThreadLocal<>();
		
		private final ColumnedRow columnedRow;
		
		private final BiFunction<Row, ColumnedRow, Object> relationIdentifierProvider;
		
		private final DefaultRelationJoinRowConsumer<C, I> parentRowConsumer;
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final TransformerListener<C> transformerListener;
		
		private JoinTablePolymorphicRelationJoinRowConsumer(ColumnedRow columnedRow,
															DefaultRelationJoinRowConsumer<C, I> parentRowConsumer,
															@Nullable TransformerListener<C> transformerListener) {
			this.columnedRow = columnedRow;
			this.parentRowConsumer = parentRowConsumer;
			this.transformerListener = transformerListener;
			this.relationIdentifierProvider = Objects.preventNull(getRelationIdentifierProvider(), (row, columnedRow1) -> giveIdentifier(row).getLeft());
		}
		
		@Override
		public C applyRelatedEntity(Object parentJoinEntity, Row row, TreeInflationContext context) {
			Duo<I, ? extends PolymorphicMergeJoinRowConsumer<C, ? extends C, I>> duo = giveIdentifier(row);
			currentlyFoundConsumer.set(duo);
			if (duo != null) {
				I rightIdentifier = duo.getLeft();
				// we avoid treating twice same relation, overall to avoid adding twice same instance to a collection (one-to-many list cases)
				// in case of multiple collections in ResultSet because it creates similar data (through cross join) which are treated as many as
				// collections cross with each other. This also works for one-to-one relations but produces no bugs. It can also be seen as a performance
				// enhancement even if it hasn't been measured.
				RelationIdentifier eventuallyApplied = new RelationIdentifier(parentJoinEntity, getEntityType(), relationIdentifierProvider.apply(row, columnedRow), this);
				// primary key null means no entity => nothing to do
				if (rightIdentifier != null && context.isTreatedOrAppend(eventuallyApplied)) {
					C rightEntity = (C) context.giveEntityFromCache(getEntityType(), rightIdentifier, () -> {
						C entity = duo.getRight().transform(row);
						// We have to apply parent properties on created bean by subclass, because sub-transformer doesn't contain them
						parentRowConsumer.getRowTransformer().applyRowToBean(row, entity);
						return entity;
					});
					getBeanRelationFixer().apply((C) parentJoinEntity, rightEntity);
					if (this.transformerListener != null) {
						this.transformerListener.onTransform(rightEntity, column -> columnedRow.getValue(column, row));
					}
					return rightEntity;
				}
			}
			return null;
		}
		
		@Nullable
		/* Optimized, from 530 000 nanos to 65 000 nanos at 1st exec, from 40 000 nanos to 12 000 nanos on usual run */
		public Duo<I, ? extends PolymorphicMergeJoinRowConsumer<C, ? extends C, I>> giveIdentifier(Row row) {
			// @Optimized : use for & return instead of stream().map().filter(notNull).findFirst()
			for (Duo<EntityConfiguredJoinedTablesPersister<? extends C, I>, PolymorphicMergeJoinRowConsumer<C, ? extends C, I>> duo : subPersisters) {
				I assemble = duo.getRight().giveIdentifier(row);
				if (assemble != null) {
					return new Duo<>(assemble, duo.getRight());
				}
			}
			return null;
		}
		
		@Override
		public JoinRowConsumer giveNextConsumer() {
			return currentlyFoundConsumer.get().getRight();
		}
	}
}
