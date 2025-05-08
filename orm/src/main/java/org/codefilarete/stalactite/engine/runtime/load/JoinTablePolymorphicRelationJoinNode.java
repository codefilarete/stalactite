package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.RelationIdentifier;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.ForkJoinRowConsumer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Particular {@link JoinNode} made to handle relation from an entity to a collection of some another polymorphic one. Actually relation doesn't
 * make the complexity of that class: polymorphic entity instantiation is the core focus of it. Here are the hot spots:
 * identifier is given by the subclass which find its id in the row (see {@link JoinTablePolymorphicRelationJoinRowConsumer#giveIdentifier}),
 * and while doing it, it remembers which consumer made it. Then while instantiating entity we invoke it to get right entity type (parent mapping
 * would only give parent entity, which, out of being a wrong approach, can be an abstract type). Then instance is filled up with parent properties
 * by calling merging method (see line 98).
 * Finally, {@link JoinTablePolymorphicRelationJoinRowConsumer} must extend {@link ForkJoinRowConsumer} to give next branch to be consumed by
 * {@link EntityTreeInflater} to avoid "dead" branch to be read : we give it according to the consumer which found the identifier. 
 * 
 * @author Guillaume Mary
 */
public class JoinTablePolymorphicRelationJoinNode<C, T1 extends Table, T2 extends Table, JOINTYPE, I> extends RelationJoinNode<C, T1, T2, JOINTYPE, I> {
	
	private final Set<SubPersisterAndDiscriminator<? extends C>> subPersisters = new HashSet<>();
	
	public JoinTablePolymorphicRelationJoinNode(JoinNode<T1> parent,
												Key<T1, JOINTYPE> leftJoinColumn,
												Key<T2, JOINTYPE> rightJoinColumn,
												JoinType joinType,
												Set<Column<T2, ?>> columnsToSelect,
												@Nullable String tableAlias,
												EntityInflater<C, I> entityInflater,
												BeanRelationFixer<Object, C> beanRelationFixer,
												@Nullable BiFunction<Row, ColumnedRow, ?> relationIdentifierProvider) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias, entityInflater, beanRelationFixer, relationIdentifierProvider);
	}
	
	@Override
	public JoinTablePolymorphicRelationJoinRowConsumer toConsumer(ColumnedRow columnedRow) {
		DefaultRelationJoinRowConsumer<C, I> parentRowConsumer = (DefaultRelationJoinRowConsumer<C, I>) super.toConsumer(columnedRow);
		return new JoinTablePolymorphicRelationJoinRowConsumer(columnedRow, parentRowConsumer, getConsumptionListener());
	}
	
	public <D extends C> void addSubPersisterJoin(PolymorphicMergeJoinRowConsumer<D, I> subPersisterJoin) {
		this.subPersisters.add(new SubPersisterAndDiscriminator<>(subPersisterJoin));
	}
	
	private class SubPersisterAndDiscriminator<D extends C> {
		
		private final PolymorphicMergeJoinRowConsumer<D, I> subPersisterJoin;
		
		public SubPersisterAndDiscriminator(PolymorphicMergeJoinRowConsumer<D, I> subPersisterJoin) {
			this.subPersisterJoin = subPersisterJoin;
		}
	}
	
	private class JoinTablePolymorphicRelationJoinRowConsumer implements RelationJoinRowConsumer<C, I>, ForkJoinRowConsumer {
		
		private final ThreadLocal<RowIdentifier<? extends C>> currentlyFoundConsumer = new ThreadLocal<>();
		
		private final ColumnedRow columnedRow;
		
		private final DefaultRelationJoinRowConsumer<C, I> parentRowConsumer;
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final EntityTreeJoinNodeConsumptionListener<C> consumptionListener;
		
		private JoinTablePolymorphicRelationJoinRowConsumer(ColumnedRow columnedRow,
															DefaultRelationJoinRowConsumer<C, I> parentRowConsumer,
															@Nullable EntityTreeJoinNodeConsumptionListener<C> consumptionListener) {
			this.columnedRow = columnedRow;
			this.parentRowConsumer = parentRowConsumer;
			this.consumptionListener = consumptionListener;
		}
		
		@Override
		public C applyRelatedEntity(Object parentJoinEntity, Row row, TreeInflationContext context) {
			RowIdentifier<? extends C> duo = giveIdentifier(row);
			currentlyFoundConsumer.set(duo);
			if (duo != null) {
				I rightIdentifier = duo.entityIdentifier;
				// we avoid treating twice same relation, overall to avoid adding twice same instance to a collection (one-to-many list cases)
				// in case of multiple collections in ResultSet because it creates similar data (through cross join) which are treated as many as
				// collections cross with each other. This also works for one-to-one relations but produces no bugs. It can also be seen as a performance
				// enhancement even if it hasn't been measured.
				RelationIdentifier eventuallyApplied = new RelationIdentifier(parentJoinEntity, getEntityType(), rightIdentifier, this);
				// primary key null means no entity => nothing to do
				if (rightIdentifier != null && context.isTreatedOrAppend(eventuallyApplied)) {
					C rightEntity = (C) context.giveEntityFromCache(getEntityType(), rightIdentifier, () -> {
						C entity = duo.rowConsumer.transform(row);
						// We have to apply parent properties on created bean by subclass, because sub-transformer doesn't contain them
						parentRowConsumer.getRowTransformer().applyRowToBean(row, entity);
						return entity;
					});
					getBeanRelationFixer().apply(parentJoinEntity, rightEntity);
					if (this.consumptionListener != null) {
						this.consumptionListener.onNodeConsumption(rightEntity, col -> columnedRow.getValue(col, row));
					}
					return rightEntity;
				}
			}
			return null;
		}
		
		@Nullable
		/* Optimized, from 530 000 nanos to 65 000 nanos at 1st exec, from 40 000 nanos to 12 000 nanos on usual run */
		private RowIdentifier<? extends C> giveIdentifier(Row row) {
			// @Optimized : use for & return instead of stream().map().filter(notNull).findFirst()
			for (SubPersisterAndDiscriminator<?> duo : subPersisters) {
				I assemble = duo.subPersisterJoin.giveIdentifier(row);
				if (assemble != null) {
					return new RowIdentifier<>(assemble, duo.subPersisterJoin);
				}
			}
			return null;
		}
		
		@Override
		public JoinRowConsumer giveNextConsumer() {
			return nullable(currentlyFoundConsumer.get()).map(rowIdentifier -> rowIdentifier.rowConsumer).get();
		}
		
		private class RowIdentifier<D extends C> {
			
			private final I entityIdentifier;
			private final PolymorphicMergeJoinRowConsumer<D, I> rowConsumer;
			
			private RowIdentifier(I entityIdentifier, PolymorphicMergeJoinRowConsumer<D, I> rowConsumer) {
				this.entityIdentifier = entityIdentifier;
				this.rowConsumer = rowConsumer;
			}
		}
	}
}
