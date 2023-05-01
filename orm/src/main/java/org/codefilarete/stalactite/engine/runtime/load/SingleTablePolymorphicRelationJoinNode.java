package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.RelationIdentifier;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.ForkJoinRowConsumer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.collection.Iterables;

/**
 * Particular {@link JoinNode} made to handle relation from an entity to a collection of some another polymorphic one. Actually relation doesn't
 * make the complexity of that class: polymorphic entity instantiation is the core focus of it. Here are the hot spots:
 * identifier is given by the subclass which find its id in the row (see {@link SingleTablePolymorphicRelationJoinRowConsumer#giveIdentifier}),
 * and while doing it, it remembers which consumer made it. Then while instantiating entity we invoke it to get right entity type (parent mapping
 * would only give parent entity, which, out of being a wrong approach, can be an abstract type). Then instance is filled up with parent properties
 * by calling merging method (see line 98).
 * Finally, {@link SingleTablePolymorphicRelationJoinRowConsumer} must extend {@link ForkJoinRowConsumer} to give next branch to be consumed by
 * {@link EntityTreeInflater} to avoid "dead" branch to be read : we give it according to the consumer which found the identifier. 
 * 
 * @author Guillaume Mary
 */
public class SingleTablePolymorphicRelationJoinNode<C, T1 extends Table<T1>, T2 extends Table<T2>, JOINCOLTYPE, I, DTYPE> extends RelationJoinNode<C, T1, T2, JOINCOLTYPE, I> {
	
	private final Set<SubPersisterAndConsumer<C, ? extends C>> subPersisters = new HashSet<>();
	
	private final Column<T2, DTYPE> discriminatorColumn;
	
	public SingleTablePolymorphicRelationJoinNode(JoinNode<T1> parent,
												  Key<T1, JOINCOLTYPE> leftJoinColumn,
												  Key<T2, JOINCOLTYPE> rightJoinColumn,
												  JoinType joinType,
												  Set<? extends Selectable<?>> columnsToSelect,
												  @Nullable String tableAlias,
												  EntityInflater<C, I> entityInflater,
												  BeanRelationFixer<Object, C> beanRelationFixer,
												  Column<T2, DTYPE> discriminatorColumn) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias, entityInflater, beanRelationFixer, null);
		this.discriminatorColumn = discriminatorColumn;
	}
	
	@Override
	public SingleTablePolymorphicRelationJoinRowConsumer toConsumer(ColumnedRow columnedRow) {
		return new SingleTablePolymorphicRelationJoinRowConsumer(columnedRow, discriminatorColumn, getConsumptionListener());
	}
	
	public <D extends C> void addSubPersisterJoin(ConfiguredRelationalPersister<D, I> subPersister, PolymorphicMergeJoinRowConsumer<C, D, I> subPersisterJoin, DTYPE discriminatorValue) {
		this.subPersisters.add(new SubPersisterAndConsumer<>(subPersister, subPersisterJoin, discriminatorValue));
	}
	
	private class SubPersisterAndConsumer<C, D extends C> {
		
		private final ConfiguredRelationalPersister<D, I> subPersister;
		private final PolymorphicMergeJoinRowConsumer<C, D, I> subPersisterJoin;
		private final DTYPE discriminatorValue;
		
		public SubPersisterAndConsumer(ConfiguredRelationalPersister<D, I> subPersister, PolymorphicMergeJoinRowConsumer<C, D, I> subPersisterJoin, DTYPE discriminatorValue) {
			this.subPersister = subPersister;
			this.subPersisterJoin = subPersisterJoin;
			this.discriminatorValue = discriminatorValue;
		}
	}
	
	protected class SingleTablePolymorphicRelationJoinRowConsumer implements RelationJoinRowConsumer<C, I> {
		
		private final ColumnedRow columnedRow;
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final EntityTreeJoinNodeConsumptionListener<C> consumptionListener;
		private final Column<T2, DTYPE> discriminatorColumn;
		
		private SingleTablePolymorphicRelationJoinRowConsumer(ColumnedRow columnedRow,
															  Column<T2, DTYPE> discriminatorColumn,
															  @Nullable EntityTreeJoinNodeConsumptionListener<C> consumptionListener) {
			this.columnedRow = columnedRow;
			this.consumptionListener = consumptionListener;
			this.discriminatorColumn = discriminatorColumn;
		}
		
		@Override
		public C applyRelatedEntity(Object parentJoinEntity, Row row, TreeInflationContext context) {
			RowIdentifier<C> rowIdentifier = giveIdentifier(row);
			if (rowIdentifier == null) {
				return null;
			} else {
				I rightIdentifier = rowIdentifier.entityIdentifier;
				// we avoid treating twice same relation, overall to avoid adding twice same instance to a collection (one-to-many list cases)
				// in case of multiple collections in ResultSet because it creates similar data (through cross join) which are treated as many as
				// collections cross with each other. This also works for one-to-one relations but produces no bugs. It can also be seen as a performance
				// enhancement even if it hasn't been measured.
				RelationIdentifier eventuallyApplied = new RelationIdentifier(
						parentJoinEntity,
						rowIdentifier.entityType,
						rowIdentifier.entityIdentifier,
						this);
				// primary key null means no entity => nothing to do
				if (rightIdentifier != null && context.isTreatedOrAppend(eventuallyApplied)) {
					C rightEntity = (C) context.giveEntityFromCache(rowIdentifier.entityType, rightIdentifier, () -> rowIdentifier.rowConsumer.transform(row));
					getBeanRelationFixer().apply(parentJoinEntity, rightEntity);
					if (this.consumptionListener != null) {
						this.consumptionListener.onNodeConsumption(rightEntity, col -> columnedRow.getValue(col, row));
					}
					return rightEntity;
				} else {
					return null;
				}
			}
		}
		
		@Nullable
		/* Optimized, from 530 000 nanos to 65 000 nanos at 1st exec, from 40 000 nanos to 12 000 nanos on usual run */
		public <D extends C> RowIdentifier<D> giveIdentifier(Row row) {
			// @Optimized : use for & return instead of stream().map().filter(notNull).findFirst()
			Object discriminatorValue = columnedRow.getValue(discriminatorColumn, row);
			if (discriminatorValue != null) {
				SubPersisterAndConsumer<C, D> discriminatorConsumer = (SubPersisterAndConsumer) Iterables.find(subPersisters, o -> o.discriminatorValue.equals(discriminatorValue));
				if (discriminatorConsumer != null) {
					I assemble = discriminatorConsumer.subPersisterJoin.giveIdentifier(row);
					if (assemble != null) {
						return new RowIdentifier<>(assemble, discriminatorConsumer.subPersisterJoin, discriminatorConsumer.subPersister.getClassToPersist());
					}
				}
			}
			return null;
		}
		
		private class RowIdentifier<D extends C> {
			
			private final I entityIdentifier;
			private final PolymorphicMergeJoinRowConsumer<C, D, I> rowConsumer;
			private final Class<D> entityType;
			
			private RowIdentifier(I entityIdentifier, PolymorphicMergeJoinRowConsumer<C, D, I> rowConsumer, Class<D> entityType) {
				this.entityIdentifier = entityIdentifier;
				this.rowConsumer = rowConsumer;
				this.entityType = entityType;
			}
		}
	}
}
