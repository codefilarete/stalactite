package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.RelationIdentifier;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.ForkJoinRowConsumer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.query.model.Union.UnionInFrom;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.collection.Iterables;

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
public class TablePerClassPolymorphicRelationJoinNode<C, T1 extends Table, JOINCOLTYPE, I> extends RelationJoinNode<C, T1, UnionInFrom, JOINCOLTYPE, I> {
	
	private final Set<SubPersisterAndConsumer<C, ? extends C>> subPersisters = new HashSet<>();
	
	private final UnionInFrom unionInFrom;
	private final Union.PseudoColumn<Integer> discriminatorColumn;
	
	TablePerClassPolymorphicRelationJoinNode(JoinNode<T1> parent,
											 Union subPersistersUnion,
											 JoinLink<T1, JOINCOLTYPE> leftJoinColumn,
											 JoinLink<?, JOINCOLTYPE> rightJoinColumn,
											 JoinType joinType,
											 Set<? extends Selectable<?>> columnsToSelect,
											 @Nullable String tableAlias,
											 EntityInflater<C, I> entityInflater,
											 BeanRelationFixer<Object, C> beanRelationFixer,
											 Union.PseudoColumn<Integer> discriminatorColumn) {
		super(parent, leftJoinColumn, subPersistersUnion.findColumn(rightJoinColumn.getExpression()), joinType, columnsToSelect, tableAlias, entityInflater, beanRelationFixer, null);
		this.unionInFrom = subPersistersUnion.asPseudoTable(getTableAlias());
		this.discriminatorColumn = discriminatorColumn;
	}
	
	@Override
	public UnionInFrom getRightTable() {
		return unionInFrom;
	}
	
	/**
	 * Overridden, else it returns the ones given at construction time which are from Union, meaning getOwner() is not
	 * {@link UnionInFrom} which, out of throwing a {@link ClassCastException}, avoid to give access to correct
	 * {@link Column}
	 */
	@Override
	public Set<Selectable<?>> getColumnsToSelect() {
		return (Set) unionInFrom.getColumns();
	}
	
	@Override
	public JoinTablePolymorphicRelationJoinRowConsumer toConsumer(ColumnedRow columnedRow) {
		return new JoinTablePolymorphicRelationJoinRowConsumer(columnedRow, discriminatorColumn, getTransformerListener());
	}
	
	public <D extends C> void addSubPersisterJoin(EntityConfiguredJoinedTablesPersister<D, I> subPersister, PolymorphicMergeJoinRowConsumer<C, D, I> subPersisterJoin, int discriminatorValue) {
		this.subPersisters.add(new SubPersisterAndConsumer<>(subPersister, subPersisterJoin, discriminatorValue));
	}
	
	private class SubPersisterAndConsumer<C, D extends C> {
		
		private final EntityConfiguredJoinedTablesPersister<D, I> subPersister;
		private final EntityJoinTree.PolymorphicMergeJoinRowConsumer<C, D, I> subPersisterJoin;
		private final int discriminatorValue;
		
		public SubPersisterAndConsumer(EntityConfiguredJoinedTablesPersister<D, I> subPersister, PolymorphicMergeJoinRowConsumer<C, D, I> subPersisterJoin, int discriminatorValue) {
			this.subPersister = subPersister;
			this.subPersisterJoin = subPersisterJoin;
			this.discriminatorValue = discriminatorValue;
		}
	}
	
	private class JoinTablePolymorphicRelationJoinRowConsumer implements RelationJoinRowConsumer<C, I>, ForkJoinRowConsumer {
		
		private final ThreadLocal<RowIdentifier> currentlyFoundConsumer = new ThreadLocal<>();
		
		private final ColumnedRow columnedRow;
		
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final TransformerListener<C> transformerListener;
		private final Union.PseudoColumn<Integer> discriminatorColumn;
		
		private JoinTablePolymorphicRelationJoinRowConsumer(ColumnedRow columnedRow,
															Union.PseudoColumn<Integer> discriminatorColumn,
															@Nullable TransformerListener<C> transformerListener) {
			this.columnedRow = columnedRow;
			this.transformerListener = transformerListener;
			this.discriminatorColumn = discriminatorColumn;
		}
		
		@Override
		public C applyRelatedEntity(Object parentJoinEntity, Row row, TreeInflationContext context) {
			RowIdentifier<C> rowIdentifier = giveIdentifier(row);
			currentlyFoundConsumer.set(rowIdentifier);
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
					if (this.transformerListener != null) {
						this.transformerListener.onTransform(rightEntity, column -> columnedRow.getValue(column, row));
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
			int discriminatorValue = columnedRow.getValue(discriminatorColumn, row);
//			int discriminatorValue = (int) row.get(discriminatorColumn);
			SubPersisterAndConsumer<C, D> discriminatorConsumer = (SubPersisterAndConsumer) Iterables.find(subPersisters, o -> o.discriminatorValue == discriminatorValue);
			if (discriminatorConsumer != null) {
				I assemble = discriminatorConsumer.subPersisterJoin.giveIdentifier(row);
				if (assemble != null) {
					return new RowIdentifier<>(assemble, discriminatorConsumer.subPersisterJoin, discriminatorConsumer.subPersister.getClassToPersist());
				}
			}
			return null;
		}
		
		@Override
		public JoinRowConsumer giveNextConsumer() {
			return currentlyFoundConsumer.get().rowConsumer;
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
