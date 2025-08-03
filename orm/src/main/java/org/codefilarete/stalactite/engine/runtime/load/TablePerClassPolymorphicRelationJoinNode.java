package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.RelationIdentifier;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.ForkJoinRowConsumer;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Particular {@link JoinNode} made to handle relation from an entity to a collection of some another polymorphic one. Actually relation doesn't
 * make the complexity of that class: polymorphic entity instantiation is the core focus of it. Here are the hot spots:
 * identifier is given by the subclass which find its id in the row (see {@link TablePerClassPolymorphicRelationJoinRowConsumer#giveIdentifier}),
 * and while doing it, it remembers which consumer made it. Then while instantiating entity we invoke it to get right entity type (parent mapping
 * would only give parent entity, which, out of being a wrong approach, can be an abstract type). Then instance is filled up with parent properties
 * by calling merging method (see line 98).
 * Finally, {@link TablePerClassPolymorphicRelationJoinRowConsumer} must extend {@link ForkJoinRowConsumer} to give next branch to be consumed by
 * {@link EntityTreeInflater} to avoid "dead" branch to be read : we give it according to the consumer which found the identifier. 
 * 
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphicRelationJoinNode<C, T1 extends Table<T1>, JOINCOLTYPE, I> extends RelationJoinNode<C, T1, PseudoTable, JOINCOLTYPE, I> {
	
	private final Set<SubPersisterAndDiscriminator<? extends C>> subPersisters = new HashSet<>();
	
	private final PseudoTable pseudoTable;
	private final Union.PseudoColumn<Integer> discriminatorColumn;
	
	public TablePerClassPolymorphicRelationJoinNode(JoinNode<?, T1> parent,
													Union subPersistersUnion,
													Accessor<?, ?> propertyAccessor,
													Key<T1, JOINCOLTYPE> leftJoinColumn,
													Key<?, JOINCOLTYPE> rightJoinColumn,
													JoinType joinType,
													Set<? extends Selectable<?>> columnsToSelect,
													@Nullable String tableAlias,
													EntityInflater<C, I> entityInflater,
													BeanRelationFixer<Object, C> beanRelationFixer,
													Union.PseudoColumn<Integer> discriminatorColumn) {
		super(parent, propertyAccessor, leftJoinColumn, (Key) rightJoinColumn, joinType, columnsToSelect, tableAlias, entityInflater, beanRelationFixer, null);
		this.pseudoTable = subPersistersUnion.asPseudoTable(getTableAlias());
		this.discriminatorColumn = discriminatorColumn;
	}
	
	@Override
	public PseudoTable getRightTable() {
		return pseudoTable;
	}
	
	/**
	 * Overridden, else it returns the ones given at construction time which are from Union, meaning getOwner() is not
	 * {@link PseudoTable} which, out of throwing a {@link ClassCastException}, avoid to give access to correct
	 * {@link Column}
	 */
	@Override
	public Set<Selectable<?>> getColumnsToSelect() {
		return (Set) pseudoTable.getColumns();
	}
	
	@Override
	public TablePerClassPolymorphicRelationJoinRowConsumer toConsumer(JoinNode<C, PseudoTable> joinNode) {
		return new TablePerClassPolymorphicRelationJoinRowConsumer(joinNode, discriminatorColumn, getConsumptionListener());
	}
	
	public <D extends C> void addSubPersisterJoin(PolymorphicMergeJoinRowConsumer<D, I> subPersisterJoin, int discriminatorValue) {
		this.subPersisters.add(new SubPersisterAndDiscriminator<>(subPersisterJoin, discriminatorValue));
	}
	
	private class SubPersisterAndDiscriminator<D extends C> {
		
		private final PolymorphicMergeJoinRowConsumer<D, I> subPersisterJoin;
		private final int discriminatorValue;
		
		public SubPersisterAndDiscriminator(PolymorphicMergeJoinRowConsumer<D, I> subPersisterJoin, int discriminatorValue) {
			this.subPersisterJoin = subPersisterJoin;
			this.discriminatorValue = discriminatorValue;
		}
	}
	
	public class TablePerClassPolymorphicRelationJoinRowConsumer implements RelationJoinRowConsumer<C, I>, ForkJoinRowConsumer {
		
		private final ThreadLocal<RowIdentifier<? extends C>> currentlyFoundConsumer = new ThreadLocal<>();

		private final JoinNode<C, PseudoTable> joinNode;
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final EntityTreeJoinNodeConsumptionListener<C> consumptionListener;
		private final Union.PseudoColumn<Integer> discriminatorColumn;

		private TablePerClassPolymorphicRelationJoinRowConsumer(JoinNode<C, PseudoTable> joinNode,
																Union.PseudoColumn<Integer> discriminatorColumn,
																@Nullable EntityTreeJoinNodeConsumptionListener<C> consumptionListener) {
			this.joinNode = joinNode;
			this.consumptionListener = consumptionListener;
			this.discriminatorColumn = discriminatorColumn;
		}

		@Override
		public JoinNode<C, ?> getNode() {
			return joinNode;
		}
		
		@Override
		public C applyRelatedEntity(Object parentJoinEntity, ColumnedRow row, TreeInflationContext context) {
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
						getEntityType(),
						rightIdentifier,
						this);
				// primary key null means no entity => nothing to do
				if (rightIdentifier != null && context.isTreatedOrAppend(eventuallyApplied)) {
					C rightEntity = context.giveEntityFromCache(getEntityType(), rightIdentifier, () -> rowIdentifier.rowConsumer.transform(row));
					getBeanRelationFixer().apply(parentJoinEntity, rightEntity);
					if (this.consumptionListener != null) {
						this.consumptionListener.onNodeConsumption(rightEntity, row);
					}
					return rightEntity;
				} else {
					return null;
				}
			}
		}
		
		@Nullable
		/* Optimized, from 530 000 nanos to 65 000 nanos at 1st exec, from 40 000 nanos to 12 000 nanos on usual run */
		private <D extends C> RowIdentifier<D> giveIdentifier(ColumnedRow row) {
			// @Optimized : use for & return instead of stream().map().filter(notNull).findFirst()
			Integer discriminatorValue = row.get(discriminatorColumn);
			if (discriminatorValue != null) {
				SubPersisterAndDiscriminator<D> discriminatorConsumer = (SubPersisterAndDiscriminator) Iterables.find(subPersisters, o -> o.discriminatorValue == discriminatorValue);
				if (discriminatorConsumer != null) {
					I identifier = discriminatorConsumer.subPersisterJoin.giveIdentifier(row);
					if (identifier != null) {
						return new RowIdentifier<>(identifier, discriminatorConsumer.subPersisterJoin);
					}
				}
			}
			return null;
		}
		
		@Override
		public JoinRowConsumer giveNextConsumer() {
			return nullable(currentlyFoundConsumer.get()).map(rowIdentifier -> rowIdentifier.rowConsumer).get();
		}
		
		@Override
		public void afterRowConsumption(TreeInflationContext context) {
			currentlyFoundConsumer.remove();
		}
		
		private class RowIdentifier<D extends C> {
			
			private final I entityIdentifier;
			private final PolymorphicMergeJoinRowConsumer<D, I> rowConsumer;
			
			private RowIdentifier(I entityIdentifier, PolymorphicMergeJoinRowConsumer<D, I> rowConsumer) {
				this.entityIdentifier = entityIdentifier;
				this.rowConsumer = rowConsumer;
			}
		}
		
		/**
		 * Implemented for debug. DO NOT RELY ON IT for anything else.
		 */
		@Override
		public String toString() {
			return Reflections.toString(this.getClass());
		}
	}
}
