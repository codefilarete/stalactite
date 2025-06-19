package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.RelationIdentifier;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.ForkJoinRowConsumer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Reflections;
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
	
	private final Set<ConfiguredRelationalPersister<? extends C, I>> subPersisters;
	
	private final SingleTablePolymorphism<C, DTYPE> polymorphismPolicy;
	
	private final Column<T2, DTYPE> discriminatorColumn;
	
	public SingleTablePolymorphicRelationJoinNode(JoinNode<T1> parent,
												  Key<T1, JOINCOLTYPE> leftJoinColumn,
												  Key<T2, JOINCOLTYPE> rightJoinColumn,
												  JoinType joinType,
												  Set<? extends Selectable<?>> columnsToSelect,
												  @Nullable String tableAlias,
												  EntityInflater<C, I> entityInflater,
												  BeanRelationFixer<Object, C> beanRelationFixer,
												  Column<T2, DTYPE> discriminatorColumn,
												  Set<ConfiguredRelationalPersister<? extends C, I>> subPersisters,
												  SingleTablePolymorphism<C, DTYPE> polymorphismPolicy) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias, entityInflater, beanRelationFixer, null);
		this.discriminatorColumn = discriminatorColumn;
		this.subPersisters = subPersisters;
		this.polymorphismPolicy = polymorphismPolicy;
	}
	
	@Override
	public RelationJoinRowConsumer<C, I> toConsumer(ColumnedRow columnedRow) {
		return new SingleTablePolymorphicRelationJoinRowConsumer(columnedRow);
	}
	
	protected class SingleTablePolymorphicRelationJoinRowConsumer implements RelationJoinRowConsumer<C, I> {
		
		private final Map<DTYPE, SubEntityDeterminer<? extends C>> subEntityDeterminerPerDiscriminatorValue;
		
		private final ColumnedRow columnedRow;
		
		private SingleTablePolymorphicRelationJoinRowConsumer(ColumnedRow columnedRow) {
			this.columnedRow = columnedRow;
			this.subEntityDeterminerPerDiscriminatorValue = Iterables.map(subPersisters,
					subPersister -> polymorphismPolicy.getDiscriminatorValue(subPersister.getClassToPersist()),
					subPersister -> new SubEntityDeterminer<>(subPersister, columnedRow),
					HashMap::new);
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
						getEntityType(),
						rightIdentifier,
						this);
				// primary key null means no entity => nothing to do
				if (context.isTreatedOrAppend(eventuallyApplied)) {
					C rightEntity = (C) context.giveEntityFromCache(getEntityType(), rightIdentifier, () -> rowIdentifier.rowConsumer.createInstance(row));
					getBeanRelationFixer().apply(parentJoinEntity, rightEntity);
					if (getConsumptionListener() != null) {
						getConsumptionListener().onNodeConsumption(rightEntity, col -> columnedRow.getValue(col, row));
					}
					return rightEntity;
				} else {
					return null;
				}
			}
		}
		
		@Nullable
		private <D extends C> RowIdentifier<D> giveIdentifier(Row row) {
			DTYPE discriminatorValue = columnedRow.getValue(discriminatorColumn, row);
			if (discriminatorValue != null) {
				SubEntityDeterminer<D> discriminatorConsumer = (SubEntityDeterminer<D>) subEntityDeterminerPerDiscriminatorValue.get(discriminatorValue);
				if (discriminatorConsumer != null) {
					I identifier = discriminatorConsumer.giveIdentifier(row);
					if (identifier != null) {
						return new RowIdentifier<>(identifier, discriminatorConsumer);
					}
				}
			}
			return null;
		}
		
		/**
		 * Small class that helps to detect if a persister is concerned by a given row and creates the entity if that's the case.
		 * @param <D> persister entity type
		 */
		private class SubEntityDeterminer<D extends C> {
			
			private final Function<Row, I> identifierProvider;
			
			private final RowTransformer<D> entityInflater;
			
			public SubEntityDeterminer(ConfiguredRelationalPersister<D, I> subPersister, ColumnedRow columnedRow) {
				this.identifierProvider = row -> subPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
				this.entityInflater = subPersister.getMapping().copyTransformerWithAliases(columnedRow);
			}
			
			@Nullable
			public I giveIdentifier(Row row) {
				return identifierProvider.apply(row);
			}
			
			public D createInstance(Row row) {
				return entityInflater.transform(row);
			}
		}
		
		private class RowIdentifier<D extends C> {
			
			private final I entityIdentifier;
			private final SubEntityDeterminer<D> rowConsumer;
			
			private RowIdentifier(I entityIdentifier, SubEntityDeterminer<D> rowConsumer) {
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
