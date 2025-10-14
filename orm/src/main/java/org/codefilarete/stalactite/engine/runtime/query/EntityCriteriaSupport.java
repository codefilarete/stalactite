package org.codefilarete.stalactite.engine.runtime.query;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.map.HashedMap;
import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.RelationJoinNode;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.RelationalEntityCriteria;
import org.codefilarete.stalactite.query.model.AbstractCriterion;
import org.codefilarete.stalactite.query.model.ColumnCriterion;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Criteria;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.LogicalOperator;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.PairIterator;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.stalactite.query.model.LogicalOperator.AND;
import static org.codefilarete.stalactite.query.model.LogicalOperator.OR;

/**
 * Implementation of {@link EntityCriteria}
 * 
 * @author Guillaume Mary
 * @see AggregateAccessPointToColumnMapping 
 */
public class EntityCriteriaSupport<C> implements RelationalEntityCriteria<C, EntityCriteriaSupport<C>>, ConfiguredEntityCriteria {
	
	/** Delegate of the query : targets of the API methods */
	private final Criteria<?> criteria = new Criteria<>();
	
	private final EntityCriteriaSupport<C> parent;
	
	/** Whole aggregate property-mapping that allows to get the columns that matches a property */
	private final AggregateAccessPointToColumnMapping<C> aggregateColumnMapping;
	
	private boolean hasCollectionCriteria;
	
	/**
	 * Official constructor.
	 * Relations are deduced from the given tree (taking {@link RelationJoinNode} and some other types of node into account) at the end of the
	 * persister build mechanism. This is done at this late stage to let the whole algorithm fill the tree and make all the relations available.
	 * Hence, this avoids registering the relations manually. However, this implies that this constructor depends on {@link PersisterBuilderContext#CURRENT}
	 * which means that it must be filled when calling this constructor.
	 * Relations will be collected by {@link AggregateAccessPointToColumnMapping}.
	 * 
	 * @param tree tree to lookup for properties through the registered joinNodeNames
	 */
	public EntityCriteriaSupport(EntityJoinTree<C, ?> tree) {
		this(tree, false);
	}
	
	/**
	 * Alternative constructor (by opposite to {@link EntityCriteriaSupport(EntityJoinTree)}) that collects properties immediately if
	 * <code>withImmediatePropertiesCollect<code> is true.
	 * 
	 * @param tree tree to lookup for properties through the registered joinNodeNames
	 * @param withImmediatePropertiesCollect true if the properties should be collected immediately, false if they must be postponed to the end of
	 *                                       persister build cycle, which make it depend on {@link PersisterBuilderContext#CURRENT}
	 */
	public EntityCriteriaSupport(EntityJoinTree<C, ?> tree, boolean withImmediatePropertiesCollect) {
		this(new AggregateAccessPointToColumnMapping<>(tree, withImmediatePropertiesCollect));
	}
	
	private EntityCriteriaSupport(AggregateAccessPointToColumnMapping<C> source) {
		this.aggregateColumnMapping = source;
		this.parent = null;
	}
	
	private EntityCriteriaSupport(AggregateAccessPointToColumnMapping<C> source, EntityCriteriaSupport<C> parent) {
		this.aggregateColumnMapping = source;
		this.parent = parent;
	}
	
	/**
	 * Clones this instance.
	 * Required because and(..) and or(..) methods irreversibly modify the criteria's internal state.
	 * Cloning allows reuse without tampering with the original instance.
	 * Note that aggregate properties scan is not done again: the copy reuses the one of the current instance.
	 */
	public EntityCriteriaSupport<C> copy() {
		return new EntityCriteriaSupport<>(aggregateColumnMapping);
	}
	
	public AggregateAccessPointToColumnMapping<C> getAggregateColumnMapping() {
		return aggregateColumnMapping;
	}
	
	public <O> EntityCriteriaSupport<C> add(LogicalOperator logicalOperator, List<? extends ValueAccessPoint<?>> accessPointChain, ConditionalOperator<O, ?> condition) {
		appendAsCriterion(logicalOperator, accessPointChain, condition);
		computeCollectionCriteriaIndicator(accessPointChain);
		return this;
	}
	
	void appendAsCriterion(LogicalOperator logicalOperator, List<? extends ValueAccessPoint<?>> accessPointChain, ConditionalOperator<?, ?> condition) {
		Selectable<?> column = aggregateColumnMapping.giveColumn(accessPointChain);
		criteria.add(new ColumnCriterion(logicalOperator, column, condition));
		if (criteria.getOperator() == null) {
			criteria.setOperator(logicalOperator);
		}
	}
	
	private void computeCollectionCriteriaIndicator(List<? extends ValueAccessPoint<?>> accessPointChain) {
		this.hasCollectionCriteria |= accessPointChain.stream()
				.anyMatch(valueAccessPoint -> Collection.class.isAssignableFrom(AccessorDefinition.giveDefinition(valueAccessPoint).getMemberType()));
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> and(SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		return add(AND, Arrays.asList(new AccessorByMethodReference<>(getter)), operator);
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> and(SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		return add(AND, Arrays.asList(new MutatorByMethodReference<>(setter)), operator);
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> or(SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		return add(OR, Arrays.asList(new AccessorByMethodReference<>(getter)), operator);
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> or(SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		return add(OR, Arrays.asList(new MutatorByMethodReference<>(setter)), operator);
	}
	
	@Override
	public EntityCriteriaSupport<C> beginNested() {
		EntityCriteriaSupport<C> abstractCriteria = new EntityCriteriaSupport<>(this.aggregateColumnMapping, this);
		// because we start a nested condition, we cast the argument as an AbstractCriterion, else (casting to CriteriaChain) would create a not
		// nested condition (without parentheses)
		this.criteria.add((AbstractCriterion) abstractCriteria.criteria);
		return abstractCriteria;
	}
	
	@Override
	public EntityCriteriaSupport<C> endNested() {
		return this.parent;
	}
	
	@Override
	public <A, B> EntityCriteriaSupport<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, ConditionalOperator<B, ?> operator) {
		return and(AccessorChain.fromMethodReferences(getter1, getter2).getAccessors(), operator);
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> and(List<? extends ValueAccessPoint<?>> getter, ConditionalOperator<O, ?> operator) {
		return add(AND, getter, operator);
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> or(List<? extends ValueAccessPoint<?>> getter, ConditionalOperator<O, ?> operator) {
		return add(OR, getter, operator);
	}
	
	@Override
	public CriteriaChain<?> getCriteria() {
		return criteria;
	}
	
	@Override
	public boolean hasCollectionCriteria() {
		return hasCollectionCriteria;
	}
	
	public boolean hasCollectionProperty() {
		return aggregateColumnMapping.hasCollectionProperty();
	}
	
	@Override
	public String toString() {
		return Reflections.toString(getClass())
				+ "criteria=" + criteria.getConditions()
				+ ", hasCollectionCriteria=" + hasCollectionCriteria
				+ ", parent=" + parent;
	}
	
	/**
	 * Maps a {@link List} of {@link ValueAccessPoint} to a {@link Selectable} column.
	 * Can be though as a duplicate of {@link org.codefilarete.reflection.ValueAccessPointMap}, but the goal is not the same: whereas
	 * ValueAccessPointMap is used to map a single {@link ValueAccessPoint}, this class is intended to map a {@link List} of {@link ValueAccessPoint},
	 * meaning that it may handle a {@link List} of {@link org.codefilarete.reflection.Mutator} or {@link Accessor}, or a mix of both, to fulfill the
	 * external usage of declaring the query criteria according to accessor or mutator.
	 * 
	 * The implementation is a {@link HashedMap} that uses the {@link AccessorDefinition} of each {@link ValueAccessPoint} to compute the hashCode and equality.
	 */
	@VisibleForTesting
	static class AccessorToColumnMap extends HashedMap<List<? extends ValueAccessPoint<?>>, JoinLink<?, ?>> {
		
		@Override
		protected int hash(Object key) {
			List<ValueAccessPoint<?>> accessors = (List<ValueAccessPoint<?>>) key;
			int result = 1;
			for (ValueAccessPoint<?> accessor : accessors) {
				AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(accessor);
				// Declaring class and name should be sufficient to compute a significant hashCode and memberType will be only
				// used in isEqualKey to make all of this robust
				result = 31 * result + 31 * accessorDefinition.getDeclaringClass().hashCode() + accessorDefinition.getName().hashCode();
			}
			return result;
		}
		
		@Override
		protected boolean isEqualKey(Object key1, Object key2) {
			List<ValueAccessPoint<?>> accessors1 = (List<ValueAccessPoint<?>>) key1;
			List<ValueAccessPoint<?>> accessors2 = (List<ValueAccessPoint<?>>) key2;
			List<AccessorDefinition> accessorDefinitions1 = accessors1.stream().map(AccessorDefinition::giveDefinition).collect(Collectors.toList());
			List<AccessorDefinition> accessorDefinitions2 = accessors2.stream().map(AccessorDefinition::giveDefinition).collect(Collectors.toList());
			PairIterator<AccessorDefinition, AccessorDefinition> pairIterator = new PairIterator<>(accessorDefinitions1, accessorDefinitions2);
			boolean result = false;
			while (!result && pairIterator.hasNext()) {
				Duo<AccessorDefinition, AccessorDefinition> accessorsPair = pairIterator.next();
				result = accessorsPair.getLeft().getDeclaringClass().equals(accessorsPair.getRight().getDeclaringClass())
						&& accessorsPair.getLeft().getName().equals(accessorsPair.getRight().getName())
						&& accessorsPair.getLeft().getMemberType().equals(accessorsPair.getRight().getMemberType());
			}
			return result;
		}
	}
}
