package org.codefilarete.stalactite.engine.runtime;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.map.HashedMap;
import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.configurer.DefaultComposedIdentifierAssembler;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecordMapping;
import org.codefilarete.stalactite.engine.runtime.load.AbstractJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.JoinRoot;
import org.codefilarete.stalactite.engine.runtime.load.RelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.TablePerClassRootJoinNode;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.RelationalEntityCriteria;
import org.codefilarete.stalactite.query.model.AbstractCriterion;
import org.codefilarete.stalactite.query.model.ColumnCriterion;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Criteria;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.LogicalOperator;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
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
 * @see AggregateAccessPointToColumnMapping#collectPropertiesMapping() 
 */
public class EntityCriteriaSupport<C> implements RelationalEntityCriteria<C, EntityCriteriaSupport<C>>, ConfiguredEntityCriteria {
	
	/** Delegate of the query : targets of the API methods */
	private final Criteria<?> criteria = new Criteria<>();
	
	private final EntityCriteriaSupport<C> parent;
	
	/** Root of the property-mapping graph representation. Must be completed with {@link AggregateAccessPointToColumnMapping#collectPropertiesMapping()} */
	private final AggregateAccessPointToColumnMapping<C> aggregateColumnMapping;
	
	private boolean hasCollectionCriteria;
	
	/**
	 * Official constructor.
	 * Relations are deduced from the given tree (taking {@link RelationJoinNode} and some other types of node into account) at the end of the
	 * persister build mechanism. This is done at this late stage to let the whole algorithm fill the tree and make all the relations available.
	 * Hence, this avoids registering the relations manually. However, this implies that this constructor depends on {@link PersisterBuilderContext#CURRENT}
	 * which means that it must be filled when calling this constructor.
	 * Relations will be collected through {@link AggregateAccessPointToColumnMapping#collectPropertiesMapping()}.
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
	EntityCriteriaSupport(EntityJoinTree<C, ?> tree, boolean withImmediatePropertiesCollect) {
		this(new AggregateAccessPointToColumnMapping<>(tree, withImmediatePropertiesCollect));
	}
	
	/**
	 * Constructor that clones an instance.
	 * Made because calling and(..), or(..) methods alter internal state of the criteria and can't be rolled back.
	 * So, in order to create several criteria, this instance must be cloned.
	 * 
	 * @param source an already-configured {@link EntityCriteriaSupport}
	 */
	public EntityCriteriaSupport(EntityCriteriaSupport<C> source) {
		this(source.aggregateColumnMapping);
	}
	
	private EntityCriteriaSupport(AggregateAccessPointToColumnMapping<C> source) {
		this.aggregateColumnMapping = source;
		this.parent = null;
	}
	
	private EntityCriteriaSupport(AggregateAccessPointToColumnMapping<C> source, EntityCriteriaSupport<C> parent) {
		this.aggregateColumnMapping = source;
		this.parent = parent;
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
	
	@Override
	public String toString() {
		return Reflections.toString(getClass())
				+ "criteria=" + criteria.getConditions()
				+ ", hasCollectionCriteria=" + hasCollectionCriteria
				+ ", parent=" + parent;
	}
	
	/**
	 * Maps the aggregate property accessors points to their column: relations are taken into account, that's the main benefit of it.
	 * Thus, anyone can get the column behind an accessor chain.
	 * The mapping is collected through the {@link EntityJoinTree} by looking for {@link RelationJoinNode} (algorithm is more complex).
	 */
	public static class AggregateAccessPointToColumnMapping<C> {
		
		/**
		 * Owned properties mapping
		 * The implementation is based on {@link AccessorDefinition} comparison as keys. This can be a quite heavy comparison but we have no other
		 * choice : propertyToColumn is built from entity mapping which can contains
		 * - {@link PropertyAccessor} (for exemple) whereas getColumn() will get
		 * - {@link AccessorByMethodReference} which are quite different but should be compared.
		 */
		private final Map<List<? extends ValueAccessPoint<?>>, Selectable<?>> propertyToColumn = new AccessorToColumnMap();
		
		private final EntityJoinTree<C, ?> tree;
		
		@VisibleForTesting
		AggregateAccessPointToColumnMapping(EntityJoinTree<C, ?> tree, boolean withImmediatePropertiesCollect) {
			this.tree = tree;
			
			if (withImmediatePropertiesCollect) {
				collectPropertiesMapping();
			} else {
				PersisterBuilderContext.CURRENT.get().addBuildLifeCycleListener(new BuildLifeCycleListener() {
					@Override
					public void afterBuild() {
						collectPropertiesMapping();
					}
					
					@Override
					public void afterAllBuild() {
						
					}
				});
			}
		}
		
		private void collectPropertiesMapping() {
			Deque<Accessor<?, ?>> accessorPath = new ArrayDeque<>();
			this.propertyToColumn.putAll(collectPropertyMapping(tree.getRoot(),accessorPath));
			Queue<AbstractJoinNode<?, ?, ?, ?>> stack = Collections.asLifoQueue(new ArrayDeque<>());
			stack.addAll(tree.getRoot().getJoins());
			while (!stack.isEmpty()) {
				AbstractJoinNode<?, ?, ?, ?> abstractJoinNode = stack.poll();
				if (abstractJoinNode instanceof RelationJoinNode) {
					RelationJoinNode<?, ?, ?, ?, ?> relationJoinNode = (RelationJoinNode<?, ?, ?, ?, ?>) abstractJoinNode;
					accessorPath.add(relationJoinNode.getPropertyAccessor());
					Map<List<ValueAccessPoint<?>>, Selectable<?>> m = collectPropertyMapping(relationJoinNode, accessorPath);

					this.propertyToColumn.putAll(m);
					if (abstractJoinNode.getJoins().isEmpty()) {
						// no more joins, this is a leaf
						accessorPath.removeLast();
					}
				}
				stack.addAll(abstractJoinNode.getJoins());
			}
		}
		
		/**
		 * Collect properties, read-only properties, and identifier column mapping from given {@link EntityMapping}
		 * @param joinNode the node from which we can get a {@link EntityMapping} to collect all properties from
		 */
		@VisibleForTesting
		<E> Map<List<ValueAccessPoint<?>>, Selectable<?>> collectPropertyMapping(JoinNode<E, ?> joinNode, Deque<Accessor<?, ?>> accessorPath) {
			EntityInflater<E, ?> entityInflater;
			if (joinNode instanceof RelationJoinNode) {
				entityInflater = ((RelationJoinNode<E, ?, ?, ?, ?>) joinNode).getEntityInflater();
			} else if (joinNode instanceof TablePerClassRootJoinNode) {
				entityInflater = ((TablePerClassRootJoinNode<E, ?>) joinNode).getEntityInflater();
				EntityMapping<E, ?, ?> entityMapping = entityInflater.getEntityMapping();
				Map<List<ValueAccessPoint<?>>, Selectable<?>> propertyToColumn = new HashMap<>();
				PseudoTable pseudoTable = ((TablePerClassRootJoinNode<E, ?>) joinNode).getTable();
				
				
				Stream.concat(entityMapping.getPropertyToColumn().entrySet().stream(), entityMapping.getReadonlyPropertyToColumn().entrySet().stream())
						.forEach((entry) -> {
							ReversibleAccessor<E, ?> accessor = entry.getKey();
							List<ValueAccessPoint<?>> key;
							if (accessor instanceof AccessorChain) {
								key = new ArrayList<>(((AccessorChain<?, ?>) accessor).getAccessors());
							} else {
								key = Arrays.asList(accessor);
							}
							propertyToColumn.put(key, joinNode.getOriginalColumnsToLocalOnes().get(pseudoTable.findColumn(entry.getValue().getExpression())));
						});
				
				// we add the identifier and primary key because they are not in the property mapping 
				IdentifierAssembler<?, ?> identifierAssembler = entityMapping.getIdMapping().getIdentifierAssembler();
				// TODO: implement other types of identifier-assemblers
				if (identifierAssembler instanceof SingleIdentifierAssembler) {
					Column idColumn = ((SingleIdentifierAssembler) identifierAssembler).getColumn();
					ReversibleAccessor idAccessor = ((AccessorWrapperIdAccessor) entityMapping.getIdMapping().getIdAccessor()).getIdAccessor();
					propertyToColumn.put(Arrays.asList(idAccessor), joinNode.getOriginalColumnsToLocalOnes().get(pseudoTable.findColumn(idColumn.getExpression())));
				}
				
				entityMapping.getEmbeddedBeanStrategies().forEach((k, v) ->
						v.getPropertyToColumn().forEach((p, c) -> {
							propertyToColumn.put(Arrays.asList(k), c);
						})
				);
				
				propertyToColumn.forEach((k, v) -> {
					k.addAll(0, accessorPath);
				});
				return propertyToColumn;
			} else if (joinNode instanceof JoinRoot) {
				entityInflater = ((JoinRoot<E, ?, ?>) joinNode).getEntityInflater();
			} else {
				// this should not happen because we master the node types that support getEntityInflater !
				throw new UnsupportedOperationException("Unsupported join type " + Reflections.toString(joinNode.getClass()));
			}
			EntityMapping<E, ?, ?> entityMapping = entityInflater.getEntityMapping();
			Map<List<ValueAccessPoint<?>>, Selectable<?>> propertyToColumn = new HashMap<>();
			if (entityMapping instanceof ElementRecordMapping) {    // Collection mapping case
				// TODO: handle complex key and value cases
				List<ValueAccessPoint<?>> accessors = new ArrayList<>(accessorPath);
				Stream.concat(entityMapping.getPropertyToColumn().entrySet().stream(), entityMapping.getReadonlyPropertyToColumn().entrySet().stream())
						.forEach((entry) -> {
							propertyToColumn.put(accessors, joinNode.getOriginalColumnsToLocalOnes().get(entry.getValue()));
						});
				
			} else {
				Stream.concat(entityMapping.getPropertyToColumn().entrySet().stream(), entityMapping.getReadonlyPropertyToColumn().entrySet().stream())
						.forEach((entry) -> {
							ReversibleAccessor<E, ?> accessor = entry.getKey();
							List<ValueAccessPoint<?>> key;
							if (accessor instanceof AccessorChain) {
								key = new ArrayList<>(((AccessorChain<?, ?>) accessor).getAccessors());
							} else {
								key = Arrays.asList(accessor);
							}
							propertyToColumn.put(key, joinNode.getOriginalColumnsToLocalOnes().get(entry.getValue()));
						});
				
				// we add the identifier and primary key because they are not in the property mapping 
				IdentifierAssembler<?, ?> identifierAssembler = entityMapping.getIdMapping().getIdentifierAssembler();
				// TODO: implement other types of identifier-assemblers
				if (identifierAssembler instanceof SingleIdentifierAssembler) {
					Column idColumn = ((SingleIdentifierAssembler) identifierAssembler).getColumn();
					ReversibleAccessor idAccessor = ((AccessorWrapperIdAccessor) entityMapping.getIdMapping().getIdAccessor()).getIdAccessor();
					propertyToColumn.put(Arrays.asList(idAccessor), joinNode.getOriginalColumnsToLocalOnes().get(idColumn));
				} else if (identifierAssembler instanceof DefaultComposedIdentifierAssembler) {
					((DefaultComposedIdentifierAssembler<?, ?>) identifierAssembler).getMapping().forEach((idAccessor, idColumn) -> {;
						ReversibleAccessor idAccessor2 = ((AccessorWrapperIdAccessor) entityMapping.getIdMapping().getIdAccessor()).getIdAccessor();
						propertyToColumn.put(Arrays.asList(idAccessor2, idAccessor), joinNode.getOriginalColumnsToLocalOnes().get(idColumn));
					});
				}
				
				entityMapping.getEmbeddedBeanStrategies().forEach((k, v) ->
						v.getPropertyToColumn().forEach((p, c) -> {
							propertyToColumn.put(Arrays.asList(k), c);
						})
				);
				
				propertyToColumn.forEach((k, v) -> {
					k.addAll(0, accessorPath);
				});
			}
			return propertyToColumn;
		}
		
		/**
		 * Gives the column of an access points {@link List}.
		 * Most of the time result is a 1-size collection, but in the case of complex type (as for a composite key), the result is a collection of columns.
		 * Note that it supports "many" accessor: given parameter acts as a "transport" of accessors, we don't use
		 * its functionality such as get() or toMutator() hence it's not necessary that it be consistent. For example,
		 * it can start with a Collection accessor then an accessor to the component of the Collection. See
		 * 
		 * @param valueAccessPoints chain of accessors to a property that has a matching column
		 * @return the found column, throws an exception if not found
		 */
		public Selectable<?> giveColumn(List<? extends ValueAccessPoint<?>> valueAccessPoints) {
			// looking among current properties
			Selectable<?> column = this.propertyToColumn.get(valueAccessPoints);
			if (column != null) {
				return column;
			} else {
				throw newConfigurationException(valueAccessPoints);
			}
		}
		
		private MappingConfigurationException newConfigurationException(List<? extends ValueAccessPoint<?>> valueAccessPoints) {
			StringAppender accessPointAsString = new StringAppender() {
				@Override
				public StringAppender cat(Object o) {
					if (o instanceof ValueAccessPoint) {
						super.cat(AccessorDefinition.toString((ValueAccessPoint) o));
					} else {
						super.cat(o);
					}
					return this;
				}
			};
			accessPointAsString.ccat(valueAccessPoints, " > ");
			return new MappingConfigurationException("Error while looking for column of " + accessPointAsString
					+ " : it is not declared in mapping of " + Reflections.toString(this.tree.getRoot().getEntityInflater().getEntityType()));
		}
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
	static class AccessorToColumnMap extends HashedMap<List<? extends ValueAccessPoint<?>>, Selectable<?>> {
		
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
