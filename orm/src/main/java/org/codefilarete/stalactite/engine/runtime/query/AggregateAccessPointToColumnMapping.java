package org.codefilarete.stalactite.engine.runtime.query;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
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
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport.AccessorToColumnMap;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Arrays;

/**
 * Maps the aggregate property accessors points to their column: relations are taken into account, that's the main benefit of it.
 * Thus, anyone can get the column behind an accessor chain.
 * The mapping is collected through the {@link EntityJoinTree} by looking for {@link RelationJoinNode} and their {@link EntityMapping}
 * (actually the algorithm is more complex). 
 */
public class AggregateAccessPointToColumnMapping<C> {
	
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
	
	@VisibleForTesting
	Map<List<? extends ValueAccessPoint<?>>, Selectable<?>> getPropertyToColumn() {
		return propertyToColumn;
	}
	
	private void collectPropertiesMapping() {
		Deque<Accessor<?, ?>> accessorPath = new ArrayDeque<>();
		this.propertyToColumn.putAll(collectPropertyMapping(tree.getRoot(), accessorPath));
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
	 *
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
			if (identifierAssembler instanceof SingleIdentifierAssembler) {
				Column idColumn = ((SingleIdentifierAssembler) identifierAssembler).getColumn();
				ReversibleAccessor idAccessor = ((AccessorWrapperIdAccessor) entityMapping.getIdMapping().getIdAccessor()).getIdAccessor();
				propertyToColumn.put(Arrays.asList(idAccessor), joinNode.getOriginalColumnsToLocalOnes().get(idColumn));
			} else if (identifierAssembler instanceof DefaultComposedIdentifierAssembler) {
				((DefaultComposedIdentifierAssembler<?, ?>) identifierAssembler).getMapping().forEach((idAccessor, idColumn) -> {
					;
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
