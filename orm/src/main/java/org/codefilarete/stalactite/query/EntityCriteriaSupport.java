package org.codefilarete.stalactite.query;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.RuntimeMappingException;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.SimpleIdentifierAssembler;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Criteria;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.VisibleForTesting;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Implementation of {@link EntityCriteria}
 * 
 * @author Guillaume Mary
 * @see #registerRelation(ValueAccessPoint, EntityMapping) 
 */
public class EntityCriteriaSupport<C> implements RelationalEntityCriteria<C> {
	
	/** Delegate of the query : targets of the API methods */
	private final Criteria criteria = new Criteria();
	
	/** Root of the property-mapping graph representation. Might be completed with {@link #registerRelation(ValueAccessPoint, EntityMapping)} */
	private final EntityGraphNode<C> rootConfiguration;
	
	/**
	 * Base constructor to start configuring an instance.
	 * Relations must be registered through {@link #registerRelation(ValueAccessPoint, EntityMapping)}.
	 * 
	 * @param entityMapping entity mapping for direct and embedded properties
	 */
	public EntityCriteriaSupport(EntityMapping<C, ?, ?> entityMapping) {
		this.rootConfiguration = new EntityGraphNode<C>(entityMapping);
	}
	
	/**
	 * Constructor that clones an instance.
	 * Made because calling and(..), or(..) methods alter internal state of the criteria and can't be rolled back.
	 * So, in order to create several criteria, this instance must be cloned.
	 * 
	 * @param source an already-configured {@link EntityCriteriaSupport}
	 */
	public EntityCriteriaSupport(EntityCriteriaSupport<C> source) {
		this.rootConfiguration = source.rootConfiguration;
	}
	
	public EntityGraphNode<C> getRootConfiguration() {
		return rootConfiguration;
	}
	
	/**
	 * Adds a relation to the root of the property-mapping graph representation
	 * @param relation the representation of the method that gives access to the value
	 * @param entityMapping the mapping of related entities
	 * @return a newly created node to configure relations of this just-declared relation
	 */
	public EntityGraphNode<?> registerRelation(ValueAccessPoint<C> relation, EntityMapping<?, ?, ?> entityMapping) {
		return rootConfiguration.registerRelation(relation, entityMapping);
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator logicalOperator, SerializableFunction<C, O> getter, ConditionalOperator<O> operator) {
		return add(logicalOperator, rootConfiguration.getColumn(new AccessorByMethodReference<>(getter)), operator);
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator logicalOperator, SerializableBiConsumer<C, O> setter, ConditionalOperator<O> operator) {
		return add(logicalOperator, rootConfiguration.getColumn(new MutatorByMethodReference<>(setter)), operator);
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator logicalOperator, Column column, ConditionalOperator<O> operator) {
		if (logicalOperator == LogicalOperator.OR) {
			criteria.or(column, operator);
		} else {
			criteria.and(column, operator);
		}
		return this;
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> and(SerializableFunction<C, O> getter, ConditionalOperator<O> operator) {
		return add(LogicalOperator.AND, getter, operator);
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> and(SerializableBiConsumer<C, O> setter, ConditionalOperator<O> operator) {
		return add(LogicalOperator.AND, setter, operator);
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> or(SerializableFunction<C, O> getter, ConditionalOperator<O> operator) {
		return add(LogicalOperator.OR, getter, operator);
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> or(SerializableBiConsumer<C, O> setter, ConditionalOperator<O> operator) {
		return add(LogicalOperator.OR, setter, operator);
	}
	
	@Override
	public <A, B> EntityCriteriaSupport<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, ConditionalOperator<B> operator) {
		criteria.and(rootConfiguration.getColumn(AccessorChain.chain(getter1, getter2)), operator);
		return this;
	}
	
	@Override
	public <O> RelationalEntityCriteria<C> and(AccessorChain<C, O> getter, ConditionalOperator<O> operator) {
		criteria.and(rootConfiguration.getColumn(getter), operator);
		return this;
	}
	
	@Override
	public <S extends Collection<A>, A, B> EntityCriteriaSupport<C> andMany(SerializableFunction<C, S> getter1, SerializableFunction<A, B> getter2, ConditionalOperator<B> operator) {
		criteria.and(rootConfiguration.getColumn(new AccessorChain<>(new AccessorByMethodReference<>(getter1), new AccessorByMethodReference<>(getter2))), operator);
		return this;
	}
	
	public CriteriaChain getCriteria() {
		return criteria;
	}
	
	/**
	 * Represents a bean mapping : its simple or embedded properties bound to columns, and its relations which are themselves a mapping between
	 * a method (as a generic {@link ValueAccessPoint}) and another {@link EntityGraphNode}
	 */
	public static class EntityGraphNode<C> {
		
		/** Owned properties mapping */
		private final ValueAccessPointMap<C, Column> propertyToColumn = new ValueAccessPointMap<>();
		
		private final ValueAccessPointMap<C, Column> readonlyPropertyToColumn = new ValueAccessPointMap<>();
		
		/** Relations mapping : one-to-one or one-to-many */
		private final ValueAccessPointMap<C, EntityGraphNode<?>> relations = new ValueAccessPointMap<>();
		
		@VisibleForTesting
		EntityGraphNode(EntityMapping<C, ?, ?> mappingStrategy) {
			propertyToColumn.putAll(mappingStrategy.getPropertyToColumn());
			readonlyPropertyToColumn.putAll(mappingStrategy.getReadonlyPropertyToColumn());
			// we add the identifier and primary key because they are not in the property mapping 
			IdMapping<C, ?> idMapping = mappingStrategy.getIdMapping();
			if (idMapping instanceof SimpleIdMapping) {
				Column primaryKey = ((SimpleIdentifierAssembler) idMapping.getIdentifierAssembler()).getColumn();
				propertyToColumn.put(((SimpleIdMapping<C, ?>) idMapping).getIdAccessor().getIdAccessor(), primaryKey);
			}
			mappingStrategy.getEmbeddedBeanStrategies().forEach((k, v) ->
					v.getPropertyToColumn().forEach((p, c) -> {
								propertyToColumn.put(new AccessorChain<>(k, p), c);
								readonlyPropertyToColumn.put(new AccessorChain<>(k, p), c);
							}
					)
			);
		}
		
		/**
		 * Adds a {@link ClassMapping} as a relation of this node
		 * 
		 * @param relationProvider the accessor that gives access to a bean mapped by the {@link ClassMapping}
		 * @param mappingStrategy a {@link ClassMapping}
		 * @return a new {@link EntityGraphNode} containing
		 */
		@VisibleForTesting
		EntityGraphNode<?> registerRelation(ValueAccessPoint<C> relationProvider, EntityMapping<?, ?, ?> mappingStrategy) {
			EntityGraphNode<?> graphNode = new EntityGraphNode<>(mappingStrategy);
			// the relation may already be present as a simple property because mapping strategy needs its column for insertion for example, but we
			// won't need it anymore. Note that it should be removed when propertyToColumn is populated but we don't have the relation information
			// at this time
			propertyToColumn.remove(relationProvider);
			relations.put(relationProvider, graphNode);
			return graphNode;
		}
		
		/**
		 * Gives the column of a chain of access points
		 * Note that it supports "many" accessor : given parameter acts as a "transport" of accessors, we don't use
		 * its functionality such as get() or toMutator() hence it's not necessary that it be consistent. For exemple
		 * it can start with a Collection accessor then an accessor to the component of the Collection. See
		 * 
		 * @param accessorChain chain to a property that has a matching column
		 * @return the found column, throws an exception if not found
		 */
		@VisibleForTesting
		Column getColumn(AccessorChain<C, ?> accessorChain) {
			Column embeddedColumn = getEmbeddedColumn(accessorChain);
			if (embeddedColumn != null) {
				return embeddedColumn;
			} else {
				Column column = giveRelationColumn(accessorChain);
				if (column != null) {
					return column;
				} else {
					throw new RuntimeMappingException("Column for " + AccessorDefinition.toString(accessorChain) + " was not found");
				}
			}
		}
		
		private Column getColumn(ValueAccessPoint<C> valueAccessPoint) {
			return this.propertyToColumn.get(valueAccessPoint);
		}
		
		private Column getEmbeddedColumn(AccessorChain<C, ?> accessorChain) {
			return this.propertyToColumn.get(accessorChain);
		}
		
		private Column giveRelationColumn(AccessorChain<C, ?> accessorChain) {
			Deque<ValueAccessPoint<?>> stack = new ArrayDeque<>(accessorChain.getAccessors());
			EntityGraphNode<?> currentNode = this;
			while (!stack.isEmpty()) {
				ValueAccessPoint<?> pawn = stack.pop();
				Column column = currentNode.propertyToColumn.get(pawn);
				if (column != null) {
					return column;
				} else {
					column = currentNode.readonlyPropertyToColumn.get(pawn);
					if (column != null) {
						return column;
					} else {
						EntityGraphNode<?> entityGraphNode = currentNode.relations.get(pawn);
						if (entityGraphNode == null) {
							return null;
						} else {
							currentNode = entityGraphNode;
						}
					}
				}
			}
			return null;
		}
	}
}
