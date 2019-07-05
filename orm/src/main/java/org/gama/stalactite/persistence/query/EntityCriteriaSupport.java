package org.gama.stalactite.persistence.query;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Arrays;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.IAccessor;
import org.gama.reflection.MemberDefinition;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.ValueAccessPoint;
import org.gama.reflection.ValueAccessPointByMethodReference;
import org.gama.reflection.ValueAccessPointMap;
import org.gama.stalactite.persistence.engine.RuntimeMappingException;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.query.ValueAccessPointCriterion.LogicalOperator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.query.model.Criteria;
import org.gama.stalactite.query.model.CriteriaChain;

/**
 * Implementation of {@link EntityCriteria}
 * 
 * @author Guillaume Mary
 * @see #registerRelation(ValueAccessPoint, ClassMappingStrategy) 
 */
public class EntityCriteriaSupport<C> implements EntityCriteria<C> {
	
	/** Delegate of the query : targets of the API methods */
	private Criteria criteria = new Criteria();
	
	/** Root of the property-mapping graph representation. Must be constructed with {@link #registerRelation(ValueAccessPoint, ClassMappingStrategy)} */
	private final EntityGraphNode rootConfiguration;
	
	public <O> EntityCriteriaSupport(ClassMappingStrategy<C, ?, ?> mappingStrategy, SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
		this(mappingStrategy);
		add(null, getter, operator);
	}
	
	public <O> EntityCriteriaSupport(ClassMappingStrategy<C, ?, ?> mappingStrategy, SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		this(mappingStrategy);
		add(null, setter, operator);
	}
	
	public EntityCriteriaSupport(ClassMappingStrategy<C, ?, ?> mappingStrategy) {
		this.rootConfiguration = new EntityGraphNode(mappingStrategy);
	}
	
	public EntityCriteriaSupport(EntityCriteriaSupport source) {
		this.rootConfiguration = source.rootConfiguration;
	}
	
	public EntityGraphNode getRootConfiguration() {
		return rootConfiguration;
	}
	
	/**
	 * Adds a relation to the root of the property-mapping graph representation
	 * @param relation the representation of the method that gives access to the value
	 * @param mappingStrategy the mapping strategy of entities relation
	 * @return a newly created node to configure relations of this just-declared relation
	 */
	public EntityGraphNode registerRelation(ValueAccessPoint relation, ClassMappingStrategy<?, ?, ?> mappingStrategy) {
		return rootConfiguration.registerRelation(relation, mappingStrategy);
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator logicalOperator, SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
		return add(logicalOperator, getColumn(new AccessorByMethodReference<>(getter)), operator);
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator logicalOperator, SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		return add(logicalOperator, getColumn(new MutatorByMethodReference<>(setter)), operator);
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator logicalOperator, Column column, AbstractRelationalOperator<O> operator) {
		if (logicalOperator == LogicalOperator.OR) {
			criteria.or(column, operator);
		} else {
			criteria.and(column, operator);
		}
		return this;
	}
	
	@Override
	public <O> EntityCriteria<C> and(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
		return add(LogicalOperator.AND, getter, operator);
	}
	
	@Override
	public <O> EntityCriteria<C> and(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		return add(LogicalOperator.AND, setter, operator);
	}
	
	@Override
	public <O> EntityCriteria<C> or(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
		return add(LogicalOperator.OR, getter, operator);
	}
	
	@Override
	public <O> EntityCriteria<C> or(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		return add(LogicalOperator.OR, setter, operator);
	}
	
	@Override
	public <A, B> EntityCriteria<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator) {
		criteria.and(getColumn(new AccessorByMethodReference<>(getter1), new AccessorByMethodReference<>(getter2)), operator);
		return this;
	}
	
	@Override
	public <S extends Collection<A>, A, B> EntityCriteria<C> andMany(SerializableFunction<C, S> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator) {
		criteria.and(getColumn(new AccessorByMethodReference<>(getter1), new AccessorByMethodReference<>(getter2)), operator);
		return this;
	}
	
	private Column getColumn(ValueAccessPointByMethodReference ... methodReferences) {
		Column column = rootConfiguration.getColumn(methodReferences);
		if (column == null) {
			throw new IllegalArgumentException("No column found for " + MemberDefinition.toString(Arrays.asList(methodReferences)));
		}
		return column;
	}
	
	public CriteriaChain getCriteria() {
		return criteria;
	}
	
	/**
	 * Represents a bean mapping : its simple or embedded properties bound to columns, and its relations which are themselves a mapping between
	 * a method (as a generic {@link ValueAccessPoint}) and another {@link EntityGraphNode}
	 */
	public static class EntityGraphNode {
		
		/** Owned properties mapping */
		private final ValueAccessPointMap<Column> propertyToColumn = new ValueAccessPointMap<>();
		
		/** Relations mapping : one-to-one or one-to-many */
		private final Map<ValueAccessPoint, EntityGraphNode> relations = new ValueAccessPointMap<>();
		
		@VisibleForTesting
		EntityGraphNode(ClassMappingStrategy<?, ?, ?> mappingStrategy) {
			propertyToColumn.putAll(mappingStrategy.getPropertyToColumn());
			// we add the identifier and primary key because they are not in the property mapping 
			IdMappingStrategy<?, ?> idMappingStrategy = mappingStrategy.getIdMappingStrategy();
			if (idMappingStrategy instanceof SimpleIdMappingStrategy) {
				Column primaryKey = ((SimpleIdentifierAssembler) idMappingStrategy.getIdentifierAssembler()).getColumn();
				propertyToColumn.put(((SimpleIdMappingStrategy<?, ?>) idMappingStrategy).getIdAccessor().getIdAccessor(), primaryKey);
			}
			mappingStrategy.getMappingStrategies().forEach((k, v) ->
					v.getPropertyToColumn().forEach((p, c) ->
							propertyToColumn.put(new AccessorChain<>(k, p), c)
					)
			);
		}
		
		/**
		 * Adds a {@link ClassMappingStrategy} as a relation of this node
		 * 
		 * @param relationProvider the accessor that gives access to a bean mapped by the {@link ClassMappingStrategy}
		 * @param mappingStrategy a {@link ClassMappingStrategy}
		 * @return a new {@link EntityGraphNode} containing
		 */
		public EntityGraphNode registerRelation(ValueAccessPoint relationProvider, ClassMappingStrategy<?, ?, ?> mappingStrategy) {
			EntityGraphNode graphNode = new EntityGraphNode(mappingStrategy);
			// the relation may already be present as a simple property because mapping strategy needs its column for insertion for example, but we
			// won't need it anymore. Note that it should be removed when propertyToColumn is populated but we don't have the relation information
			// at this time
			propertyToColumn.remove(relationProvider);
			relations.put(relationProvider, graphNode);
			return graphNode;
		}
		
		private static AccessorChain toAccessorChain(ValueAccessPointByMethodReference... accessPoints) {
			AccessorChain<Object, Object> result = new AccessorChain<>();
			for (ValueAccessPointByMethodReference accessPoint : accessPoints) {
				if (accessPoint instanceof IAccessor) {
					result.add((IAccessor) accessPoint);
				} else if (accessPoint instanceof MutatorByMethodReference) {
					MutatorByMethodReference mutatorPoint = (MutatorByMethodReference) accessPoint;
					result.add(new AccessorByMethod<>(Reflections.getMethod(mutatorPoint.getDeclaringClass(), mutatorPoint.getMethodName(), mutatorPoint.getPropertyType())));
				} else {
					// This means an internal bad usage
					throw new UnsupportedOperationException("Creating a chain from something else than accessor is not supported : "
							+ Nullable.nullable(accessPoint).apply(Object::getClass).apply(Reflections::toString).orGet("null"));
				}
			}
			return result;
		}
		
		/**
		 * Gives the column of a chain of access points
		 * @param accessPoints one or several access points, that creates a chain to a property that has a matching column
		 * @return the found column, throws an exception if not found
		 */
		@VisibleForTesting
		Column getColumn(ValueAccessPointByMethodReference... accessPoints) {
			Column embeddedColumn = getEmbeddedColumn(accessPoints);
			if (embeddedColumn != null) {
				return embeddedColumn;
			}
			
			Column column = getRelationColumn(accessPoints);
			if (column != null) {
				return column;
			} else {
				throw new RuntimeMappingException("Column for " + MemberDefinition.toString(Arrays.asList(accessPoints)) + " was not found");
			}
		}
		
		private Column getEmbeddedColumn(ValueAccessPointByMethodReference... accessPoints) {
			return this.propertyToColumn.get(toAccessorChain(accessPoints));
		}
		
		private Column getRelationColumn(ValueAccessPointByMethodReference... accessPoints) {
			Deque<ValueAccessPoint> stack = new ArrayDeque<>();
			stack.addAll(Arrays.asList(accessPoints));
			EntityGraphNode currentNode = this;
			while (!stack.isEmpty()) {
				ValueAccessPoint pawn = stack.pop();
				Column column = currentNode.propertyToColumn.get(pawn);
				if (column != null) {
					return column;
				}
				
				EntityGraphNode entityGraphNode = currentNode.relations.get(pawn);
				if (entityGraphNode == null) {
					return null;
				} else {
					currentNode = entityGraphNode;
				}
			}
			return null;
		}
	}
}
