package org.codefilarete.stalactite.query;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.SimpleIdentifierAssembler;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Criteria;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.PairIterator;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Implementation of {@link EntityCriteria}
 * 
 * @author Guillaume Mary
 * @see #registerRelation(ValueAccessPoint, ConfiguredRelationalPersister) 
 */
public class EntityCriteriaSupport<C> implements RelationalEntityCriteria<C> {
	
	/** Delegate of the query : targets of the API methods */
	private final Criteria criteria = new Criteria();
	
	/** Root of the property-mapping graph representation. Might be completed with {@link #registerRelation(ValueAccessPoint, ConfiguredRelationalPersister)} */
	private final EntityGraphNode<C> rootConfiguration;
	
	/**
	 * Base constructor to start configuring an instance.
	 * Relations must be registered through {@link #registerRelation(ValueAccessPoint, ConfiguredRelationalPersister)}.
	 * 
	 * @param entityMapping entity mapping for direct and embedded properties
	 */
	public EntityCriteriaSupport(EntityMapping<C, ?, ?> entityMapping) {
		this.rootConfiguration = new EntityGraphNode<>(entityMapping);
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
	 * Adds a simple relation to the root of the property-mapping graph representation.
	 * 
	 * @param relation the representation of the method that gives access to the value, shouldn't be a chain of accessor
	 * @param persister the persister of related entities
	 */
	public void registerRelation(ValueAccessPoint<C> relation, ConfiguredRelationalPersister<?, ?> persister) {
		rootConfiguration.registerRelation(relation, persister);
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator logicalOperator, SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		return add(logicalOperator, rootConfiguration.getColumn(Arrays.asList(new AccessorByMethodReference<>(getter))), operator);
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator logicalOperator, SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		return add(logicalOperator, rootConfiguration.getColumn(Arrays.asList(new MutatorByMethodReference<>(setter))), operator);
	}
	
	private <O> EntityCriteriaSupport<C> add(LogicalOperator logicalOperator, Column column, ConditionalOperator<O, ?> operator) {
		if (logicalOperator == LogicalOperator.OR) {
			criteria.or(column, operator);
		} else {
			criteria.and(column, operator);
		}
		return this;
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> and(SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		return add(LogicalOperator.AND, getter, operator);
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> and(SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		return add(LogicalOperator.AND, setter, operator);
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> or(SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		return add(LogicalOperator.OR, getter, operator);
	}
	
	@Override
	public <O> EntityCriteriaSupport<C> or(SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		return add(LogicalOperator.OR, setter, operator);
	}
	
	@Override
	public <A, B> EntityCriteriaSupport<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, ConditionalOperator<B, ?> operator) {
		criteria.and(rootConfiguration.getColumn(AccessorChain.chain(getter1, getter2).getAccessors()), operator);
		return this;
	}
	
	@Override
	public <O> RelationalEntityCriteria<C> and(AccessorChain<C, O> getter, ConditionalOperator<O, ?> operator) {
		criteria.and(rootConfiguration.getColumn(getter.getAccessors()), operator);
		return this;
	}
	
	@Override
	public <S extends Collection<A>, A, B> EntityCriteriaSupport<C> andMany(SerializableFunction<C, S> getter1, SerializableFunction<A, B> getter2, ConditionalOperator<B, ?> operator) {
		criteria.and(rootConfiguration.getColumn(Arrays.asList(new AccessorByMethodReference<>(getter1), new AccessorByMethodReference<>(getter2))), operator);
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
		
		/**
		 * Owned properties mapping
		 * Implementations based on {@link AccessorDefinition}. Can be a quite heavy comparison but we have no other
		 * solution : propertyToColumn is build from entity mapping which can contains
		 * {@link org.codefilarete.reflection.PropertyAccessor} (for exemple) whereas getColumn() will get
		 * {@link AccessorByMethodReference} which are quite different but should be compared.
		 */
		private final Map<List<? extends ValueAccessPoint<?>>, Column> propertyToColumn = new HashedMap<List<? extends ValueAccessPoint<?>>, Column>() {
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
				List<AccessorDefinition> accessorDefintiions1 = accessors1.stream().map(AccessorDefinition::giveDefinition).collect(Collectors.toList());
				List<AccessorDefinition> accessorDefintiions2 = accessors2.stream().map(AccessorDefinition::giveDefinition).collect(Collectors.toList());
				PairIterator<AccessorDefinition, AccessorDefinition> pairIterator = new PairIterator<>(accessorDefintiions1, accessorDefintiions2);
				boolean result = false;
				while (!result && pairIterator.hasNext()) {
					Duo<AccessorDefinition, AccessorDefinition> accessorsPair = pairIterator.next();
					result = accessorsPair.getLeft().getDeclaringClass().equals(accessorsPair.getRight().getDeclaringClass())
							&& accessorsPair.getLeft().getName().equals(accessorsPair.getRight().getName())
							&& accessorsPair.getLeft().getMemberType().equals(accessorsPair.getRight().getMemberType());
				}
				return result;
			}
		};
		
		private final Class<?> entityClass;
		
		/** Relations mapping : one-to-one or one-to-many */
		private final ValueAccessPointMap<C, RelationalEntityPersister<?, ?>> relations = new ValueAccessPointMap<>();
		
		@VisibleForTesting
		EntityGraphNode(EntityMapping<C, ?, ?> entityMapping) {
			this.entityClass = entityMapping.getClassToPersist();
			Stream.concat(entityMapping.getPropertyToColumn().entrySet().stream(), entityMapping.getReadonlyPropertyToColumn().entrySet().stream())
					.forEach((entry) -> {
						ReversibleAccessor<C, Object> accessor = entry.getKey();
						List<? extends ValueAccessPoint<?>> key;
						if (accessor instanceof AccessorChain) {
							key = ((AccessorChain<?, ?>) accessor).getAccessors();
						} else {
							key = Arrays.asList(accessor);
						}
						propertyToColumn.put(key, entry.getValue());
					});
			// we add the identifier and primary key because they are not in the property mapping 
			IdMapping<C, ?> idMapping = entityMapping.getIdMapping();
			if (idMapping instanceof SimpleIdMapping) {
				Column primaryKey = ((SimpleIdentifierAssembler) idMapping.getIdentifierAssembler()).getColumn();
				propertyToColumn.put(Arrays.asList(((SimpleIdMapping<C, ?>) idMapping).getIdAccessor().getIdAccessor()), primaryKey);
			}
			entityMapping.getEmbeddedBeanStrategies().forEach((k, v) ->
					v.getPropertyToColumn().forEach((p, c) -> propertyToColumn.put(new AccessorChain<>(k, p).getAccessors(), c))
			);
		}
		
		/**
		 * Adds a {@link ClassMapping} as a relation of this node
		 * 
		 * @param relationProvider the accessor that gives access to a bean mapped by the {@link ClassMapping}
		 * @param mappingStrategy a {@link ClassMapping}
		 */
		@VisibleForTesting
		void registerRelation(ValueAccessPoint<C> relationProvider, RelationalEntityPersister<?, ?> mappingStrategy) {
			// the relation may already be present as a simple property because mapping strategy needs its column for insertion for example, but we
			// won't need it anymore. Note that it should be removed when propertyToColumn is populated but we don't have the relation information
			// at this time
			propertyToColumn.remove(Arrays.asList(relationProvider));
			relations.put(relationProvider, mappingStrategy);
		}
		
		/**
		 * Gives the column of a chain of access points
		 * Note that it supports "many" accessor : given parameter acts as a "transport" of accessors, we don't use
		 * its functionality such as get() or toMutator() hence it's not necessary that it be consistent. For example,
		 * it can start with a Collection accessor then an accessor to the component of the Collection. See
		 * 
		 * @param valueAccessPoints chain of accessors to a property that has a matching column
		 * @return the found column, throws an exception if not found
		 */
		public Column getColumn(List<? extends ValueAccessPoint<?>> valueAccessPoints) {
			// looking among current properties
			Column column = this.propertyToColumn.get(valueAccessPoints);
			if (column != null) {
				return column;
			} else {
				// asking to persister in relation
				RelationalEntityPersister<?, ?> entityGraphNode = relations.get(valueAccessPoints.get(0));
				if (entityGraphNode != null) {
					column = entityGraphNode.getColumn(valueAccessPoints.subList(1, valueAccessPoints.size()));
				}
				if (column == null) {
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
					throw new MappingConfigurationException("Error while looking for column of " + accessPointAsString
							+ " : it is not declared in mapping of " + Reflections.toString(this.entityClass));
				} else {
					return column;
				}
			}
		}
	}
}
