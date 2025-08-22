package org.codefilarete.stalactite.engine.runtime.query;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.reflection.AbstractReflector;
import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMember;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.ReversibleMutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.engine.EntityPersister.LimitAware;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport.EntityQueryPageSupport.OrderByItem;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister.ExecutableEntityQueryCriteria;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.query.RelationalEntityCriteria;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.codefilarete.tool.function.ThrowingExecutable;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * <ul>
 * Class aimed at handling entity query configuration and execution triggering :
 * <li>query configuration will be done by redirecting {@link CriteriaChain} methods to an {@link EntityQueryCriteriaSupport}.</li>
 * <li>execution triggering calls {@link EntityFinder#select(ConfiguredEntityCriteria, OrderBy, Limit, Map)}
 * and wraps it into {@link PersisterListenerCollection#doWithSelectListener(Iterable, ThrowingExecutable)}</li>
 * </ul>
 * 
 * @param <C> entity type
 * @param <I> identifier type
 * @author Guillaume Mary
 */
public class EntityQueryCriteriaSupport<C, I> {
	
	public static final Logger LOGGER = LoggerFactory.getLogger(EntityQueryCriteriaSupport.class);
	
	/** Support for {@link EntityCriteria} query execution */
	private final EntityFinder<C, I> entityFinder;
	
	private final EntityCriteriaSupport<C> entityCriteriaSupport;
	
	private final EntityQueryPageSupport<C> queryPageSupport;
	
	public EntityQueryCriteriaSupport(EntityFinder<C, I> entityFinder,
									  EntityCriteriaSupport<C> entityCriteriaSupport) {
		this(entityFinder, entityCriteriaSupport, new EntityQueryPageSupport<>());
	}
	
	private EntityQueryCriteriaSupport(EntityFinder<C, I> entityFinder,
									   EntityCriteriaSupport<C> entityCriteriaSupport,
									   EntityQueryPageSupport<C> queryPageSupport) {
		this.entityFinder = entityFinder;
		this.entityCriteriaSupport = entityCriteriaSupport;
		this.queryPageSupport = queryPageSupport;
	}
	
	/**
	 * Makes a copy of this instance merged with given one
	 * Made to handle Spring Data's different ways of sorting (should have been put closer to its usage, but was too complex)
	 * 
	 * @param otherPageSupport some other paging options
	 * @return a merge of this instance with given page options
	 */
	public EntityQueryCriteriaSupport<C, I> copyFor(EntityQueryPageSupport<C> otherPageSupport) {
		return new EntityQueryCriteriaSupport<>(entityFinder, entityCriteriaSupport, queryPageSupport.merge(otherPageSupport));
	}
	
	public EntityCriteriaSupport<C> getEntityCriteriaSupport() {
		return entityCriteriaSupport;
	}
	
	public EntityQueryPageSupport<C> getQueryPageSupport() {
		return queryPageSupport;
	}
	
	public ExecutableEntityQueryCriteria<C, ?> wrapIntoExecutable() {
		Map<String, Object> values = new HashMap<>();
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		return methodDispatcher
				.redirect((SerializableBiFunction<ExecutableQuery<C>, Accumulator<C, ? extends Collection<C>, Object>, Object>) ExecutableQuery::execute,
						wrapGraphLoad(values))
				.redirect((SerializableTriFunction<ExecutableEntityQuery<?, ?>, String, Object, Object>) ExecutableEntityQuery::set,
						// Don't use "values::put" because its signature returns previous value, which means it is a Function
						// and dispatch to redirect(..) that takes a Function as argument, which, at runtime,
						// will create some ClassCastException due to incompatible type between ExecutableEntityQuery
						// and values contained in the Map (because ExecutableEntityQuery::set returns ExecutableEntityQuery)
						(s, object) -> { values.put(s, object); }
				)
				.redirect(OrderByChain.class, queryPageSupport, true)
				.redirect(LimitAware.class, queryPageSupport, true)
				.redirect(RelationalEntityCriteria.class, entityCriteriaSupport, true)
				// making an exception for 2 of the methods that can't return the proxy
				.redirect((SerializableFunction<ConfiguredEntityCriteria, CriteriaChain>) ConfiguredEntityCriteria::getCriteria, entityCriteriaSupport::getCriteria)
				.redirect((SerializableFunction<ConfiguredEntityCriteria, Boolean>) ConfiguredEntityCriteria::hasCollectionCriteria, entityCriteriaSupport::hasCollectionCriteria)
				.build((Class<ConfiguredExecutableEntityQueryCriteria<C>>) (Class) ConfiguredExecutableEntityQueryCriteria.class);
	}
	
	/**
	 * A mashup to redirect all {@link ExecutableEntityQueryCriteria} methods being redirected to {@link EntityCriteriaSupport} while redirecting
	 * {@link ConfiguredEntityCriteria} methods to some specific methods of {@link EntityCriteriaSupport}.
	 * Made as such to avoid to expose internal / implementation methods "getCriteria" and "hasCollectionCriteria" to the
	 * configuration API ({@link ExecutableEntityQueryCriteria})
	 *
	 * @param <C>
	 * @author Guillaume Mary
	 */
	private interface ConfiguredExecutableEntityQueryCriteria<C> extends ConfiguredEntityCriteria, ExecutableEntityQueryCriteria<C, ConfiguredExecutableEntityQueryCriteria<C>> {
		
	}
	
	public <R> Function<Accumulator<C, ? extends Collection<C>, R>, R> wrapGraphLoad(Map<String, Object> values) {
		if (queryPageSupport.getLimit() != null && entityCriteriaSupport.hasCollectionProperty()) {
			throw new UnsupportedOperationException("Can't limit query when entity graph contains Collection relations");
		}
		if (entityCriteriaSupport.hasCollectionCriteria() && !queryPageSupport.getOrderBy().isEmpty()) {
			// a collection property in criteria will trigger a 2-phases load (ids, then entities)
			// which is no compatible with an SQL "order by" clause, therefore, we sort the result in memory
			// and we don't ask for SQL "order by" because it's useless
			// Note that we must wrap this creation in an if statement due to that entities don't implement Comparable, we avoid a
			// ClassCastException of the addAll(..) operation
			return (Accumulator<C, ? extends Collection<C>, R> accumulatorParam) -> {
				LOGGER.debug("Sorting loaded entities in memory");
				Set<C> loadedEntities = entityFinder.select(
						entityCriteriaSupport,
						new OrderBy(),
						queryPageSupport.getLimit(), values);
				TreeSet<C> sortedResult = new TreeSet<>(buildComparator(queryPageSupport.getOrderBy()));
				sortedResult.addAll(loadedEntities);
				return accumulatorParam.collect(sortedResult);
			};
		} else {
			// single query
			return (Accumulator<C, ? extends Collection<C>, R> accumulatorParam) -> {
				OrderBy orderBy = new OrderBy();
				queryPageSupport.getOrderBy().forEach(duo -> {
					Selectable column = entityCriteriaSupport.getAggregateColumnMapping().giveColumn(duo.getProperty());
					orderBy.add(
							duo.isIgnoreCase()
									? Operators.lowerCase(column)
									: column,
							duo.getDirection() == Order.ASC
									? org.codefilarete.stalactite.query.model.OrderByChain.Order.ASC
									: org.codefilarete.stalactite.query.model.OrderByChain.Order.DESC);
				});
				Set<C> select = entityFinder.select(
						entityCriteriaSupport,
						orderBy,
						queryPageSupport.getLimit(),
						values);
				return accumulatorParam.collect(select);
			};
		}
	}
	
	@VisibleForTesting
	static <C> Comparator<C> buildComparator(KeepOrderSet<OrderByItem> orderBy) {
		Nullable<Comparator> result = nullable((Comparator) null);
		orderBy.forEach(orderByPawn -> {
			AccessorChain<Object, Comparable> propertyAccessor = orderByPawn.propertyAsAccessorChain();
			Comparator comparator;
			if (orderByPawn.isIgnoreCase()) {
				AccessorChain<Object, String> stringAccessor = (AccessorChain<Object, String>) (AccessorChain) propertyAccessor;
				comparator = Comparator.comparing(stringAccessor::get, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
			} else {
				comparator = Comparator.comparing(propertyAccessor::get, Comparator.nullsLast(Comparator.naturalOrder()));
			}
			if (orderByPawn.getDirection() == Order.DESC) {
				comparator = comparator.reversed();
			}
			if (result.isPresent()) {
				Comparator finalComparator = comparator;
				result.map(c -> c.thenComparing(finalComparator));
			} else {
				result.set(comparator);
			}
		});
		return result.get();
	}
	
	/**
	 * Gives the {@link Accessor} underneath given {@link ValueAccessPoint}, either being itself or its mirror if it's a {@link ReversibleMutator}
	 * @param valueAccessPoint a property accessor from which we need an {@link Accessor}
	 * @param <C> declaring class type
	 * @param <T> property type
	 * @return given {@link ValueAccessPoint} as an {@link Accessor}
	 */
	private static <C, T> Accessor<C, T> toAccessor(ValueAccessPoint<C> valueAccessPoint) {
		if (valueAccessPoint instanceof Accessor) {
			return (Accessor<C, T>) valueAccessPoint;
		} else if (valueAccessPoint instanceof ReversibleMutator) {
			return ((ReversibleMutator<C, T>) valueAccessPoint).toAccessor();
		} else {
			AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(valueAccessPoint);
			AccessorByMember accessor = Accessors.accessor(accessorDefinition.getDeclaringClass(), accessorDefinition.getName(), accessorDefinition.getMemberType());
			return (Accessor<C, T>) accessor;
		}
	}
	
	
	/**
	 * Simple class that stores paging options of the query
	 * @author Guillaume Mary
	 */
	public static class EntityQueryPageSupport<C>
			implements OrderByChain<C, EntityQueryPageSupport<C>>, LimitAware<EntityQueryPageSupport<C>> {
		
		private Limit limit;
		private final KeepOrderSet<OrderByItem> orderBy = new KeepOrderSet<>();
		
		private Limit getLimit() {
			return limit;
		}
		
		private KeepOrderSet<OrderByItem> getOrderBy() {
			return orderBy;
		}
		
		@Override
		public EntityQueryPageSupport<C> limit(int count) {
			limit = new Limit(count);
			return this;
		}
		
		@Override
		public EntityQueryPageSupport<C> limit(int count, Integer offset) {
			limit = new Limit(count, offset);
			return this;
		}
		
		@Override
		public EntityQueryPageSupport<C> orderBy(SerializableFunction<C, ?> getter, Order order) {
			AccessorByMethodReference<C, ?> methodReference = new AccessorByMethodReference<>(getter);
			orderBy.add(new OrderByItem(Arrays.asList(methodReference), order, false));
			assertAccessorIsNotIterable(methodReference, methodReference.getPropertyType());
			return this;
		}
		
		@Override
		public EntityQueryPageSupport<C> orderBy(SerializableBiConsumer<C, ?> setter, Order order) {
			MutatorByMethodReference<C, ?> methodReference = new MutatorByMethodReference<>(setter);
			orderBy.add(new OrderByItem(Arrays.asList(methodReference), order, false));
			assertAccessorIsNotIterable(methodReference, methodReference.getPropertyType());
			return this;
		}
		
		@Override
		public EntityQueryPageSupport<C> orderBy(AccessorChain<C, ?> getter, Order order) {
			return orderBy(getter, order, false);
		}
		
		public EntityQueryPageSupport<C> orderBy(AccessorChain<C, ?> getter, Order order, boolean ignoreCase) {
			orderBy.add(new OrderByItem(getter.getAccessors(), order, ignoreCase));
			getter.getAccessors().forEach(accessor -> assertAccessorIsNotIterable(accessor, AccessorDefinition.giveDefinition(accessor).getMemberType()));
			return this;
		}
		
		private void assertAccessorIsNotIterable(ValueAccessPoint valueAccessPoint, Class memberType) {
			if (Iterable.class.isAssignableFrom(memberType)) {
				throw new IllegalArgumentException("OrderBy clause on a Collection property is unsupported due to eventual inconsistency"
						+ " with Collection nature : "
						+ (valueAccessPoint instanceof AbstractReflector
							? ((AbstractReflector<?>) valueAccessPoint).getDescription()
							: AccessorDefinition.giveDefinition(valueAccessPoint)).toString());
			}
		}
		
		/**
		 * Creates a copy of this instance by merging its options with another.
		 * Made to handle Spring Data's different ways of sorting (should have been put closer to its usage, but was too complex) 
		 * 
		 * @param other some other paging options
		 * @return a merge of this instance with given one
		 */
		private EntityQueryPageSupport<C> merge(EntityQueryPageSupport<C> other) {
			EntityQueryPageSupport<C> duplicate = new EntityQueryPageSupport<>();
			// applying this instance's limit and orderBy options
			if (this.getLimit() != null) {
				duplicate.limit(this.getLimit().getCount(), this.getLimit().getOffset());
			}
			duplicate.orderBy.addAll(this.orderBy);
			// adding other instance's limit and orderBy options (may overwrite info, but that's user responsibility, we can't do anything smart here)
			if (other.getLimit() != null) {
				duplicate.limit(other.getLimit().getCount(), other.getLimit().getOffset());
			}
			duplicate.orderBy.addAll(other.orderBy);
			return duplicate;
		}
		
		public static class OrderByItem {
			
			private final List<? extends ValueAccessPoint<?>> property;
			private final Order direction;
			private final boolean ignoreCase;
			
			public OrderByItem(List<? extends ValueAccessPoint<?>> property, Order direction, boolean ignoreCase) {
				this.property = property;
				this.direction = direction;
				this.ignoreCase = ignoreCase;
			}
			
			public List<? extends ValueAccessPoint<?>> getProperty() {
				return property;
			}
			
			public Order getDirection() {
				return direction;
			}
			
			public boolean isIgnoreCase() {
				return ignoreCase;
			}
			
			public AccessorChain<Object, Comparable> propertyAsAccessorChain() {
				AccessorChain<Object, Comparable> result;
				if (property.size() == 1) {
					ValueAccessPoint<?> valueAccessPoint = property.get(0);
					if (valueAccessPoint instanceof Accessor) {
						result = new AccessorChain<>((Accessor<?, Comparable>) valueAccessPoint);
					} else {
						AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(valueAccessPoint);
						result = new AccessorChain<>(Accessors.accessor(accessorDefinition.getDeclaringClass(), accessorDefinition.getName(), accessorDefinition.getMemberType()));
					}
				} else {
					result = new AccessorChain<>(property.stream().map(EntityQueryCriteriaSupport::toAccessor).collect(Collectors.toList()));
				}
				result.setNullValueHandler(AccessorChain.RETURN_NULL);
				return result;
			}
		}
	}
}
