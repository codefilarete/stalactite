package org.codefilarete.stalactite.engine.runtime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.AbstractReflector;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityCriteria;
import org.codefilarete.stalactite.engine.EntityCriteria.CriteriaPath;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.engine.EntityCriteria.LimitAware;
import org.codefilarete.stalactite.engine.EntityCriteria.OrderByChain;
import org.codefilarete.stalactite.engine.EntityCriteria.OrderByChain.Order;
import org.codefilarete.stalactite.engine.ExecutableProjection;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * <ul>
 * Class aimed at handling projection query configuration and execution triggering :
 * <li>query configuration will be done by redirecting {@link CriteriaChain} methods to an {@link EntityQueryCriteriaSupport}.</li>
 * <li>execution triggering calls {@link EntityFinder#selectProjection(Consumer, Map, Accumulator, ConfiguredEntityCriteria, boolean, OrderBy, Limit)}</li>
 * </ul>
 *
 * @param <C> entity type
 * @param <I> identifier type
 * @author Guillaume Mary
 */
public class ProjectionQueryCriteriaSupport<C, I> {
	
	
	/** Support for {@link EntityCriteria} query execution */
	private final EntityFinder<C, I> entityFinder;
	
	private final EntityCriteriaSupport<C> entityCriteriaSupport;
	
	private final ProjectionQueryPageSupport<C> queryPageSupport;
	
	private Consumer<Select> selectAdapter;
	
	public ProjectionQueryCriteriaSupport(EntityFinder<C, I> entityFinder, Consumer<Select> selectAdapter) {
		this(entityFinder, entityFinder.newCriteriaSupport().getEntityCriteriaSupport(), new ProjectionQueryPageSupport<>(), selectAdapter);
	}
	
	public ProjectionQueryCriteriaSupport(EntityFinder<C, I> entityFinder, EntityCriteriaSupport<C> entityCriteriaSupport, Consumer<Select> selectAdapter) {
		this(entityFinder, entityCriteriaSupport, new ProjectionQueryPageSupport<>(), selectAdapter);
	}
	
	public ProjectionQueryCriteriaSupport(EntityFinder<C, I> entityFinder,
										  EntityCriteriaSupport<C> entityCriteriaSupport,
										  ProjectionQueryPageSupport<C> queryPageSupport,
										  Consumer<Select> selectAdapter) {
		this.entityFinder = entityFinder;
		this.entityCriteriaSupport = entityCriteriaSupport;
		this.queryPageSupport = queryPageSupport;
		this.selectAdapter = selectAdapter;
	}
	
	/**
	 * Makes a copy of this instance merged with given one
	 * Made to handle Spring Data's different ways of sorting (should have been put closer to its usage, but was too complex)
	 *
	 * @param otherPageSupport some other paging options
	 * @return a merge of this instance with given page options
	 */
	public ProjectionQueryCriteriaSupport<C, I> copyFor(ProjectionQueryPageSupport<C> otherPageSupport) {
		return new ProjectionQueryCriteriaSupport<>(entityFinder, entityCriteriaSupport, queryPageSupport.merge(otherPageSupport), this.selectAdapter);
	}
	
	public ProjectionQueryCriteriaSupport<C, I> copyFor(Consumer<Select> selectAdapter) {
		return new ProjectionQueryCriteriaSupport<>(entityFinder, entityCriteriaSupport, queryPageSupport, selectAdapter);
	}
	
	public EntityCriteriaSupport<C> getEntityCriteriaSupport() {
		return entityCriteriaSupport;
	}
	
	public ProjectionQueryPageSupport<C> getQueryPageSupport() {
		return queryPageSupport;
	}
	
	public ExecutableProjectionQuery<C, ?> wrapIntoExecutable() {
		Map<String, Object> values = new HashMap<>();
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		return methodDispatcher
				.redirect((SerializableBiFunction<ExecutableProjection, Accumulator<? super Function<? extends Selectable, Object>, Object, Object>, Object>) ExecutableProjection::execute,
						wrapProjectionLoad(values))
				.redirect((SerializableTriFunction<ExecutableProjectionQuery<?, ?>, String, Object, Object>) ExecutableProjectionQuery::set,
						// Don't use "values::put" because its signature returns previous value, which means it is a Function
						// and dispatch to redirect(..) that takes a Function as argument, which, at runtime,
						// will create some ClassCastException due to incompatible type between ExecutableEntityQuery
						// and values contained in the Map (because ExecutableEntityQuery::set returns ExecutableEntityQuery)
						(s, object) -> { values.put(s, object); }
				)
				.redirect(OrderByChain.class, queryPageSupport, true)
				.redirect(LimitAware.class, queryPageSupport, true)
				.redirect((SerializableFunction<ExecutableProjection, ExecutableProjection>) ExecutableProjection::distinct, queryPageSupport::distinct)
				.redirect((SerializableBiConsumer<ExecutableProjection, Consumer<Select>>) ExecutableProjection::selectInspector,
						selectAdapter -> {
					this.selectAdapter = this.selectAdapter.andThen(select -> selectAdapter.accept(new Select(select)));
				})
				.redirect(EntityCriteria.class, entityCriteriaSupport, true)
				.build((Class<ExecutableProjectionQuery<C, ?>>) (Class) ExecutableProjectionQuery.class);
	}
	
	private <R> Function<Accumulator<? super Function<? extends Selectable, Object>, Object, R>, R> wrapProjectionLoad(
			Map<String, Object> values) {
		return (Accumulator<? super Function<? extends Selectable, Object>, Object, R> accumulator) -> {
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
			
		return entityFinder.selectProjection(selectAdapter, values, accumulator, entityCriteriaSupport, queryPageSupport.isDistinct(),
					orderBy,
					queryPageSupport.getLimit());
		};
	}
	
	/**
	 * Simple class that stores options of the query
	 * @author Guillaume Mary
	 */
	public static class ProjectionQueryPageSupport<C>
			implements OrderByChain<C, ProjectionQueryPageSupport<C>>, LimitAware<ProjectionQueryPageSupport<C>> {
		
		private boolean distinct;
		private Limit limit;
		private final KeepOrderSet<OrderByItem> orderBy = new KeepOrderSet<>();
		
		
		public boolean isDistinct() {
			return distinct;
		}
		
		void distinct() {
			distinct = true;
		}
		
		public Limit getLimit() {
			return limit;
		}
		
		public KeepOrderSet<OrderByItem> getOrderBy() {
			return orderBy;
		}
		
		@Override
		public ProjectionQueryPageSupport<C> limit(int count) {
			limit = new Limit(count);
			return this;
		}
		
		@Override
		public ProjectionQueryPageSupport<C> limit(int count, Integer offset) {
			limit = new Limit(count, offset);
			return this;
		}
		
		@Override
		public ProjectionQueryPageSupport<C> orderBy(SerializableFunction<C, ?> getter, Order order) {
			orderBy.add(new OrderByItem(Arrays.asList(new AccessorByMethodReference<>(getter)), order, false));
			return this;
		}
		
		@Override
		public ProjectionQueryPageSupport<C> orderBy(SerializableBiConsumer<C, ?> setter, Order order) {
			orderBy.add(new OrderByItem(Arrays.asList(new MutatorByMethodReference<>(setter)), order, false));
			return this;
		}
		
		@Override
		public ProjectionQueryPageSupport<C> orderBy(AccessorChain<C, ?> getter, Order order) {
			orderBy.add(new OrderByItem(getter.getAccessors(), order, false));
			return this;
		}
		
		@Override
		public ProjectionQueryPageSupport<C> orderBy(AccessorChain<C, ?> getter, Order order, boolean ignoreCase) {
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
		private ProjectionQueryPageSupport<C> merge(ProjectionQueryPageSupport<C> other) {
			ProjectionQueryPageSupport<C> duplicate = new ProjectionQueryPageSupport<>();
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
		}
	}
}
