package org.codefilarete.stalactite.engine.runtime;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.engine.EntityPersister.LimitAware;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
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
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * <ul>
 * Class aimed at handling projection query configuration and execution triggering :
 * <li>query configuration will be done by redirecting {@link CriteriaChain} methods to an {@link EntityQueryCriteriaSupport}.</li>
 * <li>execution triggering calls {@link EntityFinder#selectProjection(Consumer, Accumulator, ConfiguredEntityCriteria, boolean, OrderBy, Limit)}</li>
 * </ul>
 *
 * @param <C> entity type
 * @param <I> identifier type
 * @author Guillaume Mary
 */
public class ProjectionQueryCriteriaSupport<C, I> {
	
	private final EntityCriteriaSupport<C> entityCriteriaSupport;
	
	/** Support for {@link EntityCriteria} query execution */
	private final EntityFinder<C, I> entityFinder;
	
	private final Consumer<Select> selectAdapter;
	
	public ProjectionQueryCriteriaSupport(EntityFinder<C, I> entityFinder, Consumer<Select> selectAdapter) {
		this.entityFinder = entityFinder;
		this.entityCriteriaSupport = entityFinder.newCriteriaSupport().getEntityCriteriaSupport();
		this.selectAdapter = selectAdapter;
	}
	
	
	public ExecutableProjectionQuery<C, ?> wrapIntoExecutable() {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		ExecutableProjectionQuerySupport<C> querySugarSupport = new ExecutableProjectionQuerySupport<>();
		return methodDispatcher
				.redirect((SerializableBiFunction<ExecutableProjection, Accumulator<? super Function<? extends Selectable, Object>, Object, Object>, Object>) ExecutableProjection::execute,
						wrapProjectionLoad(selectAdapter, entityCriteriaSupport, querySugarSupport))
				.redirect(OrderByChain.class, querySugarSupport)
				.redirect(LimitAware.class, querySugarSupport)
				.redirect((SerializableFunction<ExecutableProjection, ExecutableProjection>) ExecutableProjection::distinct, querySugarSupport::distinct)
				.redirect(EntityCriteria.class, entityCriteriaSupport, true)
				.build((Class<ExecutableProjectionQuery<C, ?>>) (Class) ExecutableProjectionQuery.class);
	}
	
	private <R> Function<Accumulator<? super Function<? extends Selectable, Object>, Object, R>, R> wrapProjectionLoad(
			Consumer<Select> selectAdapter,
			EntityCriteriaSupport<C> localCriteriaSupport,
			ExecutableProjectionQuerySupport<C> querySugarSupport) {
		return (Accumulator<? super Function<? extends Selectable, Object>, Object, R> accumulator) -> {
			OrderBy orderBy = new OrderBy();
			querySugarSupport.getOrderBy().forEach(duo -> {
				Selectable column = entityCriteriaSupport.getAggregateColumnMapping().giveColumn(duo.getProperty());
				orderBy.add(
						duo.isIgnoreCase()
								? Operators.lowerCase(column)
								: column,
						duo.getDirection() == Order.ASC
								? org.codefilarete.stalactite.query.model.OrderByChain.Order.ASC
								: org.codefilarete.stalactite.query.model.OrderByChain.Order.DESC);
			});
			
			return entityFinder.selectProjection(selectAdapter, accumulator, localCriteriaSupport, querySugarSupport.isDistinct(),
					orderBy,
					querySugarSupport.getLimit());
		};
	}
	
	/**
	 * Simple class that stores options of the query
	 * @author Guillaume Mary
	 */
	private static class ExecutableProjectionQuerySupport<C>
			implements OrderByChain<C, ExecutableProjectionQuerySupport<C>>, LimitAware<ExecutableProjectionQuerySupport<C>> {
		
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
		public ExecutableProjectionQuerySupport<C> limit(int count) {
			limit = new Limit(count);
			return this;
		}
		
		@Override
		public ExecutableProjectionQuerySupport<C> limit(int count, Integer offset) {
			limit = new Limit(count, offset);
			return this;
		}
		
		@Override
		public ExecutableProjectionQuerySupport<C> orderBy(SerializableFunction<C, ?> getter, Order order) {
			orderBy.add(new OrderByItem(Arrays.asList(new AccessorByMethodReference<>(getter)), order, false));
			return this;
		}
		
		@Override
		public ExecutableProjectionQuerySupport<C> orderBy(SerializableBiConsumer<C, ?> setter, Order order) {
			orderBy.add(new OrderByItem(Arrays.asList(new MutatorByMethodReference<>(setter)), order, false));
			return this;
		}
		
		@Override
		public ExecutableProjectionQuerySupport<C> orderBy(AccessorChain<C, ?> getter, Order order) {
			orderBy.add(new OrderByItem(getter.getAccessors(), order, false));
			return this;
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
