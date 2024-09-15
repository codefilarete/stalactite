package org.codefilarete.stalactite.engine.runtime;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import org.codefilarete.stalactite.engine.EntityPersister.LimitAware;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain;
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister.ExecutableEntityQueryCriteria;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.EntitySelector;
import org.codefilarete.stalactite.query.RelationalEntityCriteria;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.ThrowingExecutable;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static java.util.Collections.emptySet;
import static org.codefilarete.tool.Nullable.nullable;

/**
 * <ul>
 * Class aimed at handling entity query configuration and execution triggering :
 * <li>query configuration will be done by redirecting {@link CriteriaChain} methods to an {@link EntityQueryCriteriaSupport}.</li>
 * <li>execution triggering calls {@link EntitySelector#select(ConfiguredEntityCriteria, Consumer, Consumer)}
 * and wraps it into {@link PersisterListenerCollection#doWithSelectListener(Iterable, ThrowingExecutable)}</li>
 * </ul>
 * 
 * @param <C> entity type
 * @param <I> identifier type
 * @author Guillaume Mary
 */
public class EntityQueryCriteriaSupport<C, I> {
	
	private final EntityCriteriaSupport<C> entityCriteriaSupport;
	
	/** Support for {@link EntityCriteria} query execution */
	private final EntitySelector<C, I> entitySelector;
	
	private final PersisterListenerCollection<C, I> persisterListener;
	
	EntityQueryCriteriaSupport(EntityCriteriaSupport<C> source, EntitySelector<C, I> entitySelector, PersisterListenerCollection<C, I> persisterListener) {
		this.entityCriteriaSupport = new EntityCriteriaSupport<>(source);
		this.entitySelector = entitySelector;
		this.persisterListener = persisterListener;
	}
	
	public ExecutableEntityQueryCriteria<C> wrapIntoExecutable() {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		ExecutableEntityQuerySupport<C> querySugarSupport = new ExecutableEntityQuerySupport<>();
		return methodDispatcher
				.redirect((SerializableBiFunction<ExecutableQuery<C>, Accumulator<C, Set<C>, Object>, Object>) ExecutableQuery::execute,
						wrapGraphLoad(entityCriteriaSupport, querySugarSupport))
				.redirect(OrderByChain.class, querySugarSupport)
				.redirect(LimitAware.class, querySugarSupport)
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
	private interface ConfiguredExecutableEntityQueryCriteria<C> extends ConfiguredEntityCriteria, ExecutableEntityQueryCriteria<C> {
		
	}
	
	private <R> Function<Accumulator<C, Set<C>, R>, R> wrapGraphLoad(EntityCriteriaSupport<C> localCriteriaSupport, ExecutableEntityQuerySupport<C> querySugarSupport) {
		return (Accumulator<C, Set<C>, R> accumulator) -> {
			if (querySugarSupport.getLimit() != null) {
				if (entityCriteriaSupport.getRootConfiguration().hasCollectionProperty()) {
					throw new UnsupportedOperationException("Can't limit query when entity graph contains Collection relations");
				}
			}
			Set<C> result = persisterListener.doWithSelectListener(emptySet(), () ->
					entitySelector.select(
							localCriteriaSupport,
							orderByClause -> {
								KeepOrderSet<Duo<List<? extends ValueAccessPoint<?>>, Order>> orderBy = querySugarSupport.orderBy;
								orderBy.forEach(duo -> {
									Column column = localCriteriaSupport.getRootConfiguration().giveColumn(duo.getLeft());
									orderByClause.add(column, duo.getRight() == Order.ASC ? org.codefilarete.stalactite.query.model.OrderByChain.Order.ASC : org.codefilarete.stalactite.query.model.OrderByChain.Order.DESC);
								});
							},
							limitAware -> nullable(querySugarSupport.getLimit()).invoke(limitAware::limit))
			);
			if (!querySugarSupport.orderBy.isEmpty()) {
				// Note that we must wrap this creation in an if statement due to that entities don't implement Comparable, we avoid a
				// ClassCastException of the addAll(..) operation
				TreeSet<C> sortedResult = new TreeSet<>(buildComparator(querySugarSupport.orderBy));
				sortedResult.addAll(result);
				return accumulator.collect(sortedResult);
			} else {
				return accumulator.collect(result);
			}
		};
	}
	
	@VisibleForTesting
	static <C> Comparator<C> buildComparator(KeepOrderSet<Duo<List<? extends ValueAccessPoint<?>>, Order>> orderBy) {
		List<Duo<AccessorChain<Object, Comparable>, Order>> orderByAccessors = orderBy.stream().map(duo -> {
			List<? extends ValueAccessPoint<?>> valueAccessPoints = duo.getLeft();
			AccessorChain<Object, Comparable> localResult;
			if (valueAccessPoints.size() == 1) {
				ValueAccessPoint<?> valueAccessPoint = valueAccessPoints.get(0);
				if (valueAccessPoint instanceof Accessor) {
					localResult = new AccessorChain<>((Accessor<?, Comparable>) valueAccessPoint);
				} else {
					AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(valueAccessPoint);
					localResult = new AccessorChain<>(Accessors.accessor(accessorDefinition.getDeclaringClass(), accessorDefinition.getName(), accessorDefinition.getMemberType()));
				}
			} else {
				localResult = new AccessorChain<>(valueAccessPoints.stream().map(EntityQueryCriteriaSupport::toAccessor).collect(Collectors.toList()));
			}
			localResult.setNullValueHandler(AccessorChain.RETURN_NULL);
			return new Duo<>(localResult, duo.getRight());
		}).collect(Collectors.toList());
		Nullable<Comparator> result = nullable((Comparator) null);
		orderByAccessors.forEach(orderByPawn -> {
			Comparator comparator = Comparator.comparing(orderByPawn.getLeft()::get, Comparator.nullsLast(Comparator.naturalOrder()));
			if (orderByPawn.getRight() == Order.DESC) {
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
	 * @return given {@link ValueAccessPoint} as an {@link Accessor}
	 * @param <C> declaring class type
	 * @param <T> property type
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
	 * Simple class that stores options of the query
	 * @author Guillaume Mary
	 */
	private static class ExecutableEntityQuerySupport<C>
			implements OrderByChain<C, ExecutableEntityQuerySupport<C>>, LimitAware<ExecutableEntityQuerySupport<C>> {
		
		private Integer limit;
		private final KeepOrderSet<Duo<List<? extends ValueAccessPoint<?>>, Order>> orderBy = new KeepOrderSet<>();
		
		public Integer getLimit() {
			return limit;
		}
		
		@Override
		public ExecutableEntityQuerySupport<C> limit(int count) {
			limit = count;
			return this;
		}
		
		@Override
		public ExecutableEntityQuerySupport<C> orderBy(SerializableFunction<C, ?> getter, Order order) {
			orderBy.add(new Duo<>(Arrays.asList(new AccessorByMethodReference<>(getter)), order));
			return this;
		}
		
		@Override
		public ExecutableEntityQuerySupport<C> orderBy(SerializableBiConsumer<C, ?> setter, Order order) {
			orderBy.add(new Duo<>(Arrays.asList(new MutatorByMethodReference<>(setter)), order));
			return this;
		}
		
		@Override
		public ExecutableEntityQuerySupport<C> orderBy(AccessorChain<C, ?> getter, Order order) {
			orderBy.add(new Duo<>(getter.getAccessors(), order));
			return this;
		}
	}	
}
