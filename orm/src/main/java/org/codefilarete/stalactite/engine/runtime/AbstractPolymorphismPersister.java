package org.codefilarete.stalactite.engine.runtime;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
import org.codefilarete.stalactite.engine.EntityPersister.OrderByChain.Order;
import org.codefilarete.stalactite.engine.ExecutableProjection;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.EntitySelector;
import org.codefilarete.stalactite.query.RelationalEntityCriteria;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static java.util.Collections.emptySet;
import static org.codefilarete.tool.Nullable.nullable;

/**
 * Parent class of polymorphic persisters, made to share common code.
 * 
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public abstract class AbstractPolymorphismPersister<C, I> implements ConfiguredRelationalPersister<C, I>, PolymorphicPersister<C> {
	
	protected final Map<Class<C>, ConfiguredRelationalPersister<C, I>> subEntitiesPersisters;
	protected final ConfiguredRelationalPersister<C, I> mainPersister;
	protected final EntityCriteriaSupport<C> criteriaSupport;
	protected final EntitySelector<C, I> entitySelectExecutor;
	
	protected AbstractPolymorphismPersister(ConfiguredRelationalPersister<C, I> mainPersister,
											Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> subEntitiesPersisters,
											EntitySelector<C, I> entitySelectExecutor) {
		this.mainPersister = mainPersister;
		this.subEntitiesPersisters = (Map<Class<C>, ConfiguredRelationalPersister<C, I>>) subEntitiesPersisters;
		this.criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMapping());
		this.entitySelectExecutor = entitySelectExecutor;
	}
	
	@Override
	public ExecutableEntityQueryCriteria<C> selectWhere() {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	private ExecutableEntityQueryCriteria<C> wrapIntoExecutable(EntityCriteriaSupport<C> localCriteriaSupport) {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		ExecutableEntityQuerySupport<C> querySugarSupport = new ExecutableEntityQuerySupport<>();
		return methodDispatcher
				.redirect((SerializableBiFunction<ExecutableQuery<C>, Accumulator<C, Set<C>, Object>, Object>) ExecutableQuery::execute,
						wrapGraphLoad(localCriteriaSupport, querySugarSupport))
				.redirect(OrderByChain.class, querySugarSupport)
				.redirect(LimitAware.class, querySugarSupport)
				.redirect(RelationalEntityCriteria.class, localCriteriaSupport, true)
				// making an exception for 2 of the methods that can't return the proxy
				.redirect((SerializableFunction<ConfiguredEntityCriteria, CriteriaChain>) ConfiguredEntityCriteria::getCriteria, localCriteriaSupport::getCriteria)
				.redirect((SerializableFunction<ConfiguredEntityCriteria, Boolean>) ConfiguredEntityCriteria::hasCollectionCriteria, localCriteriaSupport::hasCollectionCriteria)
				.build((Class<ConfiguredExecutableEntityCriteria<C>>) (Class) ConfiguredExecutableEntityCriteria.class);
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
	private interface ConfiguredExecutableEntityCriteria<C> extends ConfiguredEntityCriteria, ExecutableEntityQueryCriteria<C> {
		
	}
	
	private <R> Function<Accumulator<C, Set<C>, R>, R> wrapGraphLoad(EntityCriteriaSupport<C> localCriteriaSupport, ExecutableEntityQuerySupport<C> querySugarSupport) {
		return (Accumulator<C, Set<C>, R> accumulator) -> {
			if (querySugarSupport.getLimit() != null) {
				if (criteriaSupport.getRootConfiguration().hasCollectionProperty()) {
					throw new UnsupportedOperationException("Can't limit query when entity graph contains Collection relations");
				}
			}
			Set<C> result = getPersisterListener().doWithSelectListener(emptySet(), () ->
					entitySelectExecutor.select(
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
				localResult = new AccessorChain<>(valueAccessPoints.stream().map(AbstractPolymorphismPersister::toAccessor).collect(Collectors.toList()));
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
	
	
	private EntityCriteriaSupport<C> newWhere() {
		// we must clone the underlying support, else it would be modified for all subsequent invocations and criteria will aggregate
		return new EntityCriteriaSupport<>(criteriaSupport);
	}
	
	@Override
	public ExecutableProjectionQuery<C> selectProjectionWhere(Consumer<Select> selectAdapter) {
		return wrapIntoExecutable(selectAdapter, newWhere());
	}
	
	private ExecutableProjectionQuery<C> wrapIntoExecutable(Consumer<Select> selectAdapter,
															EntityCriteriaSupport<C> localCriteriaSupport) {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		ExecutableProjectionQuerySupport<C> querySugarSupport = new ExecutableProjectionQuerySupport<>();
		return methodDispatcher
				.redirect((SerializableBiFunction<ExecutableProjection, Accumulator<? super Function<? extends Selectable, Object>, Object, Object>, Object>) ExecutableProjection::execute,
						wrapProjectionLoad(selectAdapter, localCriteriaSupport, querySugarSupport))
				.redirect((SerializableFunction<ExecutableProjection, ExecutableProjection>) ExecutableProjection::distinct, querySugarSupport::distinct)
				.redirect(OrderByChain.class, querySugarSupport)
				.redirect(LimitAware.class, querySugarSupport)
				.redirect(EntityCriteria.class, localCriteriaSupport, true)
				.build((Class<ExecutableProjectionQuery<C>>) (Class) ExecutableProjectionQuery.class);
	}
	
	private <R> Function<Accumulator<? super Function<? extends Selectable, Object>, Object, R>, R> wrapProjectionLoad(
			Consumer<Select> selectAdapter,
			EntityCriteriaSupport<C> localCriteriaSupport,
			ExecutableProjectionQuerySupport<C> querySugarSupport) {
		return (Accumulator<? super Function<? extends Selectable, Object>, Object, R> accumulator) ->
				entitySelectExecutor.selectProjection(selectAdapter, accumulator, localCriteriaSupport.getCriteria(),
						querySugarSupport.isDistinct(),
						orderByClause -> {},
						limitAware -> nullable(querySugarSupport.getLimit()).invoke(limitAware::limit));
	}
	
	@Override
	public Set<C> selectAll() {
		return entitySelectExecutor.select(newWhere(), fluentOrderByClause -> {}, limitAware -> {});
	}
	
	@Override
	public boolean isNew(C entity) {
		return mainPersister.isNew(entity);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return mainPersister.getClassToPersist();
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return mainPersister.getEntityJoinTree();
	}
	
	/**
	 * Simple class that stores options of the query
	 * @author Guillaume Mary
	 */
	private static class ExecutableProjectionQuerySupport<C>
			implements OrderByChain<C, ExecutableProjectionQuerySupport<C>>, LimitAware<ExecutableProjectionQuerySupport<C>> {
		
		private boolean distinct;
		private Integer limit;
		
		public boolean isDistinct() {
			return distinct;
		}
		
		void distinct() {
			distinct = true;
		}
		
		private final KeepOrderSet<Duo<List<? extends ValueAccessPoint<?>>, Order>> orderBy = new KeepOrderSet<>();
		
		public Integer getLimit() {
			return limit;
		}
		
		@Override
		public ExecutableProjectionQuerySupport<C> limit(int count) {
			limit = count;
			return this;
		}
		
		@Override
		public ExecutableProjectionQuerySupport<C> orderBy(SerializableFunction<C, ?> getter, Order order) {
			orderBy.add(new Duo<>(Arrays.asList(new AccessorByMethodReference<>(getter)), order));
			return this;
		}
		
		@Override
		public ExecutableProjectionQuerySupport<C> orderBy(SerializableBiConsumer<C, ?> setter, Order order) {
			orderBy.add(new Duo<>(Arrays.asList(new MutatorByMethodReference<>(setter)), order));
			return this;
		}
		
		@Override
		public ExecutableProjectionQuerySupport<C> orderBy(AccessorChain<C, ?> getter, Order order) {
			orderBy.add(new Duo<>(getter.getAccessors(), order));
			return this;
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
