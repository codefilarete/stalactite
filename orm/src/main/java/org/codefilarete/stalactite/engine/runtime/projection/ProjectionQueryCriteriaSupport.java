package org.codefilarete.stalactite.engine.runtime.projection;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.engine.EntityCriteria;
import org.codefilarete.stalactite.engine.EntityCriteria.CriteriaPath;
import org.codefilarete.stalactite.engine.EntityCriteria.LimitAware;
import org.codefilarete.stalactite.engine.EntityCriteria.OrderByChain;
import org.codefilarete.stalactite.engine.EntityCriteria.OrderByChain.Order;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.engine.EntityPersister.SelectAdapter;
import org.codefilarete.stalactite.engine.ExecutableProjection;
import org.codefilarete.stalactite.engine.ExecutableProjection.ProjectionDataProvider;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Accumulator;
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
	
	private Consumer<SelectAdapter<C>> selectAdapter;
	
	public ProjectionQueryCriteriaSupport(EntityFinder<C, I> entityFinder, Consumer<SelectAdapter<C>> selectAdapter) {
		this(entityFinder, entityFinder.newCriteriaSupport().getEntityCriteriaSupport(), new ProjectionQueryPageSupport<>(), selectAdapter);
	}
	
	public ProjectionQueryCriteriaSupport(EntityFinder<C, I> entityFinder, EntityCriteriaSupport<C> entityCriteriaSupport, Consumer<SelectAdapter<C>> selectAdapter) {
		this(entityFinder, entityCriteriaSupport, new ProjectionQueryPageSupport<>(), selectAdapter);
	}
	
	public ProjectionQueryCriteriaSupport(EntityFinder<C, I> entityFinder,
										  EntityCriteriaSupport<C> entityCriteriaSupport,
										  ProjectionQueryPageSupport<C> queryPageSupport,
										  Consumer<SelectAdapter<C>> selectAdapter) {
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
	
	public ProjectionQueryCriteriaSupport<C, I> copyFor(Consumer<SelectAdapter<C>> selectAdapter) {
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
				.redirect((SerializableBiFunction<ExecutableProjection, Accumulator<? super ProjectionDataProvider, Object, Object>, Object>) ExecutableProjection::execute,
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
				.redirect((SerializableBiConsumer<ExecutableProjection, Consumer<Set<Selectable<?>>>>) ExecutableProjection::selectInspector,
						selectInspector -> {
					this.selectAdapter = this.selectAdapter.andThen(selectAdapter -> selectInspector.accept(selectAdapter.getColumns()));
				})
				.redirect(EntityCriteria.class, entityCriteriaSupport, true)
				.build((Class<ExecutableProjectionQuery<C, ?>>) (Class) ExecutableProjectionQuery.class);
	}
	
	private <R> Function<Accumulator<? super ProjectionDataProvider, Object, R>, R> wrapProjectionLoad(
			Map<String, Object> values) {
		return (Accumulator<? super ProjectionDataProvider, Object, R> projectionDataProvider) -> {
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
			
			// creating an Accumulator that wraps the given one (projectionDataProvider) to plug the ProjectionDataProvider
			// onto the Function<Selectable<Object>, ?> that is consumed by the entityFinder.selectProjection(..) method
			Accumulator<Function<Selectable<Object>, ?>, Object, R> accumulator = new Accumulator<Function<Selectable<Object>, ?>, Object, R>() {
				@Override
				public Supplier<Object> supplier() {
					return projectionDataProvider.supplier();
				}
				
				@Override
				public BiConsumer<Object, Function<Selectable<Object>, ?>> aggregator() {
					return (o, selectableFunction) -> projectionDataProvider.aggregator().accept(o, new ProjectionDataProvider() {
						@Override
						public <O> O getValue(Selectable<O> selectable) {
							return (O) selectableFunction.apply((Selectable<Object>) selectable);
						}
						
						@Override
						public <O> O getValue(CriteriaPath<?, O> selectable) {
							return (O) selectableFunction.apply((Selectable<Object>) entityCriteriaSupport.getAggregateColumnMapping().giveColumn(selectable.getAccessors()));
						}
					});
				}
				
				@Override
				public Function<Object, R> finisher() {
					return projectionDataProvider.finisher();
				}
			};
			// we make an object that applies the SelectAdapter to the Select, this avoid to expose the Select class to end user
			Consumer<Select> selectConsumer = select -> ProjectionQueryCriteriaSupport.this.selectAdapter.accept(new SelectAdapterSupport<>(select, entityCriteriaSupport.getAggregateColumnMapping()));
			return entityFinder.selectProjection(
					selectConsumer,
					values,
					accumulator,
					entityCriteriaSupport,
					queryPageSupport.isDistinct(),
					orderBy,
					queryPageSupport.getLimit());
		};
	}
	
}
