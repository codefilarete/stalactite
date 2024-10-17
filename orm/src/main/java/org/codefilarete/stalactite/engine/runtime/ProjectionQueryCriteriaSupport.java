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
import org.codefilarete.stalactite.engine.ExecutableProjection;
import org.codefilarete.stalactite.query.EntitySelector;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * <ul>
 * Class aimed at handling projection query configuration and execution triggering :
 * <li>query configuration will be done by redirecting {@link CriteriaChain} methods to an {@link EntityQueryCriteriaSupport}.</li>
 * <li>execution triggering calls {@link EntitySelector#selectProjection(Consumer, Accumulator, CriteriaChain, boolean, Consumer, Consumer)}</li>
 * </ul>
 *
 * @param <C> entity type
 * @param <I> identifier type
 * @author Guillaume Mary
 */
public class ProjectionQueryCriteriaSupport<C, I> {
	
	private final EntityCriteriaSupport<C> entityCriteriaSupport;
	
	/** Support for {@link EntityCriteria} query execution */
	private final EntitySelector<C, I> entitySelector;
	
	private final Consumer<Select> selectAdapter;
	
	public ProjectionQueryCriteriaSupport(EntityCriteriaSupport<C> source, EntitySelector<C, I> entitySelector, Consumer<Select> selectAdapter) {
		this.entityCriteriaSupport = new EntityCriteriaSupport<>(source);
		this.entitySelector = entitySelector;
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
		return (Accumulator<? super Function<? extends Selectable, Object>, Object, R> accumulator) ->
				entitySelector.selectProjection(selectAdapter, accumulator, localCriteriaSupport.getCriteria(), querySugarSupport.isDistinct(),
						orderByClause -> {},
						limitAware -> nullable(querySugarSupport.getLimit()).invoke(limit -> limitAware.limit(limit.getCount(), limit.getOffset())));
	}
	
	/**
	 * Simple class that stores options of the query
	 * @author Guillaume Mary
	 */
	private static class ExecutableProjectionQuerySupport<C>
			implements OrderByChain<C, ExecutableProjectionQuerySupport<C>>, LimitAware<ExecutableProjectionQuerySupport<C>> {
		
		private boolean distinct;
		private Limit limit;
		
		public boolean isDistinct() {
			return distinct;
		}
		
		void distinct() {
			distinct = true;
		}
		
		private final KeepOrderSet<Duo<List<? extends ValueAccessPoint<?>>, Order>> orderBy = new KeepOrderSet<>();
		
		public Limit getLimit() {
			return limit;
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
}
