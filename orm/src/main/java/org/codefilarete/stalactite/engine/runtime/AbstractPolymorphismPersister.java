package org.codefilarete.stalactite.engine.runtime;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.ExecutableProjection;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.EntitySelector;
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
	protected final EntitySelector<C, I> entitySelector;
	
	protected AbstractPolymorphismPersister(ConfiguredRelationalPersister<C, I> mainPersister,
											Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> subEntitiesPersisters,
											EntitySelector<C, I> entitySelector) {
		this.mainPersister = mainPersister;
		this.subEntitiesPersisters = (Map<Class<C>, ConfiguredRelationalPersister<C, I>>) subEntitiesPersisters;
		this.criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMapping());
		this.entitySelector = entitySelector;
	}
	
	@Override
	public ExecutableEntityQueryCriteria<C> selectWhere() {
		EntityQueryCriteriaSupport<C, I> support = new EntityQueryCriteriaSupport<>(criteriaSupport, entitySelector, getPersisterListener());
		return support.wrapIntoExecutable();
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
				entitySelector.selectProjection(selectAdapter, accumulator, localCriteriaSupport.getCriteria(),
						querySugarSupport.isDistinct(),
						orderByClause -> {},
						limitAware -> nullable(querySugarSupport.getLimit()).invoke(limitAware::limit));
	}
	
	@Override
	public Set<C> selectAll() {
		return entitySelector.select(newWhere(), fluentOrderByClause -> {}, limitAware -> {});
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
}
