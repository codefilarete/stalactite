package org.codefilarete.stalactite.engine.runtime;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.MethodReferenceDispatcher;
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
import org.codefilarete.stalactite.sql.result.Accumulator;
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
		return methodDispatcher
				.redirect((SerializableBiFunction<ExecutableQuery<C>, Accumulator<C, Set<C>, Object>, Object>) ExecutableQuery::execute,
						wrapGraphLoad(localCriteriaSupport))
				.redirect(RelationalEntityCriteria.class, localCriteriaSupport, true)
				// making an exception for 2 of the methods that can't return the proxy
				.redirect((SerializableFunction<ConfiguredEntityCriteria, CriteriaChain>) ConfiguredEntityCriteria::getCriteria, localCriteriaSupport::getCriteria)
				.redirect((SerializableFunction<ConfiguredEntityCriteria, Boolean>) ConfiguredEntityCriteria::hasCollectionCriteria, localCriteriaSupport::hasCollectionCriteria)
				.build((Class<ConfiguredExecutableEntityCriteria<C>>) (Class) ConfiguredExecutableEntityCriteria.class);
	}
	
	/**
	 * A mashup to let redirect all {@link ExecutableEntityQueryCriteria} methods being redirected to {@link EntityCriteriaSupport} while redirecting
	 * {@link ConfiguredEntityCriteria} methods to some specific methods of {@link EntityCriteriaSupport}.
	 * Made as such to avoid to expose internal / implementation methods "getCriteria" and "hasCollectionCriteria" to the
	 * configuration API ({@link ExecutableEntityQueryCriteria})
	 * 
	 * @param <C>
	 * @author Guillaume Mary
	 */
	private interface ConfiguredExecutableEntityCriteria<C> extends ConfiguredEntityCriteria, ExecutableEntityQueryCriteria<C> {
		
	}
	
	private <R> Function<Accumulator<C, Set<C>, R>, R> wrapGraphLoad(EntityCriteriaSupport<C> localCriteriaSupport) {
		return (Accumulator<C, Set<C>, R> accumulator) -> {
			Set<C> result = getPersisterListener().doWithSelectListener(emptySet(), () ->
					entitySelectExecutor.select(localCriteriaSupport)
			);
			return accumulator.collect(result);
		};
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
		ExecutableProjectionQuerySupport querySugarSupport = new ExecutableProjectionQuerySupport();
		return methodDispatcher
				.redirect((SerializableBiFunction<ExecutableProjection, Accumulator<? super Function<? extends Selectable, Object>, Object, Object>, Object>) ExecutableProjection::execute,
						wrapProjectionLoad(selectAdapter, localCriteriaSupport, querySugarSupport))
				.redirect((SerializableFunction<ExecutableProjection, ExecutableProjection>) ExecutableProjection::distinct, querySugarSupport::distinct)
				.redirect((SerializableBiFunction<ExecutableProjection, Integer, ExecutableProjection>) ExecutableProjection::limit, querySugarSupport::limit)
				.redirect(EntityCriteria.class, localCriteriaSupport, true)
				.build((Class<ExecutableProjectionQuery<C>>) (Class) ExecutableProjectionQuery.class);
	}
	
	private <R> Function<Accumulator<? super Function<? extends Selectable, Object>, Object, R>, R> wrapProjectionLoad(
			Consumer<Select> selectAdapter,
			EntityCriteriaSupport<C> localCriteriaSupport,
			ExecutableProjectionQuerySupport querySugarSupport) {
		return (Accumulator<? super Function<? extends Selectable, Object>, Object, R> accumulator) ->
				entitySelectExecutor.selectProjection(selectAdapter, accumulator, localCriteriaSupport.getCriteria(),
						querySugarSupport.isDistinct(),
						fluentOrderByClause -> nullable(querySugarSupport.getLimit()).invoke(fluentOrderByClause::limit));
	}
	
	@Override
	public Set<C> selectAll() {
		return entitySelectExecutor.select(newWhere());
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
	private static class ExecutableProjectionQuerySupport {
		
		private boolean distinct;
		private Integer limit;
		
		public boolean isDistinct() {
			return distinct;
		}
		
		void distinct() {
			distinct = true;
		}
		
		public Integer getLimit() {
			return limit;
		}
		
		void limit(int count) {
			limit = count;
		}
	} 
}
