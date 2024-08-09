package org.codefilarete.stalactite.engine.runtime;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.engine.ExecutableProjection;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister.CriteriaProvider;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.EntitySelector;
import org.codefilarete.stalactite.query.RelationalEntityCriteria;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static java.util.Collections.emptySet;

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
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(getter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	@Override
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(setter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(AccessorChain<C, O> accessorChain, ConditionalOperator<O, ?> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(accessorChain, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	private RelationalExecutableEntityQuery<C> wrapIntoExecutable(EntityCriteriaSupport<C> localCriteriaSupport) {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		return methodDispatcher
				.redirect((SerializableBiFunction<ExecutableQuery<C>, Accumulator<C, Set<C>, Object>, Object>) ExecutableQuery::execute,
						wrapGraphLoad(localCriteriaSupport))
				.redirect(CriteriaProvider::getCriteria, localCriteriaSupport::getCriteria)
				.redirect(RelationalEntityCriteria.class, localCriteriaSupport, true)
				.build((Class<RelationalExecutableEntityQuery<C>>) (Class) RelationalExecutableEntityQuery.class);
	}
	
	private <R> Function<Accumulator<C, Set<C>, R>, R> wrapGraphLoad(EntityCriteriaSupport<C> localCriteriaSupport) {
		return (Accumulator<C, Set<C>, R> accumulator) -> {
			Set<C> result = getPersisterListener().doWithSelectListener(emptySet(), () ->
					entitySelectExecutor.select(localCriteriaSupport.getCriteria())
			);
			return accumulator.collect(result);
		};
	}
	
	private EntityCriteriaSupport<C> newWhere() {
		// we must clone the underlying support, else it would be modified for all subsequent invocations and criteria will aggregate
		return new EntityCriteriaSupport<>(criteriaSupport);
	}
	
	@Override
	public <O> ExecutableProjectionQuery<C> selectProjectionWhere(Consumer<Select> selectAdapter, SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		ProjectionCriteriaSupport<C> localCriteriaSupport = newProjectionWhere();
		localCriteriaSupport.and(getter, operator);
		return wrapIntoExecutable(selectAdapter, localCriteriaSupport);
	}
	
	@Override
	public <O> ExecutableProjectionQuery<C> selectProjectionWhere(Consumer<Select> selectAdapter, SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		ProjectionCriteriaSupport<C> localCriteriaSupport = newProjectionWhere();
		localCriteriaSupport.and(setter, operator);
		return wrapIntoExecutable(selectAdapter, localCriteriaSupport);
	}
	
	@Override
	public <O> ExecutableProjectionQuery<C> selectProjectionWhere(Consumer<Select> selectAdapter, AccessorChain<C, O> accessorChain, ConditionalOperator<O, ?> operator) {
		ProjectionCriteriaSupport<C> localCriteriaSupport = newProjectionWhere();
		localCriteriaSupport.and(accessorChain, operator);
		return wrapIntoExecutable(selectAdapter, localCriteriaSupport);
	}
	
	private ExecutableProjectionQuery<C> wrapIntoExecutable(Consumer<Select> selectAdapter, ProjectionCriteriaSupport<C> localCriteriaSupport) {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		return methodDispatcher
				.redirect((SerializableBiFunction<ExecutableProjection, Accumulator<? super Function<? extends Selectable, Object>, Object, Object>, Object>) ExecutableProjection::execute,
						wrapProjectionLoad(selectAdapter, localCriteriaSupport))
				.redirect((SerializableFunction<ExecutableProjection, ExecutableProjection>) ExecutableProjection::distinct, localCriteriaSupport::distinct)
				.redirect(EntityCriteria.class, localCriteriaSupport, true)
				.build((Class<ExecutableProjectionQuery<C>>) (Class) ExecutableProjectionQuery.class);
	}
	
	private <R> Function<Accumulator<? super Function<? extends Selectable, Object>, Object, R>, R> wrapProjectionLoad(Consumer<Select> selectAdapter, ProjectionCriteriaSupport<C> localCriteriaSupport) {
		return (Accumulator<? super Function<? extends Selectable, Object>, Object, R> accumulator) ->
				entitySelectExecutor.selectProjection(selectAdapter, accumulator, localCriteriaSupport.getCriteria(), localCriteriaSupport.isDistinct());
	}
	
	private ProjectionCriteriaSupport<C> newProjectionWhere() {
		// we must clone the underlying support, else it would be modified for all subsequent invocations and criteria will aggregate
		return new ProjectionCriteriaSupport<>(criteriaSupport);
	}
	
	@Override
	public Set<C> selectAll() {
		return entitySelectExecutor.select(newWhere().getCriteria());
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
	
	private static class ProjectionCriteriaSupport<C> extends EntityCriteriaSupport<C> {
		
		private boolean distinct;
		
		public ProjectionCriteriaSupport(EntityCriteriaSupport<C> source) {
			super(source);
		}
		
		public RelationalEntityCriteria<C> distinct() {
			this.distinct = true;
			return this;
		}
		
		public boolean isDistinct() {
			return distinct;
		}
	}
}
