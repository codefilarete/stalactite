package org.codefilarete.stalactite.engine.configurer.polymorphism;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.JoinTablePolymorphism;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphismPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.tool.function.Converter;

/**
 * Builder of polymorphic persisters. Handles {@link PolymorphismPolicy} subtypes, as such, it is the main entry point for polymorphic persisters :
 * it will invoke {@link JoinTablePolymorphismBuilder}, {@link SingleTablePolymorphismBuilder} or {@link TablePerClassPolymorphismBuilder}
 * according to policy. Hence, those builders are not expected to be invoked directly outside this class.
 * 
 * @author Guillaume Mary
 */
public class PolymorphismPersisterBuilder<C, I, T extends Table> implements PolymorphismBuilder<C, I, T> {
	
	private final PolymorphismPolicy<C> polymorphismPolicy;
	private final ConfiguredRelationalPersister<C, I> mainPersister;
	private final AbstractIdentification<C, I> identification;
	private final ColumnBinderRegistry columnBinderRegistry;
	
	private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> mainMapping;
	private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> mainReadonlyMapping;
	private final ValueAccessPointMap<C, Converter<Object, Object>> mainReadConverters;
	private final ValueAccessPointMap<C, Converter<Object, Object>> mainWriteConverters;
	private final NamingConfiguration namingConfiguration;
	
	public PolymorphismPersisterBuilder(PolymorphismPolicy<C> polymorphismPolicy,
										AbstractIdentification<C, I> identification,
										ConfiguredRelationalPersister<C, I> mainPersister,
										ColumnBinderRegistry columnBinderRegistry,
										Map<? extends ReversibleAccessor<C, Object>, Column<T, Object>> mainMapping,
										Map<? extends ReversibleAccessor<C, Object>, Column<T, Object>> mainReadonlyMapping,
										ValueAccessPointMap<C, ? extends Converter<Object, Object>> mainReadConverters,
										ValueAccessPointMap<C, ? extends Converter<Object, Object>> mainWriteConverters,
										NamingConfiguration namingConfiguration) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.identification = identification;
		this.mainPersister = mainPersister;
		this.columnBinderRegistry = columnBinderRegistry;
		this.mainMapping = (Map<ReversibleAccessor<C, Object>, Column<T, Object>>) mainMapping;
		this.mainReadonlyMapping = (Map<ReversibleAccessor<C, Object>, Column<T, Object>>) mainReadonlyMapping;
		this.mainReadConverters = (ValueAccessPointMap<C, Converter<Object, Object>>) mainReadConverters;
		this.mainWriteConverters = (ValueAccessPointMap<C, Converter<Object, Object>>) mainWriteConverters;
		this.namingConfiguration = namingConfiguration;
	}
	
	@Override
	public AbstractPolymorphismPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		PolymorphismBuilder<C, I, T> polymorphismBuilder;
		if (polymorphismPolicy instanceof PolymorphismPolicy.SingleTablePolymorphism) {
			polymorphismBuilder = new SingleTablePolymorphismBuilder<>((SingleTablePolymorphism<C, ?>) polymorphismPolicy,
					this.identification, this.mainPersister,
					this.mainMapping, this.mainReadonlyMapping,
					this.mainReadConverters, this.mainWriteConverters,
					this.columnBinderRegistry, this.namingConfiguration);
		} else if (polymorphismPolicy instanceof PolymorphismPolicy.TablePerClassPolymorphism) {
			polymorphismBuilder = new TablePerClassPolymorphismBuilder<>((TablePerClassPolymorphism<C>) polymorphismPolicy,
					this.identification, this.mainPersister,
					this.mainMapping, this.mainReadonlyMapping,
					this.mainReadConverters, this.mainWriteConverters,
					this.columnBinderRegistry, this.namingConfiguration);
		} else if (polymorphismPolicy instanceof PolymorphismPolicy.JoinTablePolymorphism) {
			polymorphismBuilder = new JoinTablePolymorphismBuilder<>((JoinTablePolymorphism<C>) polymorphismPolicy,
					this.identification, this.mainPersister, this.columnBinderRegistry, this.namingConfiguration);
		} else {
			// this exception is more to satisfy Sonar than for real case
			throw new NotImplementedException("Given policy is not implemented : " + polymorphismPolicy);
		}
		AbstractPolymorphismPersister<C, I> result = polymorphismBuilder.build(dialect, connectionConfiguration);
		// We transfer listeners so that all actions are made in the same "event listener context" : all listeners are aggregated in a top level one.
		// Made in particular for relation cascade triggering.
		// TODO: move main persister selectListener propagation to sub-persisters here ? see JoinTablePolymorphismBuilder 
		mainPersister.getPersisterListener().moveTo(result.getPersisterListener());
		
		// We propagate listeners to sub-persisters to make them trigger their own select listeners, in particular the relation ones and their
		// already-assigned persisted flag. This is necessary because the main persister is not aware of sub-relations.
		result.addSelectListener(new SelectListenerPropagator<>(result));
		
		return result;
	}
	
	private static class SelectListenerPropagator<C, I> implements SelectListener<C, I> {
		
		private final AbstractPolymorphismPersister<C, I> parentPersister;
		
		private SelectListenerPropagator(AbstractPolymorphismPersister<C, I> parentPersister) {
			this.parentPersister = parentPersister;
		}
		
		@Override
		public void beforeSelect(Iterable<I> ids) {
			parentPersister.getSubEntitiesPersisters().values()
					.forEach(subPersister -> subPersister.getPersisterListener().getSelectListener().beforeSelect(ids));
		}
		
		@Override
		public void afterSelect(Set<? extends C> entities) {
			Map<Class<C>, Set<C>> entitiesPerType = computeEntitiesPerType(entities);
			parentPersister.getSubEntitiesPersisters().forEach((type, subPersister) -> {
				Set<C> subEntities = entitiesPerType.get(type);
				if (subEntities != null) {
					subPersister.getPersisterListener().getSelectListener().afterSelect(subEntities);
				}
			});
		}
		
		@Override
		public void onSelectError(Iterable<I> ids, RuntimeException exception) {
			// since ids are not those of its entities, we should not pass them as argument
			parentPersister.getSubEntitiesPersisters().values()
					.forEach(subPersister -> subPersister.getPersisterListener().getSelectListener().onSelectError(ids, exception));
		}
		
		private Map<Class<C>, Set<C>> computeEntitiesPerType(Iterable<? extends C> entities) {
			Map<Class<C>, Set<C>> entitiesPerType = new HashMap<>();
			entities.forEach(entity ->
					entitiesPerType.computeIfAbsent((Class<C>) entity.getClass(), p -> new KeepOrderSet<>()).add(entity)
			);
			return entitiesPerType;
		}
	}
}
