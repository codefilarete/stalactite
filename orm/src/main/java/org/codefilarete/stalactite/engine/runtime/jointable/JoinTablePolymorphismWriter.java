package org.codefilarete.stalactite.engine.runtime.jointable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.DeleteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.InsertExecutor;
import org.codefilarete.stalactite.engine.UpdateExecutor;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityWriter;
import org.codefilarete.stalactite.engine.runtime.EntityMappingWrapper;
import org.codefilarete.stalactite.engine.runtime.PolymorphicPersister;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class JoinTablePolymorphismWriter<C, I, T extends Table<T>, SUBENTITY extends C> extends EntityWriter<C, I, T>
		implements PolymorphicPersister<C> {
	
	private final EntityWriteExecutor<C, I> mainPersister;
	private final Map<Class<SUBENTITY>, EntityWriteExecutor<SUBENTITY, I>> subEntitiesPersisters;
	
	public JoinTablePolymorphismWriter(EntityWriteExecutor<C, I> mainPersister,
	                                   Map<Class<SUBENTITY>, ? extends EntityWriteExecutor<SUBENTITY, I>> subEntitiesPersisters,
	                                   Dialect dialect,
	                                   ConnectionConfiguration connectionConfiguration
	) {
		super(mainPersister.getMapping(), dialect, connectionConfiguration);
		
		this.mainPersister = mainPersister;
		this.subEntitiesPersisters = (Map<Class<SUBENTITY>, EntityWriteExecutor<SUBENTITY, I>>) subEntitiesPersisters;
	}
	
	public Map<Class<SUBENTITY>, EntityWriteExecutor<SUBENTITY, I>> getSubEntitiesPersisters() {
		return subEntitiesPersisters;
	}
	
	@Override
	public Set<Class<? extends C>> getSupportedEntityTypes() {
		Collection<Class<SUBENTITY>> subTypes = Iterables.collect(this.subEntitiesPersisters.values(), p -> p.getMapping().getClassToPersist(), HashSet::new);
		return (Set) subTypes;
	}
	
	@Override
	public void doInsert(Iterable<? extends C> entities) {
		mainPersister.insert(entities);
		Map<EntityWriteExecutor<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(InsertExecutor::insert);
	}
	
	@Override
	public void doUpdateById(Iterable<? extends C> entities) {
		mainPersister.updateById(entities);
		Map<EntityWriteExecutor<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(UpdateExecutor::updateById);
	}
	
	@Override
	public void doUpdate(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		mainPersister.update(differencesIterable, allColumnsStatement);
		
		doUpdateWithGenerics(differencesIterable, allColumnsStatement);
	}
	
	private <D extends C> void doUpdateWithGenerics(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		// Below we keep the order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but it's very difficult to measure
		Map<UpdateExecutor<D>, Set<Duo<D, D>>> entitiesPerType = new KeepOrderMap<>();
		differencesIterable.forEach(payload ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					C entity = Objects.preventNull(payload.getLeft(), payload.getRight());
					if (persister.getMapping().getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent((UpdateExecutor<D>) persister, p -> new KeepOrderSet<>())
								.add((Duo<D, D>) payload);
					}
				})
		);
		
		entitiesPerType.forEach((updateExecutor, adhocEntities) -> updateExecutor.update(adhocEntities, allColumnsStatement));
	}
	
	@Override
	public void doDelete(Iterable<? extends C> entities) {
		Map<EntityWriteExecutor<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::delete);
		mainPersister.delete(entities);
	}
	
	@Override
	public void doDeleteById(Iterable<? extends C> entities) {
		Map<EntityWriteExecutor<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::deleteById);
		mainPersister.deleteById(entities);
	}
	
	private <D extends C> Map<EntityWriteExecutor<D, I>, Set<D>> computeEntitiesPerPersister(Iterable<? extends C> entities) {
		// Below we keep the order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but it's very difficult to measure
		Map<EntityWriteExecutor<D, I>, Set<D>> entitiesPerType = new KeepOrderMap<>();
		entities.forEach(entity ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					if (persister.getMapping().getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent((EntityWriteExecutor<D, I>) persister, p -> new KeepOrderSet<>()).add((D) entity);
					}
				})
		);
		return entitiesPerType;
	}
	
	/**
	 * Overridden to capture {@link EntityMapping#addTransformerListener(TransformerListener)} in order to dispatch it to
	 * sub-entities strategies since their persisters are in charge of managing their entities (not the parent one).
	 * <p>
	 * Design question : one may think that's not a good design to override a getter, caller should invoke an intention-clear method on
	 * ourselves (Persister) but the case is to add a transformer which is not the goal of the Persister to know implementation
	 * detail : they are to manage cascades and coordinate their mapping strategies. {@link EntityMapping} are in charge of knowing
	 * such actions.
	 *
	 * @return an enhanced version of our main persister mapping strategy which dispatches transformer listeners to sub-entities ones
	 */
	@Override
	public EntityMapping<C, I, T> getMapping() {
		return new EntityMappingWrapper<C, I, T>(mainPersister.getMapping()) {
			@Override
			public void addTransformerListener(TransformerListener<? super C> listener) {
				super.addTransformerListener(listener);
				subEntitiesPersisters.values().forEach(persister -> persister.getMapping().addTransformerListener(listener));
			}
		};
	}
}
