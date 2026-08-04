package org.codefilarete.stalactite.engine.runtime.singletable;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.DeleteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.InsertExecutor;
import org.codefilarete.stalactite.engine.UpdateExecutor;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityWriter;
import org.codefilarete.stalactite.engine.runtime.EntityMappingWrapper;
import org.codefilarete.stalactite.engine.runtime.PolymorphicPersister;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismWriter<C, I, T extends Table<T>, SUBENTITY extends C, DTYPE> extends EntityWriter<C, I, T>
		implements PolymorphicPersister<C> {
	
	private final EntityWriteExecutor<C, I> mainPersister;
	private final Map<Class<SUBENTITY>, EntityWriteExecutor<SUBENTITY, I>> subEntitiesPersisters;
	
	public SingleTablePolymorphismWriter(EntityWriteExecutor<C, I> mainPersister,
	                                     Map<Class<SUBENTITY>, ? extends EntityWriteExecutor<SUBENTITY, I>> subEntitiesPersisters,
	                                     Column<T, DTYPE> discriminatorColumn,
										 Function<? super Class<SUBENTITY>, DTYPE> discriminatorValueProvider,
										 Dialect dialect,
	                                     ConnectionConfiguration connectionConfiguration) {
		super(mainPersister.getMapping(), dialect, connectionConfiguration);
		
		this.mainPersister = mainPersister;
		this.subEntitiesPersisters = (Map<Class<SUBENTITY>, EntityWriteExecutor<SUBENTITY, I>>) subEntitiesPersisters;
		
		ShadowColumnValueProvider<C, T> discriminatorColumnValueProvider = new ShadowColumnValueProvider<C, T>() {
			
			@Override
			public Set<Column<T, ?>> getColumns() {
				return Arrays.asHashSet(discriminatorColumn);
			}
			
			@Override
			public Map<Column<T, ?>, ?> giveValue(C bean) {
				Map<Column<T, ?>, Object> result = new HashMap<>();
				result.put(discriminatorColumn, discriminatorValueProvider.apply((Class<SUBENTITY>) bean.getClass()));
				return result;
			}
		};
		this.subEntitiesPersisters.values().forEach(subclassPersister -> ((EntityMapping) subclassPersister.getMapping())
				.addShadowColumnInsert(discriminatorColumnValueProvider));
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
		Map<EntityWriteExecutor<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(InsertExecutor::insert);
	}
	
	@Override
	public void doUpdateById(Iterable<? extends C> entities) {
		Map<EntityWriteExecutor<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(UpdateExecutor::updateById);
	}
	
	@Override
	public void doUpdate(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
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
	}
	
	@Override
	public void doDeleteById(Iterable<? extends C> entities) {
		Map<EntityWriteExecutor<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::deleteById);
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
	 * Overridden to capture {@link EntityMapping#addShadowColumnInsert(ShadowColumnValueProvider)} and
	 * {@link EntityMapping#addShadowColumnUpdate(ShadowColumnValueProvider)} (see {@link OneToManyRelationConfigurer})
	 * Made to dispatch those methods subclass strategies since their persisters are in charge of managing their entities (not the parent one).
	 * <p>
	 * Design question : one may think that's not a good design to override a getter, caller should invoke an intention-clear method on
	 * ourselves (Persister) but the case is to add a silent Column insert/update which is not the goal of the Persister to know implementation
	 * detail : they are to manage cascades and coordinate their mapping strategies. {@link EntityMapping} are in charge of knowing
	 * {@link Column} actions.
	 *
	 * @return an enhanced version of our main persister mapping strategy which dispatches silent column insert/update to sub-entities ones
	 */
	@Override
	public EntityMapping<C, I, T> getMapping() {
		return new EntityMappingWrapper<C, I, T>(mainPersister.getMapping()) {
			@Override
			public void addTransformerListener(TransformerListener<? super C> listener) {
				subEntitiesPersisters.values().forEach(p -> ((EntityMapping) p.getMapping()).addTransformerListener(listener));
			}
			
			@Override
			public void addShadowColumnInsert(ShadowColumnValueProvider<C, T> provider) {
				subEntitiesPersisters.values().forEach(p -> ((EntityMapping) p.getMapping()).addShadowColumnInsert(provider));
			}
			
			@Override
			public void addShadowColumnUpdate(ShadowColumnValueProvider<C, T> provider) {
				subEntitiesPersisters.values().forEach(p -> ((EntityMapping) p.getMapping()).addShadowColumnUpdate(provider));
			}
		};
	}
}
