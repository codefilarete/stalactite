package org.codefilarete.stalactite.engine.runtime.tableperclass;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.DeleteExecutor;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.InsertExecutor;
import org.codefilarete.stalactite.engine.UpdateExecutor;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityWriter;
import org.codefilarete.stalactite.engine.runtime.EntityMappingWrapper;
import org.codefilarete.stalactite.engine.runtime.PolymorphicWriter;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.Maps;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismWriter<C, I, T extends Table<T>, SUBENTITY extends C> extends EntityWriter<C, I, T>
		implements PolymorphicWriter<C, I, SUBENTITY> {
	
	private final Map<Class<SUBENTITY>, EntityWriteExecutor<SUBENTITY, I>> subEntitiesPersisters;
	
	public TablePerClassPolymorphismWriter(EntityWriteExecutor<C, I> templatePersister,
	                                       Map<Class<SUBENTITY>, ? extends EntityWriteExecutor<SUBENTITY, I>> subEntitiesPersisters,
										   Dialect dialect,
										   ConnectionConfiguration connectionConfiguration
	) {
		super(templatePersister.getMapping(), dialect, connectionConfiguration);
		
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
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(InsertExecutor::insert);
	}
	
	@Override
	public void doUpdateById(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(UpdateExecutor::updateById);
	}
	
	@Override
	public void doUpdate(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		// Below we keep the order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but it's very difficult to measure
		Map<UpdateExecutor<SUBENTITY>, Set<Duo<SUBENTITY, SUBENTITY>>> entitiesPerType = new KeepOrderMap<>();
		differencesIterable.forEach(payload ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					SUBENTITY entity = (SUBENTITY) Objects.preventNull(payload.getLeft(), payload.getRight());
					if (persister.getMapping().getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent(persister, p -> new KeepOrderSet<>()).add((Duo<SUBENTITY, SUBENTITY>) payload);
					}
				})
		);
		
		entitiesPerType.forEach((updateExecutor, adhocEntities) -> updateExecutor.update(adhocEntities, allColumnsStatement));
	}
	
	@Override
	public void doDelete(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::delete);
	}
	
	@Override
	public void doDeleteById(Iterable<? extends C> entities) {
		Map<EntityPersister<C, I>, Set<C>> entitiesPerType = computeEntitiesPerPersister(entities);
		entitiesPerType.forEach(DeleteExecutor::deleteById);
	}
	
	private <D extends C> Map<EntityPersister<D, I>, Set<D>> computeEntitiesPerPersister(Iterable<? extends C> entities) {
		// Below we keep the order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but it's very difficult to measure
		Map<EntityPersister<D, I>, Set<D>> entitiesPerType = new KeepOrderMap<>();
		entities.forEach(entity ->
				this.subEntitiesPersisters.values().forEach(persister -> {
					if (persister.getMapping().getClassToPersist().isInstance(entity)) {
						entitiesPerType.computeIfAbsent((EntityPersister<D, I>) persister, p -> new KeepOrderSet<>()).add((D) entity);
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
		return new EntityMappingWrapper<C, I, T>(super.getMapping()) {
			@Override
			public void addTransformerListener(TransformerListener<? super C> listener) {
				forEachSubPersister(p -> p.getMapping().addTransformerListener(listener));
			}
			
			@Override
			public void addShadowColumnInsert(ShadowColumnValueProvider<C, T> provider) {
				forEachSubPersister(p -> p.getMapping().addShadowColumnInsert(projectShadowColumnProvider(provider, p)));
			}
			
			@Override
			public void addShadowColumnUpdate(ShadowColumnValueProvider<C, T> provider) {
				forEachSubPersister(p -> p.getMapping().addShadowColumnUpdate(projectShadowColumnProvider(provider, p)));
			}
			
			void forEachSubPersister(Consumer<EntityWriteExecutor<SUBENTITY, I>> consumer) {
				subEntitiesPersisters.values().forEach(consumer);
			}
			
			private <D extends C, SUBENTITYTABLE extends Table<SUBENTITYTABLE>>
			ShadowColumnValueProvider<D, SUBENTITYTABLE>
			projectShadowColumnProvider(ShadowColumnValueProvider<C, T> provider, EntityWriteExecutor<D, I> subEntityPersister) {
				
				// we create the equivalent columns in the sub-entity table and keep a map to be able to project values from the main entity table to the sub-entity table
				Map<Column<T, ?>, Column<SUBENTITYTABLE, ?>> projectedColumnMap = new HashMap<>(provider.getColumns().size());
				provider.getColumns().forEach(c -> {
					Column<SUBENTITYTABLE, ?> projectedColumn = (Column<SUBENTITYTABLE, ?>) subEntityPersister.getMapping().getTargetTable().addColumn(c.getName(), c.getJavaType(), c.getSize(), c.isNullable());
					projectedColumnMap.put(c, projectedColumn);
				});
				return new ShadowColumnValueProvider<D, SUBENTITYTABLE>() {
					
					private final Set<Column<SUBENTITYTABLE, ?>> values = new HashSet<>(projectedColumnMap.values());
					
					@Override
					public Set<Column<SUBENTITYTABLE, ?>> getColumns() {
						return values;
					}
					
					@Override
					public Map<Column<SUBENTITYTABLE, ?>, ?> giveValue(D bean) {
						Map<Column<T, ?>, ?> templateColumnValues = provider.giveValue(bean);
						return Maps.innerJoin(projectedColumnMap, templateColumnValues);
					}
				};
			}
		};
	}
}
