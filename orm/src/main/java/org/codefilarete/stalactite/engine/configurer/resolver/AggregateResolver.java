package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.DefaultComposedIdentifierAssembler;
import org.codefilarete.stalactite.engine.configurer.builder.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMapping;
import org.codefilarete.stalactite.engine.configurer.model.AncestorJoin;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.Entity.Versioning;
import org.codefilarete.stalactite.engine.configurer.model.ExtraTableJoin;
import org.codefilarete.stalactite.engine.configurer.model.IdentifierMapping;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.WriteExecutor.JDBCBatchingIterator;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.CompositeKeyAlreadyAssignedIdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;

public class AggregateResolver {
	
	private final AggregateMetadataResolver aggregateMetadataResolver;
	private final PersistenceContext persistenceContext;
	private final PersisterRegistry persisterRegistry;
	private PersisterBuilderContext persisterBuilderContext;
	
	public AggregateResolver(PersistenceContext persistenceContext) {
		this(persistenceContext, persistenceContext.getPersisterRegistry());
	}
	
	AggregateResolver(PersistenceContext persistenceContext, PersisterRegistry persisterRegistry) {
		this.persistenceContext = persistenceContext;
		this.aggregateMetadataResolver = new AggregateMetadataResolver(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
		this.persisterRegistry = persisterRegistry;
	}
	
	<C, I> EntityPersister<C, I> resolve(EntityMappingConfiguration<C, I> rootConfiguration) {
		Entity<C, I, ?> rootEntity = aggregateMetadataResolver.resolveEntityHierarchy(rootConfiguration);
		return build(rootEntity);
	}
	
	<C, I> ConfiguredRelationalPersister<C, I> build(Entity<C, I, ?> rootEntity) {
		persisterBuilderContext = PersisterBuilderContext.CURRENT.get();
		boolean isInitiator = false;
		if (persisterBuilderContext == null) {
			persisterBuilderContext = new PersisterBuilderContext(persisterRegistry);
			PersisterBuilderContext.CURRENT.set(persisterBuilderContext);
			isInitiator = true;
		}
		
		try {
			ConfiguredRelationalPersister<C, I> result = buildPersister(rootEntity);
			// making aggregate persister available for external usage
			persisterRegistry.addPersister(result);
			if (isInitiator) {
				// This if is only there to execute code below only once, at the very end of persistence graph build,
				// even if it could seem counterintuitive since it compares "isInitiator" whereas this comment talks about end of graph :
				// because persistence configuration is made with a deep-first algorithm, this code (after doBuild()) will be called at the very end.
				persisterBuilderContext.getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterBuild);
				persisterBuilderContext.getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterAllBuild);
			}
			return result;
		} finally {
			if (isInitiator) {
				PersisterBuilderContext.CURRENT.remove();
			}
		}
	}
	
	private <B, C extends B, I, T extends Table<T>,
			LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	ConfiguredRelationalPersister<C, I> buildPersister(Entity<C, I, T> entity) {
		// TODO: check for ealready existing persister in the persistence context
		// TODO: wrap result in an OptimizedUpdatePersister
		// TODO: be inspired from DefaultPersisterBuilder.build()
		
		IdMapping<C, I> idMapping = createIdMapping(entity);
		
		Versioning<C, ?, T> versioning = entity.getVersioning();
		DefaultEntityMapping<C, I, T> entityMapping = new DefaultEntityMapping<>(
				entity.getEntityType(), entity.getTable(),
				entity.getPropertyMappingHolder().getWritablePropertiesPerAccessor(), entity.getPropertyMappingHolder().getReadonlyPropertiesPerAccessor(),
				versioning == null ? null : new Duo<>(versioning.getVersioningAccessor(), versioning.getVersioningColumn()),
				idMapping,
				null,
				false
		);
		
		SimpleRelationalEntityPersister<C, I, T> result = new SimpleRelationalEntityPersister<>(
				entityMapping,
				persistenceContext.getDialect(),
				persistenceContext.getConnectionConfiguration()
		);
		
		entity.getRelations().stream()
				.filter(ExtraTableJoin.class::isInstance).map(ExtraTableJoin.class::cast).forEach(extraTableJoin -> {
					sewExtraTables(extraTableJoin, result, entity);
				});
		
		AncestorJoin<B, LEFTTABLE, RIGHTTABLE, I> parent = (AncestorJoin<B, LEFTTABLE, RIGHTTABLE, I>) entity.getParent();
		while (parent != null) {
			SimpleRelationalEntityPersister<B, I, RIGHTTABLE> parentPersister = sewParentEntity(parent, result);
			
			DirectRelationJoin<LEFTTABLE, RIGHTTABLE, I> join = parent.getJoin();
			Key<LEFTTABLE, I> leftKey = join.getLeftKey();
			Key<RIGHTTABLE, I> rightKey = join.getRightKey();
			EntityMapping<B, I, RIGHTTABLE> mapping = parentPersister.getMapping();
			String joinName = result.getEntityJoinTree().addMergeJoin(
					EntityJoinTree.ROOT_JOIN_NAME,
					new EntityMergerAdapter<>(mapping),
					leftKey,
					rightKey,
					EntityJoinTree.JoinType.INNER);
			
			Entity<B, I, RIGHTTABLE> ancestorEntity = parent.getAncestor();
			ancestorEntity.getRelations().stream()
					.filter(ExtraTableJoin.class::isInstance).map(ExtraTableJoin.class::cast).forEach(extraTableJoin -> {
						SimpleRelationalEntityPersister<B, I, RIGHTTABLE> extraTablePersister = buildExtraTablePersister(extraTableJoin, parentPersister, ancestorEntity);
						// we join the extra table persister to the result persister, not to the ancestor one, to create
						// a single complete aggregate query instead of multiple partial ones that would need to be
						// reconciled later
						result.getEntityJoinTree().addMergeJoin(
								joinName,
								new EntityMergerAdapter<>(extraTablePersister.getMapping()),
								extraTableJoin.getJoin().getLeftKey(),
								extraTableJoin.getJoin().getRightKey(),
								EntityJoinTree.JoinType.INNER);
					});
			
			// preparing next iteration
			parent = (AncestorJoin<B, LEFTTABLE, RIGHTTABLE, I>) parent.getAncestor().getParent();
		}
		
		return result;
	}
	
	@Nullable
	private <B, C extends B, I, T extends Table<T>> IdMapping<C, I> createIdMapping(Entity<C, I, T> entity) {
		IdMapping<C, I> idMapping = null;
		IdentifierMapping<C, I> identifierMapping = entity.getIdentifierMapping();
		if (identifierMapping instanceof SingleIdentifierMapping<?, ?>) {
			Column<T, I> column = (Column<T, I>) Iterables.first(entity.getTable().getPrimaryKey().getColumns());
			idMapping = new SimpleIdMapping<>(
					entity.getIdAccessor(),
					identifierMapping.getIdentifierInsertionManager(),
					new SingleIdentifierAssembler<>(column));
		} else if (identifierMapping instanceof CompositeIdentifierMapping) {
			CompositeIdentifierMapping<C, I, T> compositeIdentifierMapping = (CompositeIdentifierMapping<C, I, T>) identifierMapping;
			EmbeddableMapping<I, T> build = compositeIdentifierMapping.getCompositeKeyMapping();
			Map<ReadWritePropertyAccessPoint<I, Object>, Column<T, Object>> compositeKeyMapping = build.getMapping();
			CompositeKeyAlreadyAssignedIdentifierInsertionManager<C, I> identifierInsertionManager = compositeIdentifierMapping.getIdentifierInsertionManager();
			idMapping = new ComposedIdMapping<>(
					entity.getIdAccessor(),
					identifierInsertionManager,
					new DefaultComposedIdentifierAssembler<>(entity.getTable(), identifierInsertionManager.getIdentifierType(), compositeKeyMapping));
		} else if (identifierMapping instanceof AssignedByAnotherIdentifierMapping) {
			idMapping = mimicIdMapping(identifierMapping, entity.getTable());
		}
		return idMapping;
	}
	
	private <B, C extends B, I, T extends Table<T>> IdMapping<C, I> mimicIdMapping(IdentifierMapping<C, I> identifierMapping, T targetTable) {
		IdMapping<C, I> result;
		if (identifierMapping instanceof AssignedByAnotherIdentifierMapping) {
			result = mimicIdMapping(((AssignedByAnotherIdentifierMapping<C, I>) identifierMapping).getSource(), targetTable);
		} else if (identifierMapping instanceof SingleIdentifierMapping) {
			Column<T, I> column = (Column<T, I>) Iterables.first(targetTable.getPrimaryKey().getColumns());
			SingleIdentifierAssembler<I, T> identifierAssembler = new SingleIdentifierAssembler<>(column);
			result = new SimpleIdMapping<>(
					identifierMapping.getIdAccessor(),
					new BasicIdentifierInsertionManager<>(identifierMapping.getIdentifierInsertionManager().getIdentifierType()),
					identifierAssembler);
		} else {
			// propagating the composite identifier to the actual entity and table
			CompositeKeyAlreadyAssignedIdentifierInsertionManager<C, I> identifierInsertionManager = (CompositeKeyAlreadyAssignedIdentifierInsertionManager<C, I>) identifierMapping.getIdentifierInsertionManager();
			EmbeddableMapping<I, ?> build = ((CompositeIdentifierMapping<C, I, ?>) identifierMapping).getCompositeKeyMapping();
			Map<ReadWritePropertyAccessPoint<I, Object>, Column<T, Object>> entityTableKeyMapping = new HashMap<>();
			build.getMapping().forEach((accessor, column) -> {
				Column<T, Object> entityColumn = targetTable.getColumn(column.getName());
				entityTableKeyMapping.put(accessor, entityColumn);
			});
			entityTableKeyMapping.values().forEach(Column::primaryKey);
			result = new ComposedIdMapping<>(
					identifierMapping.getIdAccessor(),
					identifierInsertionManager,
					new DefaultComposedIdentifierAssembler<>(
							targetTable,
							identifierInsertionManager.getIdentifierType(),
							entityTableKeyMapping));
		}
		return result;
	}
	
	private <C, I, LEFTTABLE extends Table<LEFTTABLE>, EXTRATABLE extends Table<EXTRATABLE>>
	SimpleRelationalEntityPersister<C, I, EXTRATABLE> buildExtraTablePersister(ExtraTableJoin<C, LEFTTABLE, EXTRATABLE, I> extraTableJoin,
	                                                                           SimpleRelationalEntityPersister<C, I, LEFTTABLE> owningPersister,
	                                                                           Entity<C, I, LEFTTABLE> entity) {
		EXTRATABLE extratable = extraTableJoin.getJoin().getRightKey().getTable();
		IdMapping<C, I> idMapping = mimicIdMapping(entity.getIdentifierMapping(), extratable);
		
		DefaultEntityMapping<C, I, EXTRATABLE> entityMapping = new DefaultEntityMapping<>(
				entity.getEntityType(), extratable,
				extraTableJoin.getPropertyMappingHolder().getWritablePropertiesPerAccessor(), extraTableJoin.getPropertyMappingHolder().getReadonlyPropertiesPerAccessor(),
				// no versioning for extra table persister (make no sense because it's managed by the "trunk" persister)
				null,
				idMapping,
				// entity factory makes no sense for extra table properties because the values will be merged through
				// a merge join, thus there won't be any call to the row transformer to create an instance
				null,
				false
		);
		
		SimpleRelationalEntityPersister<C, I, EXTRATABLE> extraTablePersister = new SimpleRelationalEntityPersister<>(
				entityMapping,
				persistenceContext.getDialect(),
				persistenceContext.getConnectionConfiguration()
		);
		
		owningPersister.addInsertListener(new InsertListener<C>() {
			@Override
			public void afterInsert(Iterable<? extends C> entities) {
				extraTablePersister.insert(entities);
			}
		});
		owningPersister.addUpdateListener(new UpdateListener<C>() {
			@Override
			public void afterUpdate(Iterable<? extends Duo<C, C>> entities, boolean allColumnsStatement) {
				extraTablePersister.update(entities, allColumnsStatement);
			}
		});
		owningPersister.addDeleteListener(new DeleteListener<C>() {
			@Override
			public void beforeDelete(Iterable<? extends C> entities) {
				extraTablePersister.delete(entities);
			}
		});
		return extraTablePersister;
	}
	
	private <C, I, LEFTTABLE extends Table<LEFTTABLE>, EXTRATABLE extends Table<EXTRATABLE>>
	void sewExtraTables(ExtraTableJoin<C, LEFTTABLE, EXTRATABLE, I> extraTableJoin, SimpleRelationalEntityPersister<C, I, LEFTTABLE> result, Entity<C, I, LEFTTABLE> entity) {
		SimpleRelationalEntityPersister<C, I, EXTRATABLE> extraTablePersister = buildExtraTablePersister(extraTableJoin, result, entity);
		
		DirectRelationJoin<LEFTTABLE, EXTRATABLE, I> join = extraTableJoin.getJoin();
		Key<LEFTTABLE, I> leftKey = join.getLeftKey();
		Key<EXTRATABLE, I> rightKey = join.getRightKey();
		
		result.getEntityJoinTree().addMergeJoin(
				EntityJoinTree.ROOT_JOIN_NAME,
				new EntityMergerAdapter<>(extraTablePersister.getMapping()),
				leftKey,
				rightKey,
				EntityJoinTree.JoinType.INNER);
	}
	
	private <B, C extends B, I, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	SimpleRelationalEntityPersister<B, I, RIGHTTABLE> sewParentEntity(AncestorJoin<B, LEFTTABLE, RIGHTTABLE, I> parent, RelationalEntityPersister<C, I> child) {
		Map<ReadWritePropertyAccessPoint<B, ?>, Column<RIGHTTABLE, ?>> writablePropertyToColumn = new HashMap<>();
		parent.getAncestor().getPropertyMappingHolder().getWritablePropertyToColumn().forEach(propertyMapping -> {
			writablePropertyToColumn.put(propertyMapping.getAccessPoint(), propertyMapping.getColumn());
		});
		Map<PropertyMutator<B, ?>, Column<RIGHTTABLE, ?>> readonlyPropertyToColumn = new HashMap<>();
		parent.getAncestor().getPropertyMappingHolder().getReadonlyPropertyToColumn().forEach(propertyMapping -> {
			readonlyPropertyToColumn.put(propertyMapping.getAccessPoint(), propertyMapping.getColumn());
		});
		
		IdMapping<B, I> idMapping = createIdMapping(parent.getAncestor());
		
		Versioning<B, ?, RIGHTTABLE> versioning = parent.getAncestor().getVersioning();
		DefaultEntityMapping<B, I, RIGHTTABLE> entityMapping = new DefaultEntityMapping<>(
				parent.getAncestor().getEntityType(), parent.getAncestor().getTable(),
				writablePropertyToColumn, readonlyPropertyToColumn,
				versioning == null ? null : new Duo<>(versioning.getVersioningAccessor(), versioning.getVersioningColumn()),
				idMapping,
				null,
				false
		);
		
		SimpleRelationalEntityPersister<B, I, RIGHTTABLE> ancestorPersister = new SimpleRelationalEntityPersister<>(
				entityMapping,
				persistenceContext.getDialect(),
				persistenceContext.getConnectionConfiguration()
		);
		
		persisterRegistry.addPersister(ancestorPersister);
		child.addInsertListener(new InsertListener<C>() {
			@Override
			public void beforeInsert(Iterable<? extends C> entities) {
				ancestorPersister.insert(entities);
			}
		});
		child.addUpdateListener(new UpdateListener<C>() {
			@Override
			public void afterUpdate(Iterable<? extends Duo<C, C>> entities, boolean allColumnsStatement) {
				ancestorPersister.update((Iterable) entities, allColumnsStatement);
			}
		});
		child.addDeleteListener(new DeleteListener<C>() {
			@Override
			public void afterDelete(Iterable<? extends C> entities) {
				ancestorPersister.delete(entities);
			}
		});
		
		return ancestorPersister;
	}
	
	/**
	 * Basic {@link IdentifierInsertionManager} that provides a simple JDBC batching iterator to write entities
	 * to the table.
	 * 
	 * @param <C> the entity type to write
	 * @param <I> the entity identifier type
	 * @see JDBCBatchingIterator   
	 */
	private static class BasicIdentifierInsertionManager<C, I> implements IdentifierInsertionManager<C, I> {
		
		private final Class<I> memberType;
		
		public BasicIdentifierInsertionManager(Class<I> memberType) {
			this.memberType = memberType;
		}
		
		@Override
		public Class<I> getIdentifierType() {
			return memberType;
		}
		
		@Override
		public JDBCBatchingIterator<C> buildJDBCBatchingIterator(Iterable<? extends C> entities, WriteOperation<? extends Column<? extends Table, ?>> writeOperation, int batchSize) {
			return new JDBCBatchingIterator<>(entities, writeOperation, batchSize);
		}
	}
}
