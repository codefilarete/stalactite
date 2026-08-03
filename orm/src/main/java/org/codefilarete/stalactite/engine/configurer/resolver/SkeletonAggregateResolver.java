package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.DefaultComposedIdentifierAssembler;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMapping;
import org.codefilarete.stalactite.engine.configurer.dslresolver.AssignedByAnotherIdentifierMapping;
import org.codefilarete.stalactite.engine.configurer.dslresolver.CompositeIdentifierMapping;
import org.codefilarete.stalactite.engine.configurer.dslresolver.SingleIdentifierMapping;
import org.codefilarete.stalactite.engine.configurer.model.AbstractEntity;
import org.codefilarete.stalactite.engine.configurer.model.AbstractEntity.Versioning;
import org.codefilarete.stalactite.engine.configurer.model.AncestorJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.ExtraTableJoin;
import org.codefilarete.stalactite.engine.configurer.model.IdentifierMapping;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.WriteExecutor.JDBCBatchingIterator;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.CompositeKeyAlreadyAssignedIdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;


/**
 * Class aims at solving the structural "bones" of an {@link Entity}, which means the mapping of:
 * - identifier
 * - direct properties, including embedded ones
 * - inheritance (ancestors, not polymorphism)
 * <p>
 * This logic was extended to properties on extra tables.
 */
public class SkeletonAggregateResolver {
	
	private final PersistenceContext persistenceContext;
	
	public SkeletonAggregateResolver(PersistenceContext persistenceContext) {
		this.persistenceContext = persistenceContext;
	}
	
	public <B, C extends B, I, T extends Table<T>>
	EntityReadWriteExecutor<C, I> buildPersister(AbstractEntity<C, I, T> entity, CreatedPersisterCollector<C, I> persisterCollector) {
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
		
		// we create a ReadWriteEntityExecutor that has the persist() capability because insert() cascade will
		// trigger the persist(..) method, thus, the persister shall have the select ability to determine if the entity is new or not.
		// However, the read ability is not one of the aggregate, it only focuses on the direct properties.
		EntityReadWriteExecutor<C, I> rootPersister = new DelegatingReadWriteEntityExecutor<>(new EntityWriter<>(
				entityMapping,
				persistenceContext.getDialect(),
				persistenceContext.getConnectionConfiguration()
		), new EntityReader<>(
				entityMapping,
				persistenceContext.getConnectionProvider(),
				persistenceContext.getDialect()
		));
		
		// when identifier policy is already-assigned one, we must ensure that entity is marked as persisted when it comes back from database
		// because user may forget to / can't mark it as such
		IdentifierInsertionManager<C, I> identifierInsertionManager = rootPersister.getMapping().getIdMapping().getIdentifierInsertionManager();
		if (identifierInsertionManager instanceof AlreadyAssignedIdentifierManager) {
			// Transferring identifier manager InsertListener to here
			rootPersister.addInsertListener(((AlreadyAssignedIdentifierManager<C, I>) identifierInsertionManager).getInsertListener());
		}
		
		persisterCollector.setPersister(rootPersister);
		
		appendInheritance(entity, rootPersister, persisterCollector);
		
		return rootPersister;
	}
	
	private <B, C extends B, I, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	void appendInheritance(AbstractEntity<C, I, LEFTTABLE> entity, EntityWriteExecutor<C, I> rootPersister, CreatedPersisterCollector<C, I> persisterCollector) {
		// looking for extra tables: they are stored as ExtraTableJoin in the entity relations 
		entity.getRelations().stream()
				.filter(ExtraTableJoin.class::isInstance).map(ExtraTableJoin.class::cast).forEach(extraTableJoin -> {
					EntityWriter<C, I, LEFTTABLE> extraTablePersister = buildExtraTablePersister(extraTableJoin, rootPersister, entity);
					persisterCollector.addExtraTablePersister(rootPersister, extraTablePersister);
				});
		
		AncestorJoin<B, LEFTTABLE, RIGHTTABLE, I> parent = (AncestorJoin<B, LEFTTABLE, RIGHTTABLE, I>) entity.getParent();
		while (parent != null) {
			EntityWriter<B, I, RIGHTTABLE> parentPersister = sewParentEntity(parent, rootPersister);
			persisterCollector.addAncestorPersister(parentPersister);
			
			AbstractEntity<B, I, RIGHTTABLE> ancestorEntity = parent.getAncestor();
			ancestorEntity.getRelations().stream()
					.filter(ExtraTableJoin.class::isInstance).map(ExtraTableJoin.class::cast).forEach(extraTableJoin -> {
						
						// we join the extra table persister to the result persister, not to the ancestor one, to create
						// a single complete aggregate query instead of multiple partial ones that would need to be
						// reconciled later
						EntityWriter<C, I, LEFTTABLE> extraTablePersister = buildExtraTablePersister(extraTableJoin, parentPersister, ancestorEntity);
						persisterCollector.addExtraTablePersister(parentPersister, extraTablePersister);
					});
			
			// preparing next iteration
			parent = (AncestorJoin<B, LEFTTABLE, RIGHTTABLE, I>) parent.getAncestor().getParent();
		}
	}
	
	/**
	 * Creates the appropriate type of {@link IdMapping} based on the structure of the identifier mapping present in
	 * the given entity (e.g., single, composite, or externally assigned identifiers).
	 *
	 * @param <C> the base type of the entity
	 * @param <I> the entity identifier type
	 * @param <T> the entity persistence table type
	 * @param entity the entity for which the {@link IdMapping} is to be created. This includes metadata
	 *  about the entity's identifier mapping and its associated table.
	 * @return an {@link IdMapping} instance configured for the specified entity. This maps the entity's 
	 *  identifier to its underlying relational representation, supporting insertion and retrieval operations.
	 */
	public <C, I, T extends Table<T>> IdMapping<C, I> createIdMapping(AbstractEntity<C,I,T> entity) {
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
			Map<ReadWritePropertyAccessPoint<I, ?>, Column<T, ?>> compositeKeyMapping = build.getMapping();
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
			Map<ReadWritePropertyAccessPoint<I, ?>, Column<T, ?>> entityTableKeyMapping = new HashMap<>();
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
	EntityWriter<C, I, EXTRATABLE> buildExtraTablePersister(ExtraTableJoin<C, LEFTTABLE, EXTRATABLE, I> extraTableJoin,
	                                                        EntityWriteExecutor<C, I> owningPersister,
	                                                        AbstractEntity<C, I, LEFTTABLE> identifierDefiner) {
		EXTRATABLE extratable = extraTableJoin.getJoin().getRightKey().getTable();
		IdMapping<C, I> idMapping = mimicIdMapping(identifierDefiner.getIdentifierMapping(), extratable);
		
		DefaultEntityMapping<C, I, EXTRATABLE> entityMapping = new DefaultEntityMapping<>(
				identifierDefiner.getEntityType(), extratable,
				extraTableJoin.getPropertyMappingHolder().getWritablePropertiesPerAccessor(), extraTableJoin.getPropertyMappingHolder().getReadonlyPropertiesPerAccessor(),
				// no versioning for extra table persister (make no sense because it's managed by the "trunk" persister)
				null,
				idMapping,
				// entity factory makes no sense for extra table properties because the values will be merged through
				// a merge join, thus there won't be any call to the row transformer to create an instance
				null,
				false
		);
		
		EntityWriter<C, I, EXTRATABLE> extraTablePersister = new EntityWriter<>(
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
	
	private <B, C extends B, I, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	EntityWriter<B, I, RIGHTTABLE> sewParentEntity(AncestorJoin<B, LEFTTABLE, RIGHTTABLE, I> parent, EntityWriteExecutor<C, I> child) {
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
		
		EntityWriter<B, I, RIGHTTABLE> ancestorPersister = new EntityWriter<>(
				entityMapping,
				persistenceContext.getDialect(),
				persistenceContext.getConnectionConfiguration()
		);

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
