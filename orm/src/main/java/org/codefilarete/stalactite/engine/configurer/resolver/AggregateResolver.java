package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteByIdSupport;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteSupport;
import org.codefilarete.stalactite.engine.cascade.BeforeInsertSupport;
import org.codefilarete.stalactite.engine.configurer.DefaultComposedIdentifierAssembler;
import org.codefilarete.stalactite.engine.configurer.builder.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMapping;
import org.codefilarete.stalactite.engine.configurer.manyToOne.ManyToOneOwnedBySourceConfigurer.MandatoryRelationAssertBeforeUpdateListener;
import org.codefilarete.stalactite.engine.configurer.model.AncestorJoin;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.Entity.Versioning;
import org.codefilarete.stalactite.engine.configurer.model.ExtraTableJoin;
import org.codefilarete.stalactite.engine.configurer.model.IdentifierMapping;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneConfigurerTemplate.MandatoryRelationAssertBeforeInsertListener;
import org.codefilarete.stalactite.engine.configurer.onetoone.OrphanRemovalOnUpdate;
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
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.CompositeKeyAlreadyAssignedIdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.PairIterator;
import org.codefilarete.tool.function.Functions.NullProofFunction;
import org.codefilarete.tool.function.Predicates;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.INNER;

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
		Entity<C, I, ?> rootEntity = aggregateMetadataResolver.resolve(rootConfiguration);
		return build(rootEntity);
	}
	
	<C, I> ConfiguredRelationalPersister<C, I> build(Entity<C, I, ?> rootEntity) {
		// all this is left for compatibility with existing persister builders mechanism
		// it should be removed (or replaced by a close mechanism) at the very end of the implementation of the new persister build mechanism
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
	
	private <B, C extends B, I, T extends Table<T>>
	ConfiguredRelationalPersister<C, I> buildPersister(Entity<C, I, T> rootEntity) {
		// TODO: check for ealready existing persister in the persistence context
		// TODO: wrap result in an OptimizedUpdatePersister
		// TODO: be inspired from DefaultPersisterBuilder.build()
		
		IdMapping<C, I> idMapping = createIdMapping(rootEntity);
		
		Versioning<C, ?, T> versioning = rootEntity.getVersioning();
		DefaultEntityMapping<C, I, T> entityMapping = new DefaultEntityMapping<>(
				rootEntity.getEntityType(), rootEntity.getTable(),
				rootEntity.getPropertyMappingHolder().getWritablePropertiesPerAccessor(), rootEntity.getPropertyMappingHolder().getReadonlyPropertiesPerAccessor(),
				versioning == null ? null : new Duo<>(versioning.getVersioningAccessor(), versioning.getVersioningColumn()),
				idMapping,
				null,
				false
		);
		
		SimpleRelationalEntityPersister<C, I, T> rootPersister = new SimpleRelationalEntityPersister<>(
				entityMapping,
				persistenceContext.getDialect(),
				persistenceContext.getConnectionConfiguration()
		);
		
		// when identifier policy is already-assigned one, we must ensure that entity is marked as persisted when it comes back from database
		// because user may forget to / can't mark it as such
		IdentifierInsertionManager<C, I> identifierInsertionManager = rootPersister.getMapping().getIdMapping().getIdentifierInsertionManager();
		if (identifierInsertionManager instanceof AlreadyAssignedIdentifierManager) {
			// Transferring identifier manager InsertListener to here
			rootPersister.addInsertListener(((AlreadyAssignedIdentifierManager<C, I>) identifierInsertionManager).getInsertListener());
			rootPersister.addSelectListener(((AlreadyAssignedIdentifierManager<C, I>) identifierInsertionManager).getSelectListener());
		}
		
		appendInheritance(rootEntity, rootPersister);
		
		appendOneToOnes(rootEntity, rootPersister);
		
		return rootPersister;
	}
	
	private <B, C extends B, I, T extends Table<T>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	void appendInheritance(Entity<C, I, T> entity, SimpleRelationalEntityPersister<C, I, T> result) {
		// looking for extra tables: they are stored as ExtraTableJoin in the entity relations 
		entity.getRelations().stream()
				.filter(ExtraTableJoin.class::isInstance).map(ExtraTableJoin.class::cast).forEach(extraTableJoin -> {
					sewExtraTables(extraTableJoin, result, result, entity, EntityJoinTree.ROOT_JOIN_NAME);
				});
		
		AncestorJoin<B, LEFTTABLE, RIGHTTABLE, I> parent = (AncestorJoin<B, LEFTTABLE, RIGHTTABLE, I>) entity.getParent();
		while (parent != null) {
			SimpleRelationalEntityPersister<B, I, RIGHTTABLE> parentPersister = sewParentEntity(parent, result);
			
			DirectRelationJoin<LEFTTABLE, RIGHTTABLE, I> join = parent.getJoin();
			EntityMapping<B, I, RIGHTTABLE> mapping = parentPersister.getMapping();
			String joinName = result.getEntityJoinTree().addMergeJoin(
					EntityJoinTree.ROOT_JOIN_NAME,
					new EntityMergerAdapter<>(mapping),
					join.getLeftKey(),
					join.getRightKey(),
					INNER);
			
			Entity<B, I, RIGHTTABLE> ancestorEntity = parent.getAncestor();
			ancestorEntity.getRelations().stream()
					.filter(ExtraTableJoin.class::isInstance).map(ExtraTableJoin.class::cast).forEach(extraTableJoin -> {
						
						// we join the extra table persister to the result persister, not to the ancestor one, to create
						// a single complete aggregate query instead of multiple partial ones that would need to be
						// reconciled later
						sewExtraTables(extraTableJoin, result, parentPersister, ancestorEntity, joinName);
					});
			
			// preparing next iteration
			parent = (AncestorJoin<B, LEFTTABLE, RIGHTTABLE, I>) parent.getAncestor().getParent();
		}
	}
	
	private <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void appendOneToOnes(Entity<SRC, SRCID, RIGHTTABLE> entity, SimpleRelationalEntityPersister<SRC, SRCID, LEFTTABLE> result) {
		entity.getRelations().stream()
				.filter(ResolvedOneToOneRelation.class::isInstance)
				.map(ResolvedOneToOneRelation.class::cast)
				.forEach(relation -> {
					ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> resolvedRelation = relation;
					ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = buildPersister(resolvedRelation.getTargetEntity());
					targetPersister.joinAsOne(result,
							resolvedRelation.getAccessor(),
							resolvedRelation.getJoin().getLeftKey(),
							resolvedRelation.getJoin().getRightKey(),
							null,
							resolvedRelation.getBeanRelationFixer(),
							true,
							resolvedRelation.isFetchSeparately());
					
					
					boolean orphanRemoval = resolvedRelation.getRelationMode() == CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
					boolean writeAuthorized = resolvedRelation.getRelationMode() != CascadeOptions.RelationMode.READ_ONLY;
					if (writeAuthorized) {
						// NB: "delete removed" will be treated internally by updateCascade() and deleteCascade()
						
						ReadWritePropertyAccessPoint<SRC, TRGT> targetAccessor = resolvedRelation.getAccessor();
						// if cascade is mandatory, then adding nullability checking before insert
						if (resolvedRelation.isMandatory()) {
							result.addInsertListener(new MandatoryRelationAssertBeforeInsertListener<>(targetAccessor));
							result.addUpdateListener(new MandatoryRelationAssertBeforeUpdateListener<>(targetAccessor));
						}
						
						Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> keyColumnsMapping;
						PairIterator<JoinLink<LEFTTABLE, ?>, JoinLink<RIGHTTABLE, ?>> keyColumnsIterator = new PairIterator<>(resolvedRelation.getJoin().getLeftKey().getColumns(), resolvedRelation.getJoin().getRightKey().getColumns());
						keyColumnsMapping = Iterables.map(() -> keyColumnsIterator, d -> (Column<LEFTTABLE, ?>) d.getLeft(), d -> (Column<RIGHTTABLE, ?>) d.getRight());
						
						
						IdMapping<TRGT, TRGTID> trgtIdMapping = targetPersister.getMapping().getIdMapping();
						ShadowColumnValueProvider<SRC, LEFTTABLE> foreignKeyValueProvider = new ShadowColumnValueProvider<SRC, LEFTTABLE>() {
							@Override
							public Set<Column<LEFTTABLE, ?>> getColumns() {
								return keyColumnsMapping.keySet();
							}
							
							@Override
							public Map<Column<LEFTTABLE, ?>, ?> giveValue(SRC bean) {
								TRGTID trgtid = trgtIdMapping.getIdAccessor().getId(targetAccessor.get(bean));
								Map<Column<RIGHTTABLE, ?>, ?> columnValues = trgtIdMapping.<RIGHTTABLE>getIdentifierAssembler().getColumnValues(trgtid);
								return Maps.innerJoinOnValuesAndKeys(keyColumnsMapping, columnValues);
							}
						};
						
						// Managing insert cascade
						result.getMapping().addShadowColumnInsert(foreignKeyValueProvider);
						// adding cascade treatment: before source insert, target is inserted to comply with foreign key constraint
						result.addInsertListener(new BeforeInsertSupport<>(targetPersister::persist, targetAccessor::get, Objects::nonNull));
						
						// Managing update cascade
						if (orphanRemoval) {
							result.addUpdateListener(new OrphanRemovalOnUpdate<>(targetPersister, targetAccessor));
						}
						result.getMapping().addShadowColumnUpdate(foreignKeyValueProvider);
						// - insert non-persisted target instances to fulfill foreign key requirement
						Function<SRC, TRGT> targetProviderAsFunction = new NullProofFunction<>(targetAccessor::get);
						result.addUpdateListener(new UpdateListener<SRC>() {
							
							private final Predicate<TRGT> newInstancePredicate = Predicates.<TRGT>predicate(Objects::nonNull).and(targetPersister.getMapping()::isNew);
							
							@Override
							public void beforeUpdate(Iterable<? extends Duo<SRC, SRC>> payloads, boolean allColumnsStatement) {
								// we only insert new instances
								targetPersister.persist(Iterables.stream(payloads).map(duo -> targetProviderAsFunction.apply(duo.getLeft()))
										.filter(newInstancePredicate).collect(Collectors.toSet()));
							}
						});
						// - after source update, target is updated too
						result.addUpdateListener(new UpdateListener<SRC>() {
							
							@Override
							public void afterUpdate(Iterable<? extends Duo<SRC, SRC>> payloads, boolean allColumnsStatement) {
								List<Duo<TRGT, TRGT>> targetsToUpdate = Iterables.collect(payloads,
										// targets of nullified relations don't need to be updated 
										e -> getTarget(e.getLeft()) != null,
										e -> getTargets(e.getLeft(), e.getRight()),
										ArrayList::new);
								targetPersister.update(targetsToUpdate, allColumnsStatement);
							}
							
							private Duo<TRGT, TRGT> getTargets(SRC modifiedTrigger, SRC unmodifiedTrigger) {
								return new Duo<>(getTarget(modifiedTrigger), getTarget(unmodifiedTrigger));
							}
							
							private TRGT getTarget(SRC src) {
								return targetAccessor.get(src);
							}
						});
						
						// Managing delete cascade
						if (orphanRemoval) {
							// adding cascade treatment: target is deleted after source deletion (because of foreign key constraint)
							result.addDeleteListener(new AfterDeleteSupport<>(targetPersister::delete, targetAccessor::get, Objects::nonNull));
							// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
							result.addDeleteByIdListener(new AfterDeleteByIdSupport<>(targetPersister::delete, targetAccessor::get, Objects::nonNull));
						} // else : no target entities deletion asked (no delete orphan) : nothing more to do than deleting the source entity
					}
				});
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
	                                                                           Entity<C, I, LEFTTABLE> identifierDefiner) {
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
	
	private <B, C extends B, I, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, EXTRATABLE extends Table<EXTRATABLE>>
	void sewExtraTables(ExtraTableJoin<B, RIGHTTABLE, EXTRATABLE, I> extraTableJoin,
	                    SimpleRelationalEntityPersister<C, I, LEFTTABLE> owningPersister,
	                    SimpleRelationalEntityPersister<B, I, RIGHTTABLE> cascader,
	                    Entity<B, I, RIGHTTABLE> entity,
	                    String owningJoinName) {
		SimpleRelationalEntityPersister<B, I, EXTRATABLE> extraTablePersister = buildExtraTablePersister(extraTableJoin, cascader, entity);
		
		owningPersister.getEntityJoinTree().addMergeJoin(
				owningJoinName,
				new EntityMergerAdapter<>(extraTablePersister.getMapping()),
				extraTableJoin.getJoin().getLeftKey(),
				extraTableJoin.getJoin().getRightKey(),
				INNER);
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
