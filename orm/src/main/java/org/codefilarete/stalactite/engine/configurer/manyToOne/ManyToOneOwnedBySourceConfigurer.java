package org.codefilarete.stalactite.engine.configurer.manyToOne;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.RuntimeMappingException;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.manyToOne.ManyToOneRelation.MappedByConfiguration;
import org.codefilarete.stalactite.engine.configurer.onetoone.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.PassiveJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.RelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.manytoone.ManyToOneOwnedBySourceEngine;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.bean.FieldIterator;
import org.codefilarete.tool.bean.InstanceFieldIterator;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <SRCID> identifier type of source entities
 * @param <TRGTID> identifier type of target entities
 * @param <LEFTTABLE> left (source entity) table type
 * @param <RIGHTTABLE> right (target entity) table type
 * @param <JOINID> joining columns type
 * @author Guillaume Mary
 */
public class ManyToOneOwnedBySourceConfigurer<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID> {
	
	private final ConfiguredRelationalPersister<SRC, SRCID> sourcePersister;
	
	private final ManyToOneRelation<SRC, TRGT, TRGTID, Collection<SRC>> manyToOneRelation;
	
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	
	private final Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> keyColumnsMapping = new HashMap<>();
	
	private ManyToOneOwnedBySourceEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> engine;
	
	public ManyToOneOwnedBySourceConfigurer(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
											ManyToOneRelation<SRC, TRGT, TRGTID, ? extends Collection<SRC>> manyToOneRelation,
											JoinColumnNamingStrategy joinColumnNamingStrategy,
											ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this.sourcePersister = sourcePersister;
		this.manyToOneRelation = (ManyToOneRelation<SRC, TRGT, TRGTID, Collection<SRC>>) manyToOneRelation;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
	}
	
	public String configure(String tableAlias,
							ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
							ManyToOneRelation<SRC, TRGT, TRGTID, ?> manyToOneRelation) {
		assertConfigurationIsSupported();
		
		// Finding joined columns
		EntityMapping<TRGT, TRGTID, RIGHTTABLE> targetMappingStrategy = targetPersister.getMapping();
		Duo<Key<LEFTTABLE, JOINID>, Key<RIGHTTABLE, JOINID>> foreignKeyColumns = determineForeignKeyColumns(sourcePersister.getMapping(), targetMappingStrategy, manyToOneRelation.getColumnName());
		
		BeanRelationFixer<SRC, TRGT> beanRelationFixer = determineRelationFixer(targetPersister);
		
		String relationJoinNodeName = addSelectJoin(tableAlias, targetPersister, foreignKeyColumns.getLeft(), foreignKeyColumns.getRight(), beanRelationFixer, manyToOneRelation.isFetchSeparately());
		addWriteCascades(targetPersister);
		return relationJoinNodeName;
	}
	
	public CascadeConfigurationResult<SRC, TRGT> configureWithSelectIn2Phases(String tableAlias,
																			  ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
																			  FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener,
																			  @javax.annotation.Nullable String leftColumnName) {
		assertConfigurationIsSupported();
		
		// Finding joined columns
		EntityMapping<TRGT, TRGTID, RIGHTTABLE> targetMappingStrategy = targetPersister.getMapping();
		Duo<Key<LEFTTABLE, JOINID>, Key<RIGHTTABLE, JOINID>> foreignKeyColumns = determineForeignKeyColumns(sourcePersister.getMapping(), targetMappingStrategy, leftColumnName);
		
		BeanRelationFixer<SRC, TRGT> beanRelationFixer = determineRelationFixer(targetPersister);
		
		addSelectIn2Phases(tableAlias, targetPersister, foreignKeyColumns.getLeft(), foreignKeyColumns.getRight(), firstPhaseCycleLoadListener);
		addWriteCascades(targetPersister);
		return new CascadeConfigurationResult<>(beanRelationFixer, sourcePersister);
	}
	
	private void assertConfigurationIsSupported() {
		RelationMode maintenanceMode = manyToOneRelation.getRelationMode();
		if (maintenanceMode == RelationMode.ASSOCIATION_ONLY) {
			throw new MappingConfigurationException(RelationMode.ASSOCIATION_ONLY + " is only relevant for one-to-many association");
		}
	}
	
	/**
	 * Build the combiner between target entities and source ones.
	 *
	 * @param targetClass target entity type, provided to look up for reverse property if no sufficient info was given
	 * @return null if no information was provided about the reverse side (no bidirectionality)
	 */
	private SerializableBiConsumer<TRGT, SRC> buildReverseCombiner(Class<TRGT> targetClass) {
		MappedByConfiguration<SRC, TRGT, Collection<SRC>> mappedByConfiguration = manyToOneRelation.getMappedByConfiguration();
		if (mappedByConfiguration.isEmpty()) {
			// relation is not bidirectional, and not even set by the reverse link, there's nothing to do
			return null;
		} else {
			PropertyAccessor<TRGT, Collection<SRC>> collectionAccessor = manyToOneRelation.buildReversePropertyAccessor();
			if (collectionAccessor == null) {
				// since some reverse info has been done but not the collection accessor, we try to find the matching property by type
				FieldIterator targetFields = new InstanceFieldIterator(targetClass);
				Class<SRC> sourceEntityType = sourcePersister.getClassToPersist();
				Field reverseField = Iterables.find(targetFields, field -> Collection.class.isAssignableFrom(field.getType())
						&& ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0].equals(sourceEntityType));
				if (reverseField != null) {
					Nullable<AccessorByMethod<TRGT, Collection<SRC>>> reverseGetterMethod = nullable(Accessors.accessorByMethod(reverseField));
					if (reverseGetterMethod.isPresent()) {
						collectionAccessor = new PropertyAccessor<>(reverseGetterMethod.get());
					} else {
						Nullable<MutatorByMethod<TRGT, Collection<SRC>>> reverseSetterMethod = nullable(Accessors.mutatorByMethod(reverseField));
						if (reverseSetterMethod.isPresent()) {
							collectionAccessor = new PropertyAccessor<>(reverseSetterMethod.get());
						}
					}
				} // else : relation is not bidirectional, or not a usual one, may be set by reverse link
			}
			
			Nullable<SerializableBiConsumer<TRGT, SRC>> configuredCombiner = nullable(mappedByConfiguration.getCombiner());
			if (collectionAccessor == null) {
				return configuredCombiner.get();
			} else {
				// collection factory is in priority the one configured
				Supplier<Collection<SRC>> collectionFactory = mappedByConfiguration.getFactory();
				if (collectionFactory == null) {
					Class<Collection<SRC>> collectionType = AccessorDefinition.giveDefinition(collectionAccessor).getMemberType();
					collectionFactory = Reflections.giveCollectionFactory(collectionType);
				}
				PropertyAccessor<TRGT, Collection<SRC>> finalCollectionAccessor = collectionAccessor;
				SerializableBiConsumer<TRGT, SRC> combiner = configuredCombiner.getOr((TRGT trgt, SRC src) -> {
					// collectionAccessor can't be null due to nullable check
					finalCollectionAccessor.get(trgt).add(src);
				});
				
				Supplier<Collection<SRC>> effectiveCollectionFactory = collectionFactory;
				return (TRGT trgt, SRC src) -> {
					// we call the collection factory to ensure that property is initialized
					if (finalCollectionAccessor.get(trgt) == null) {
						finalCollectionAccessor.set(trgt, effectiveCollectionFactory.get());
					}
					// Note that combiner can't be null here thanks to nullable(..) check
					combiner.accept(trgt, src);
				};
			}
		}
	}
	
	protected Duo<Key<LEFTTABLE, JOINID>, Key<RIGHTTABLE, JOINID>> determineForeignKeyColumns(EntityMapping<SRC, SRCID, LEFTTABLE> mappingStrategy,
																							  EntityMapping<TRGT, TRGTID, RIGHTTABLE> targetMappingStrategy,
																							  String leftColumnName) {
		Key<RIGHTTABLE, JOINID> rightKey = targetMappingStrategy.getTargetTable().getPrimaryKey();
		// adding foreign key constraint
		KeyBuilder<LEFTTABLE, JOINID> leftKeyBuilder = Key.from(mappingStrategy.getTargetTable());
		AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(manyToOneRelation.getTargetProvider());
		targetMappingStrategy.getTargetTable().getPrimaryKey().getColumns().forEach(column -> {
			String effectiveLeftColumnName = nullable(leftColumnName).elseSet(() -> joinColumnNamingStrategy.giveName(accessorDefinition, column)).get();
			Column<LEFTTABLE, ?> foreignKeyColumn = mappingStrategy.getTargetTable().addColumn(effectiveLeftColumnName, column.getJavaType());
			leftKeyBuilder.addColumn(foreignKeyColumn);
			keyColumnsMapping.put(foreignKeyColumn, column);
		});
		Key<LEFTTABLE, JOINID> leftKey = leftKeyBuilder.build();
		
		// According to the nullable option, we specify the ddl schema option
		leftKey.getColumns().forEach(c -> ((Column) c).nullable(manyToOneRelation.isNullable()));
		
		// we don't create foreign key for table-per-class because source columns should reference different tables (the one
		// per entity) which databases do not allow
		boolean createForeignKey = !manyToOneRelation.isTargetTablePerClassPolymorphic();
		if (createForeignKey) {
			String foreignKeyName = foreignKeyNamingStrategy.giveName(leftKey, rightKey);
			sourcePersister.<LEFTTABLE>getMapping().getTargetTable().addForeignKey(foreignKeyName, leftKey, rightKey);
		}
		
		return new Duo<>(leftKey, rightKey);
	}
	
	protected BeanRelationFixer<SRC, TRGT> determineRelationFixer(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		Mutator<SRC, TRGT> targetSetter = manyToOneRelation.getTargetProvider().toMutator();
		SerializableBiConsumer<TRGT, SRC> reverseCombiner = buildReverseCombiner(targetPersister.getClassToPersist());
		
		if (reverseCombiner == null) {
			return BeanRelationFixer.of(targetSetter::set);
		} else {
			return (target, input) -> {
				targetSetter.set(target, input);
				if (reverseCombiner != null) {
					reverseCombiner.accept(input, target);
				}
			};
		}
	}
	
	protected void addWriteCascades(ConfiguredPersister<TRGT, TRGTID> targetPersister) {
		this.engine = new ManyToOneOwnedBySourceEngine<>(sourcePersister, targetPersister, manyToOneRelation.getTargetProvider(), keyColumnsMapping);
		boolean orphanRemoval = manyToOneRelation.getRelationMode() == RelationMode.ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = manyToOneRelation.getRelationMode() != RelationMode.READ_ONLY;
		if (writeAuthorized) {
			// NB: "delete removed" will be treated internally by updateCascade() and deleteCascade()
			addInsertCascade();
			addUpdateCascade(orphanRemoval);
			addDeleteCascade(orphanRemoval);
		} else {
			// even if write is not authorized, we still have to insert and update source-to-target link, because we are in relation-owned-by-source
			this.engine.addForeignKeyMaintainer();
		}
	}
	
	protected void addInsertCascade() {
		// if cascade is mandatory, then adding nullability checking before insert
		if (!manyToOneRelation.isNullable()) {
			sourcePersister.addInsertListener(new MandatoryRelationAssertBeforeInsertListener<>(manyToOneRelation.getTargetProvider()));
		}
		engine.addInsertCascade();
	}
	
	protected void addUpdateCascade(boolean orphanRemoval) {
		// if cascade is mandatory, then adding nullability checking before insert
		if (!manyToOneRelation.isNullable()) {
			sourcePersister.addUpdateListener(new MandatoryRelationAssertBeforeUpdateListener<>(manyToOneRelation.getTargetProvider()));
		}
		engine.addUpdateCascade(orphanRemoval);
	}
	
	protected void addDeleteCascade(boolean orphanRemoval) {
		engine.addDeleteCascade(orphanRemoval);
	}
	
	protected String addSelectJoin(
			String tableAlias,
			ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
			Key<LEFTTABLE, JOINID> leftKey,
			Key<RIGHTTABLE, JOINID> rightKey,
			BeanRelationFixer<SRC, TRGT> beanRelationFixer,
			boolean loadSeparately) {
		// we add target subgraph joins to the one that was created
		String joinNodeName = targetPersister.joinAsOne(sourcePersister, manyToOneRelation.getTargetProvider(), leftKey, rightKey, tableAlias, beanRelationFixer, true, loadSeparately);
		
		// We trigger subgraph load event (via targetSelectListener) on loading of our graph.
		// Done for instance for event consumers that initialize some things, because given ids of methods are those of source entity
		SelectListener<TRGT, TRGTID> targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				// since ids are not those of its entities, we should not pass them as argument, this will only initialize things if needed
				targetSelectListener.beforeSelect(Collections.emptyList());
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				Set<TRGT> collect = Iterables.collect(result, manyToOneRelation.getTargetProvider()::get, HashSet::new);
				// NB: entity can be null when loading relation, we skip nulls to prevent a NPE
				collect.removeIf(Objects::isNull);
				targetSelectListener.afterSelect(collect);
			}
			
			@Override
			public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
				// since ids are not those of its entities, we should not pass them as argument
				targetSelectListener.onSelectError(Collections.emptyList(), exception);
			}
		});
		return joinNodeName;
	}
	
	protected void addSelectIn2Phases(
			String tableAlias,
			ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
			Key<LEFTTABLE, JOINID> leftKey,
			Key<RIGHTTABLE, JOINID> rightKey,
			FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		
		Table targetTableClone = new Table(targetPersister.getMapping().getTargetTable().getName());
		KeepOrderSet<Column> columns = (KeepOrderSet<Column>) (KeepOrderSet) rightKey.getColumns();
		columns.forEach(column -> targetTableClone.addColumn(column.getExpression(), column.getJavaType()).primaryKey());
		// This can't be done directly on root persister (took via persisterRegistry and targetPersister.getClassToPersist()) because
		// TransformerListener would get root instance as source (aggregate root), not current source
		String joinName = sourcePersister.getEntityJoinTree().addPassiveJoin(
				EntityJoinTree.ROOT_JOIN_NAME,
				leftKey,
				targetTableClone.getPrimaryKey(),
				tableAlias,
				manyToOneRelation.isNullable() ? JoinType.OUTER : JoinType.INNER,
				targetTableClone.getPrimaryKey().getColumns(),
				(src, columnValueProvider) -> firstPhaseCycleLoadListener.onFirstPhaseRowRead(src, targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnValueProvider)),
				true);
		
		// Propagating 2-phases load to all nodes that use cycling type
		PassiveJoinNode passiveJoin = (PassiveJoinNode) sourcePersister.getEntityJoinTree().getJoin(joinName);
		targetPersister.getEntityJoinTree().foreachJoin(joinNode -> {
			if (joinNode instanceof RelationJoinNode
					&& ((RelationJoinNode<?, ?, ?, ?, ?>) joinNode).getEntityInflater().getEntityType() == sourcePersister.getClassToPersist()) {
				EntityJoinTree.cloneNodeForParent(passiveJoin, joinNode, leftKey);
			}
		});
	}
	
	public static class MandatoryRelationAssertBeforeInsertListener<C> implements InsertListener<C> {
		
		private final Accessor<C, ?> targetAccessor;
		
		public MandatoryRelationAssertBeforeInsertListener(Accessor<C, ?> targetAccessor) {
			this.targetAccessor = targetAccessor;
		}
		
		@Override
		public void beforeInsert(Iterable<? extends C> entities) {
			for (C pawn : entities) {
				Object modifiedTarget = targetAccessor.get(pawn);
				if (modifiedTarget == null) {
					throw newRuntimeMappingException(pawn, targetAccessor);
				}
			}
		}
	}
	
	public static class MandatoryRelationAssertBeforeUpdateListener<C> implements UpdateListener<C> {
		
		private final Accessor<C, ?> targetAccessor;
		
		public MandatoryRelationAssertBeforeUpdateListener(Accessor<C, ?> targetAccessor) {
			this.targetAccessor = targetAccessor;
		}
		
		@Override
		public void beforeUpdate(Iterable<? extends Duo<C, C>> payloads, boolean allColumnsStatement) {
			for (Duo<? extends C, ? extends C> payload : payloads) {
				C modifiedEntity = payload.getLeft();
				Object modifiedTarget = targetAccessor.get(modifiedEntity);
				if (modifiedTarget == null) {
					throw newRuntimeMappingException(modifiedEntity, targetAccessor);
				}
			}
		}
	}
	
	public static RuntimeMappingException newRuntimeMappingException(Object pawn, ValueAccessPoint<?> accessor) {
		return new RuntimeMappingException("Non null value expected for relation "
				+ AccessorDefinition.toString(accessor) + " on object " + pawn);
	}
	
}
