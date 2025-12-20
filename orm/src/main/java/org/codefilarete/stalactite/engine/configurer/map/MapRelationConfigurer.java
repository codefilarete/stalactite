package org.codefilarete.stalactite.engine.configurer.map;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.IndexNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableLinkage;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMapping;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingBuilder;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.AfterUpdateTrigger;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.PairIterator;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Configurer for a {@link Map} relation
 * 
 * @author Guillaume Mary
 */
public class MapRelationConfigurer<SRC, ID, K, V, M extends Map<K, V>> {
	
	private static final AccessorDefinition KEY_VALUE_RECORD_ID_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(
			new AccessorByMethodReference<>(KeyValueRecord<Object, Object, Object>::getId));
	protected static final AccessorDefinition ENTRY_KEY_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(new AccessorByMethodReference<Entry<Object, Object>, Object>(Entry::getKey));
	protected static final AccessorDefinition ENTRY_VALUE_ACCESSOR_DEFINITION = AccessorDefinition.giveDefinition(new AccessorByMethodReference<Entry<Object, Object>, Object>(Entry::getValue));
	
	protected final MapRelation<SRC, K, V, M> mapRelation;
	protected final ConfiguredRelationalPersister<SRC, ID> sourcePersister;
	protected final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	protected final ColumnNamingStrategy columnNamingStrategy;
	protected final MapEntryTableNamingStrategy tableNamingStrategy;
	protected final Dialect dialect;
	protected final ConnectionConfiguration connectionConfiguration;
	protected final IndexNamingStrategy indexNamingStrategy;

	public MapRelationConfigurer(MapRelation<SRC, K, V, M> mapRelation,
								 ConfiguredRelationalPersister<SRC, ID> sourcePersister,
								 ForeignKeyNamingStrategy foreignKeyNamingStrategy,
								 ColumnNamingStrategy columnNamingStrategy,
								 MapEntryTableNamingStrategy tableNamingStrategy,
								 Dialect dialect,
								 ConnectionConfiguration connectionConfiguration,
								 IndexNamingStrategy indexNamingStrategy) {
		this.mapRelation = mapRelation;
		this.sourcePersister = sourcePersister;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.columnNamingStrategy = columnNamingStrategy;
		this.tableNamingStrategy = tableNamingStrategy;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.indexNamingStrategy = indexNamingStrategy;
	}
	
	public <SRCTABLE extends Table<SRCTABLE>, MAPTABLE extends Table<MAPTABLE>> void configure() {
		
		AccessorDefinition mapProviderDefinition = AccessorDefinition.giveDefinition(mapRelation.getMapProvider());
		// schema configuration
		PrimaryKey<SRCTABLE, ID> sourcePK = sourcePersister.<SRCTABLE>getMapping().getTargetTable().getPrimaryKey();
		
		// Note that the table will participate to DDL due to select cascading and thus its join in the whole entity graph
		MAPTABLE targetTable = nullable((MAPTABLE) mapRelation.getTargetTable()).getOr(() -> {
			String tableName = nullable(mapRelation.getTargetTableName()).getOr(() -> tableNamingStrategy.giveTableName(mapProviderDefinition, mapRelation.getKeyType(), mapRelation.getValueType()));
			return (MAPTABLE) new Table(tableName);
		});
		Map<Column<SRCTABLE, ?>, Column<MAPTABLE, ?>> srcPrimaryKeyToForeignKeyColumns = new HashMap<>();
		Key<MAPTABLE, ID> reverseKey = nullable((Column<MAPTABLE, ID>) (Column) mapRelation.getReverseColumn()).map(Key::ofSingleColumn)
				.getOr(() -> {
					KeyBuilder<MAPTABLE, ID> result = Key.from(targetTable);
					sourcePK.getColumns().forEach(col -> {
						String reverseColumnName = nullable(mapRelation.getReverseColumnName()).getOr(() ->
								columnNamingStrategy.giveName(KEY_VALUE_RECORD_ID_ACCESSOR_DEFINITION));
						Column<MAPTABLE, ?> reverseCol = targetTable.addColumn(reverseColumnName, col.getJavaType())
								.primaryKey();
						srcPrimaryKeyToForeignKeyColumns.put(col, reverseCol);
						result.addColumn(reverseCol);
					});
					return result.build();
				});
		ForeignKey<MAPTABLE, SRCTABLE, ID> reverseForeignKey = targetTable.addForeignKey(this.foreignKeyNamingStrategy::giveName, reverseKey, sourcePK);
		
		EmbeddableMappingConfiguration<K> keyEmbeddableConfiguration =
				nullable(mapRelation.getKeyEmbeddableConfigurationProvider()).map(EmbeddableMappingConfigurationProvider::getConfiguration).get();
		EmbeddableMappingConfiguration<V> valueEmbeddableConfiguration =
				nullable(mapRelation.getValueEmbeddableConfigurationProvider()).map(EmbeddableMappingConfigurationProvider::getConfiguration).get();
		DefaultEntityMapping<KeyValueRecord<K, V, ID>, RecordId<K, ID>, MAPTABLE> relationRecordMapping;
		IdentifierAssembler<ID, SRCTABLE> sourceIdentifierAssembler = sourcePersister.getMapping().getIdMapping().getIdentifierAssembler();
		relationRecordMapping = buildKeyValueRecordMapping(targetTable, sourceIdentifierAssembler, srcPrimaryKeyToForeignKeyColumns, keyEmbeddableConfiguration, valueEmbeddableConfiguration);
		
		SimpleRelationalEntityPersister<KeyValueRecord<K, V, ID>, RecordId<K, ID>, MAPTABLE> relationRecordPersister =
				new SimpleRelationalEntityPersister<>(relationRecordMapping, dialect, connectionConfiguration);
		
		// insert management
		Accessor<SRC, M> collectionAccessor = mapRelation.getMapProvider();
		addInsertCascade(sourcePersister, relationRecordPersister, collectionAccessor);
		
		// update management
		addUpdateCascade(sourcePersister, relationRecordPersister);

		// delete management (we provide persisted instances so they are perceived as deletable)
		addDeleteCascade(sourcePersister, relationRecordPersister);

		// select management
		Supplier<M> collectionFactory = preventNull(
				mapRelation.getMapFactory(),
				Reflections.giveMapFactory((Class<M>) mapProviderDefinition.getMemberType()));
		addSelectCascade(sourcePersister, relationRecordPersister, sourcePK, reverseForeignKey,
				mapRelation.getMapProvider().toMutator()::set, collectionAccessor,
				collectionFactory);
	}
	
	<SRCTABLE extends Table<SRCTABLE>, MAPTABLE extends Table<MAPTABLE>>
	DefaultEntityMapping<KeyValueRecord<K, V, ID>, RecordId<K, ID>, MAPTABLE>
	buildKeyValueRecordMapping(MAPTABLE targetTable,
							   IdentifierAssembler<ID, SRCTABLE> sourceIdentifierAssembler,
							   Map<Column<SRCTABLE, ?>, Column<MAPTABLE, ?>> srcPrimaryKeyToForeignKeyColumns,
							   EmbeddableMappingConfiguration<K> keyEmbeddableConfiguration,
							   EmbeddableMappingConfiguration<V> valueEmbeddableConfiguration) {
		KeyValueRecordMappingBuilder<K, V, ID, MAPTABLE, SRCTABLE> builder = new KeyValueRecordMappingBuilder<>(targetTable, sourceIdentifierAssembler, srcPrimaryKeyToForeignKeyColumns);
		return buildKeyValueRecordMapping(keyEmbeddableConfiguration, targetTable, builder, valueEmbeddableConfiguration);
	}
	
	<SRCTABLE extends Table<SRCTABLE>, MAPTABLE extends Table<MAPTABLE>>
	DefaultEntityMapping<KeyValueRecord<K, V, ID>, RecordId<K, ID>, MAPTABLE>
	buildKeyValueRecordMapping(EmbeddableMappingConfiguration<K> keyEmbeddableConfiguration,
							   MAPTABLE targetTable,
							   KeyValueRecordMappingBuilder<K, V, ID, MAPTABLE, SRCTABLE> builder,
							   EmbeddableMappingConfiguration<V> valueEmbeddableConfiguration) {
		DefaultEntityMapping<KeyValueRecord<K, V, ID>, RecordId<K, ID>, MAPTABLE> relationRecordMapping;
		if (keyEmbeddableConfiguration == null) {
			String keyColumnName = nullable(mapRelation.getKeyColumnName())
					.getOr(() -> columnNamingStrategy.giveName(ENTRY_KEY_ACCESSOR_DEFINITION));
			Column<MAPTABLE, K> keyColumn = targetTable.addColumn(keyColumnName, mapRelation.getKeyType(), mapRelation.getKeyColumnSize())
					.primaryKey();
			builder.withEntryKeyIsSingleProperty(keyColumn);
		} else {
			// a special configuration was given, we compute a EmbeddedClassMapping from it
			EmbeddableMappingBuilder<K, MAPTABLE> entryKeyMappingBuilder = new EmbeddableMappingBuilder<K, MAPTABLE>(keyEmbeddableConfiguration, targetTable,
					dialect.getColumnBinderRegistry(), columnNamingStrategy, indexNamingStrategy) {
				@Override
				protected <O> String determineColumnName(EmbeddableLinkage<K, O> linkage, @Nullable String overriddenColumName) {
					return super.determineColumnName(linkage, mapRelation.getOverriddenKeyColumnNames().get(linkage.getAccessor()));
				}
				
				@Override
				protected <O> Size determineColumnSize(EmbeddableLinkage<K, O> linkage, @Nullable Size overriddenColumSize) {
					return super.determineColumnSize(linkage, mapRelation.getOverriddenKeyColumnSizes().get(linkage.getAccessor()));
				}
			};
			EmbeddableMapping<K, MAPTABLE> entryKeyMapping = entryKeyMappingBuilder.build();
			Map<ReversibleAccessor<K, Object>, Column<MAPTABLE, Object>> columnMapping = entryKeyMapping.getMapping();
			
			columnMapping.forEach((propertyAccessor, column) -> column.primaryKey());
			builder.withEntryKeyIsComplexType(new EmbeddedClassMapping<>(keyEmbeddableConfiguration.getBeanType(), targetTable, columnMapping));
		}
		if (valueEmbeddableConfiguration == null) {
			String valueColumnName = nullable(mapRelation.getValueColumnName())
					.getOr(() -> columnNamingStrategy.giveName(ENTRY_VALUE_ACCESSOR_DEFINITION));
			Column<MAPTABLE, V> valueColumn = targetTable.addColumn(valueColumnName, mapRelation.getValueType(), mapRelation.getValueColumnSize());
			builder.withEntryValueIsSingleProperty(valueColumn);
		} else {
			// a special configuration was given, we compute a EmbeddedClassMapping from it
			EmbeddableMappingBuilder<V, MAPTABLE> recordKeyMappingBuilder = new EmbeddableMappingBuilder<V, MAPTABLE>(valueEmbeddableConfiguration, targetTable,
					dialect.getColumnBinderRegistry(), columnNamingStrategy, indexNamingStrategy) {
				@Override
				protected <O> String determineColumnName(EmbeddableLinkage<V, O> linkage, @Nullable String overriddenColumName) {
					return super.determineColumnName(linkage, mapRelation.getOverriddenValueColumnNames().get(linkage.getAccessor()));
				}
				
				@Override
				protected <O> Size determineColumnSize(EmbeddableLinkage<V, O> linkage, @Nullable Size overriddenColumSize) {
					return super.determineColumnSize(linkage, mapRelation.getOverriddenValueColumnSizes().get(linkage.getAccessor()));
				}
			};
			EmbeddableMapping<V, MAPTABLE> entryValueMapping = recordKeyMappingBuilder.build();
			Map<ReversibleAccessor<V, Object>, Column<MAPTABLE, Object>> columnMapping = entryValueMapping.getMapping();
			
			builder.withEntryValueIsComplexType(new EmbeddedClassMapping<>(valueEmbeddableConfiguration.getBeanType(), targetTable, columnMapping));
		}
		relationRecordMapping = builder.build();
		return relationRecordMapping;
	}
	
	protected void addInsertCascade(ConfiguredRelationalPersister<SRC, ID> sourcePersister,
									EntityPersister<KeyValueRecord<K, V, ID>, RecordId<K, ID>> relationRecordPersister,
									Accessor<SRC, M> collectionAccessor) {
		Function<SRC, Collection<KeyValueRecord<K, V, ID>>> collectionProviderForInsert = toRecordCollectionProvider(
				sourcePersister.getMapping(),
				false);
		
		sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(relationRecordPersister, collectionProviderForInsert));
	}
	
	protected void addUpdateCascade(ConfiguredRelationalPersister<SRC, ID> sourcePersister,
									EntityPersister<KeyValueRecord<K, V, ID>, RecordId<K, ID>> relationRecordPersister) {
		Function<SRC, Collection<KeyValueRecord<K, V, ID>>> collectionProviderAsPersistedInstances = toRecordCollectionProvider(
				sourcePersister.getMapping(),
				true);
		
		BiConsumer<Duo<SRC, SRC>, Boolean> collectionUpdater = new CollectionUpdater<SRC, KeyValueRecord<K, V, ID>, Collection<KeyValueRecord<K, V, ID>>>(
				collectionProviderAsPersistedInstances,
				relationRecordPersister,
				(o, i) -> { /* no reverse setter because we store only raw values */ },
				true,
				// we base our id policy on a particular identifier because Id is all the same for KeyValueRecord (it is source bean id)
				KeyValueRecord::footprint) {
			
			/**
			 * Overridden to force insertion of added entities because as a difference with default behavior (parent class), collection elements are
			 * not entities, so they can't be moved from a collection to another, hence they don't need to be updated, therefore there's no need to
			 * use {@code getElementPersister().persist(..)} mechanism. Even more : it is counterproductive (meaning false) because
			 * {@code persist(..)} uses {@code update(..)} when entities are considered already persisted (not {@code isNew()}), which is always the
			 * case for new {@link KeyValueRecord}
			 */
			@Override
			protected void insertTargets(UpdateContext updateContext) {
				relationRecordPersister.insert(updateContext.getAddedElements());
			}
		};
		
		sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(collectionUpdater));
	}
	
	protected void addDeleteCascade(ConfiguredRelationalPersister<SRC, ID> sourcePersister,
								  EntityPersister<KeyValueRecord<K, V, ID>, RecordId<K, ID>> relationRecordPersister) {
		Function<SRC, Collection<KeyValueRecord<K, V, ID>>> recordsProviderAsPersistedInstances = toRecordCollectionProvider(
				sourcePersister.getMapping(),
				true);
		
		sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(relationRecordPersister, recordsProviderAsPersistedInstances));
	}
	
	protected void addSelectCascade(ConfiguredRelationalPersister<SRC, ID> sourcePersister,
									SimpleRelationalEntityPersister<KeyValueRecord<K, V, ID>, RecordId<K, ID>, ?> relationRecordPersister,
									PrimaryKey<?, ID> sourcePK,
									ForeignKey<?, ?, ID> keyValueRecordToSourceForeignKey,
									BiConsumer<SRC, M> mapSetter,
									Accessor<SRC, M> mapGetter,
									Supplier<M> mapFactory) {
		// a particular Map fixer that gets raw values (Map entries) from KeyValueRecord
		// because elementRecordPersister manages KeyValueRecord, so it gives them as input of the relation,
		// hence an adaption is needed to "convert" it
		BeanRelationFixer<SRC, KeyValueRecord<K, V, ID>> relationFixer = BeanRelationFixer.ofMapAdapter(
				mapSetter,
				mapGetter::get,
				mapFactory,
				(bean, input, map) -> map.put(input.getKey(), input.getValue()));
		
		relationRecordPersister.joinAsMany(
				EntityJoinTree.ROOT_JOIN_NAME,
				sourcePersister,
				mapGetter,
                sourcePK,
                keyValueRecordToSourceForeignKey,
                relationFixer,
                null,
                true,
				mapRelation.isFetchSeparately());
	}
	
	/**
	 * Transforms given mapAccessor to provider of a collection of {@link KeyValueRecord} that contains {@link Map} entries
	 * converted to {@link KeyValueRecord}
	 * 
	 * @param idAccessor identifier accessor of entity declaring the relation, used to set the referential integrity
	 * @param markAsPersisted should we set generated {@link KeyValueRecord} as persisted ?
	 * @return a provider of {@link Map} entries converted to {@link KeyValueRecord}s
	 */
	protected Function<SRC, Collection<KeyValueRecord<K, V, ID>>> toRecordCollectionProvider(IdAccessor<SRC, ID> idAccessor,
																							 boolean markAsPersisted) {
		return src -> Iterables.collect(nullable(mapRelation.getMapProvider().get(src)).getOr(() -> (M) Collections.emptyMap()).entrySet(),
				entry -> new KeyValueRecord<>(idAccessor.getId(src), entry.getKey(), entry.getValue()).setPersisted(markAsPersisted),
				HashSet::new);
	}
	
	protected static class TargetInstancesInsertCascader<SRC, K, V, ID> extends AfterInsertCollectionCascader<SRC, KeyValueRecord<K, V, ID>> {
		
		private final Function<SRC, ? extends Collection<KeyValueRecord<K, V, ID>>> mapGetter;
		
		public TargetInstancesInsertCascader(EntityPersister<KeyValueRecord<K, V, ID>, RecordId<K, ID>> targetPersister,
											 Function<SRC, ? extends Collection<KeyValueRecord<K, V, ID>>> mapGetter) {
			super(targetPersister);
			this.mapGetter = mapGetter;
		}
		
		@Override
		protected void postTargetInsert(Iterable<? extends KeyValueRecord<K, V, ID>> entities) {
			// Nothing to do. Identified#isPersisted flag should be fixed by target persister
		}
		
		@Override
		protected Collection<KeyValueRecord<K, V, ID>> getTargets(SRC source) {
			return mapGetter.apply(source);
		}
	}
}
