package org.codefilarete.stalactite.engine.configurer.map;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.IndexNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteCollectionCascader;
import org.codefilarete.stalactite.engine.cascade.BeforeInsertCollectionCascader;
import org.codefilarete.stalactite.engine.configurer.DefaultComposedIdentifierAssembler;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableLinkage;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMapping;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingBuilder;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.AfterUpdateTrigger;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
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
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.function.Functions.NullProofFunction;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;
import static org.codefilarete.tool.Nullable.nullable;

/**
 * Handle particular case of {@link MapRelationConfigurer} when Map key is an entity : it requires some cascading
 * with key-entity table and select handling.
 * Design of this class is to inherit from simple case letting parent class handle the relation as if it was a usual
 * Map made of simple beans. Then current class "has only" to manage cascading and entity construction at selection time. 
 * 
 * @param <SRC> entity type owning the relation
 * @param <SRCID> entity owning the relation identifier type 
 * @param <K> Map key entity type
 * @param <KID> Map key entity identifier type
 * @param <V> Map value type
 * @param <M> relation Map type
 * @param <MM> redefined Map type to get entity key identifier 
 * @author Guillaume Mary
 */
public class EntityAsKeyMapRelationConfigurer<SRC, SRCID, K, KID, V, M extends Map<K, V>, MM extends Map<KID, V>> extends MapRelationConfigurer<SRC, SRCID, KID, V, MM> {
	
	private static <SRC, K, KID, V, M extends Map<K, V>, MM extends Map<KID, V>> MapRelation<SRC, KID, V, MM> convertEntityMapToIdentifierMap(
			MapRelation<SRC, K, V, M> mapRelation,
			ConfiguredRelationalPersister<K, KID> keyEntityPersister) {
		ConvertingMapAccessor<SRC, K, V, KID, V, M, MM> mapAccessor = new ConvertingMapAccessor<>(mapRelation, (k, v, result) -> result.put(keyEntityPersister.getId(k), v));
		PropertyAccessor<SRC, MM> propertyAccessor = new PropertyAccessor<>(
				mapAccessor,
				(src, mm) -> {
					// No setter need because afterSelect(..) method is in charge of setting the values (too complex to be done here)
					// Don't give null Mutator to avoir NPE later
				}
		);
		return new MapRelation<>(
				propertyAccessor,
				keyEntityPersister.getMapping().getIdMapping().getIdentifierInsertionManager().getIdentifierType(),
				mapRelation.getValueType());
	}
	
	
	private final MapRelation<SRC, K, V, M> originalMapRelation;
	private final ConfiguredRelationalPersister<K, KID> keyEntityPersister;
	private final Function<SRC, M> mapGetter;
	private final InMemoryRelationHolder<SRCID, KID, V, K> inMemoryRelationHolder;
	private Key<?, KID> keyIdColumnsProjectInAssociationTable;
	private final RelationMode maintenanceMode;
	
	public EntityAsKeyMapRelationConfigurer(
			MapRelation<SRC, K, V, M> mapRelation,
			ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
			ConfiguredRelationalPersister<K, KID> keyEntityPersister,
			ForeignKeyNamingStrategy foreignKeyNamingStrategy,
			ColumnNamingStrategy columnNamingStrategy,
			MapEntryTableNamingStrategy tableNamingStrategy,
			Dialect dialect,
			ConnectionConfiguration connectionConfiguration,
			IndexNamingStrategy indexNamingStrategy) {
		super(convertEntityMapToIdentifierMap(mapRelation, keyEntityPersister),
				sourcePersister,
				foreignKeyNamingStrategy,
				columnNamingStrategy,
				tableNamingStrategy,
				dialect,
				connectionConfiguration,
				indexNamingStrategy);
		this.originalMapRelation = mapRelation;
		this.keyEntityPersister = keyEntityPersister;
		this.mapGetter = originalMapRelation.getMapProvider()::get;
		this.inMemoryRelationHolder = new InMemoryRelationHolder<>(trio -> new Duo<>(trio.getEntity(), trio.getEntryValue()));
		this.maintenanceMode = mapRelation.getKeyEntityRelationMode();
	}
	
	@Override
	public void configure() {
		
		AccessorDefinition mapProviderDefinition = AccessorDefinition.giveDefinition(originalMapRelation.getMapProvider());
		Supplier<M> mapFactory = Reflections.giveMapFactory((Class<M>) mapProviderDefinition.getMemberType());
		
		// Finally put elements into source entities by converting in-memory stored objects as Map entries.
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				inMemoryRelationHolder.init();
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				BeanRelationFixer<SRC, Duo<K, V>> originalRelationFixer = BeanRelationFixer.ofMapAdapter(
						originalMapRelation.getMapProvider().toMutator()::set,
						mapGetter,
						mapFactory,
						(bean, duo, map) -> map.put(duo.getLeft(), duo.getRight()));
				result.forEach(bean -> {
					Collection<Duo<K, V>> keyValuePairs = (Collection) inMemoryRelationHolder.get(sourcePersister.getId(bean));
					if (keyValuePairs != null) {
						keyValuePairs.forEach(duo -> originalRelationFixer.apply(bean, duo));
					} // else : no association record
				});
				
				inMemoryRelationHolder.clear();
			}
		});
		
		super.configure();
	}
	
	@Override
	<SRCTABLE extends Table<SRCTABLE>, MAPTABLE extends Table<MAPTABLE>>
	DefaultEntityMapping<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>, MAPTABLE>
	buildKeyValueRecordMapping(MAPTABLE targetTable,
							   IdentifierAssembler<SRCID, SRCTABLE> sourceIdentifierAssembler,
							   Map<Column<SRCTABLE, ?>, Column<MAPTABLE, ?>> srcPrimaryKeyToForeignKeyColumns,
							   EmbeddableMappingConfiguration<KID> keyEmbeddableConfiguration,
							   EmbeddableMappingConfiguration<V> valueEmbeddableConfiguration) {
		KeyValueRecordMappingBuilder<KID, V, SRCID, MAPTABLE, SRCTABLE> builder
				= new KeyValueRecordMappingBuilder<KID, V, SRCID, MAPTABLE, SRCTABLE>(targetTable, sourceIdentifierAssembler, srcPrimaryKeyToForeignKeyColumns) {
			
			private final Map<Column<MAPTABLE, ?>, Column<Table, ?>> foreignKeyBootstrap = new KeepOrderMap<>();
			
			@Override
			void withEntryKeyIsSingleProperty(Column<MAPTABLE, KID> keyColumn) {
				super.withEntryKeyIsSingleProperty(keyColumn);
				Column<Table, Object> column = ((SimpleIdMapping) keyEntityPersister.getMapping().getIdMapping()).getIdentifierAssembler().getColumn();
				foreignKeyBootstrap.put(keyColumn, column);
				keyIdColumnsProjectInAssociationTable = Key.ofSingleColumn(keyColumn);
			}
			
			@Override
			void withEntryKeyIsComplexType(EmbeddedClassMapping<KID, MAPTABLE> entryKeyMapping) {
				super.withEntryKeyIsComplexType(entryKeyMapping);
				KeyBuilder<MAPTABLE, KID> keyIdColumnsProjectInAssociationTableBuilder = Key.from(targetTable);
				entryKeyMapping.getPropertyToColumn().values().forEach(keyIdColumnsProjectInAssociationTableBuilder::addColumn);
				keyIdColumnsProjectInAssociationTable = keyIdColumnsProjectInAssociationTableBuilder.build();
				entryKeyMapping.getPropertyToColumn().values().forEach(associationColumn -> {
					Column<Table, ?> rightColumn = keyEntityPersister.<Table>getMainTable().getColumn(associationColumn.getExpression());
					foreignKeyBootstrap.put(associationColumn, rightColumn);
				});
			}
			
			@Override
			KeyValueRecordMapping<KID, V, SRCID, MAPTABLE> build() {
				KeyBuilder<MAPTABLE, Object> keyBuilder1 = Key.from(targetTable);
				KeyBuilder<Table, Object> keyBuilder2 = Key.from(keyEntityPersister.<Table>getMainTable());
				foreignKeyBootstrap.forEach((key, value) -> {
					keyBuilder1.addColumn(key);
					keyBuilder2.addColumn(value);
				});
				targetTable.addForeignKey(foreignKeyNamingStrategy::giveName, keyBuilder1.build(), keyBuilder2.build());
				return super.build();
			}
		};
		
		if (valueEmbeddableConfiguration == null) {
			String valueColumnName = nullable(mapRelation.getValueColumnName())
					.getOr(() -> columnNamingStrategy.giveName(ENTRY_VALUE_ACCESSOR_DEFINITION));
			Column<MAPTABLE, V> valueColumn = targetTable.addColumn(valueColumnName, mapRelation.getValueType(), mapRelation.getValueColumnSize());
			builder.withEntryValueIsSingleProperty(valueColumn);
		} else {
			// a special configuration was given, we compute a EmbeddedClassMapping from it
			EmbeddableMappingBuilder<V, MAPTABLE> entryKeyMappingBuilder = new EmbeddableMappingBuilder<V, MAPTABLE>(valueEmbeddableConfiguration, targetTable,
					dialect.getColumnBinderRegistry(), columnNamingStrategy, indexNamingStrategy) {
				@Override
				protected <O> String determineColumnName(EmbeddableLinkage<V, O> linkage, @Nullable String overriddenColumName) {
					return super.determineColumnName(linkage, mapRelation.getOverriddenKeyColumnNames().get(linkage.getAccessor()));
				}
				
				@Override
				protected <O> Size determineColumnSize(EmbeddableLinkage<V, O> linkage, @Nullable Size overriddenColumSize) {
					return super.determineColumnSize(linkage, mapRelation.getOverriddenKeyColumnSizes().get(linkage.getAccessor()));
				}
			};
			EmbeddableMapping<V, MAPTABLE> entryKeyMapping = entryKeyMappingBuilder.build();
			Map<ReversibleAccessor<V, Object>, Column<MAPTABLE, Object>> columnMapping = entryKeyMapping.getMapping();
			
			columnMapping.forEach((propertyAccessor, column) -> column.primaryKey());
			builder.withEntryValueIsComplexType(new EmbeddedClassMapping<>(valueEmbeddableConfiguration.getBeanType(), targetTable, columnMapping));
		}
		
		IdentifierAssembler<KID, ?> identifierAssembler = keyEntityPersister.getMapping().getIdMapping().getIdentifierAssembler();
		if (identifierAssembler instanceof SingleIdentifierAssembler) {
			Column<?, KID> singleKeyColumn = ((SingleIdentifierAssembler) identifierAssembler).getColumn();
			String columnName = columnNamingStrategy.giveName(ENTRY_KEY_ACCESSOR_DEFINITION);
			Column<MAPTABLE, KID> maptableColumn = targetTable.addColumn(columnName, singleKeyColumn.getJavaType(), singleKeyColumn.getSize());
			maptableColumn.primaryKey();
			builder.withEntryKeyIsSingleProperty(maptableColumn);
		} else if (identifierAssembler instanceof DefaultComposedIdentifierAssembler) {
			Map<ReversibleAccessor<KID, ?>, Column<?, ?>> idMapping = ((DefaultComposedIdentifierAssembler) identifierAssembler).getMapping();
			Map<ReversibleAccessor<KID, ?>, Column<MAPTABLE, ?>> idMappingInMapTable = new KeepOrderMap<>();
			idMapping.forEach((key, column) -> {
				Column<MAPTABLE, ?> maptableColumn = targetTable.addColumn(column.getName(), column.getJavaType(), column.getSize());
				maptableColumn.primaryKey();
				idMappingInMapTable.put(key, maptableColumn);
			});
			EmbeddedClassMapping<KID, MAPTABLE> embeddedClassMapping = new EmbeddedClassMapping<>(
					((DefaultComposedIdentifierAssembler) identifierAssembler).getKeyType(),
					targetTable,
					idMappingInMapTable);
			builder.withEntryKeyIsComplexType(embeddedClassMapping);
		}
		
		return builder.build();
	}
	
	@Override
	protected void addInsertCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									EntityPersister<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>> relationRecordPersister,
									Accessor<SRC, MM> mapAccessor) {
		if (maintenanceMode != RelationMode.READ_ONLY) {
			sourcePersister.addInsertListener(new BeforeInsertCollectionCascader<SRC, K>(keyEntityPersister) {
				
				@Override
				protected Collection<K> getTargets(SRC src) {
					return mapGetter.apply(src).keySet();
				}
			});
		}
		if (maintenanceMode != RelationMode.READ_ONLY) {
			Function<SRC, Collection<KeyValueRecord<KID, V, SRCID>>> mapProviderForInsert = toRecordCollectionProvider(sourcePersister.getMapping(), false);
			sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(relationRecordPersister, mapProviderForInsert));
		}
	}
	
	@Override
	protected void addUpdateCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									EntityPersister<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>> relationRecordPersister) {
		Function<SRC, Set<Entry<K, V>>> targetEntitiesGetter = new NullProofFunction<>(mapGetter).andThen(Map::entrySet);
		BiFunction<Entry<K, V>, SRCID, KeyValueRecord<KID, V, SRCID>> entryKeyValueRecordFunction =
				(record, srcId) -> new KeyValueRecord<>(srcId, keyEntityPersister.getId(record.getKey()), record.getValue());
		BiConsumer<Duo<SRC, SRC>, Boolean> mapUpdater = new MapUpdater<>(targetEntitiesGetter, keyEntityPersister,
				relationRecordPersister, sourcePersister, maintenanceMode,
				Entry::getKey, entryKeyValueRecordFunction
		);
		sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(mapUpdater));
	}
	
	@Override
	protected void addDeleteCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister, EntityPersister<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>> relationRecordPersister) {
		if (maintenanceMode != RelationMode.READ_ONLY) {
			super.addDeleteCascade(sourcePersister, relationRecordPersister);
		}
		
		if (maintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
			Function<SRC, Set<K>> targetEntitiesGetter = new NullProofFunction<>(mapGetter).andThen(Map::entrySet).andThen(entries -> entries.stream().map(Entry::getKey).collect(Collectors.toSet()));
			sourcePersister.addDeleteListener(new BeforeDeleteCollectionCascader<SRC, K>(keyEntityPersister) {
				@Override
				protected Collection<K> getTargets(SRC src) {
					return targetEntitiesGetter.apply(src);
				}
			});
		}
	}
	
	@Override
	protected void addSelectCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									SimpleRelationalEntityPersister<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>, ?> relationRecordPersister,
									PrimaryKey<?, SRCID> sourcePK,
									ForeignKey<?, ?, SRCID> keyValueRecordToSourceForeignKey,
									BiConsumer<SRC, MM> mapSetter,
									Accessor<SRC, MM> mapGetter,
									Supplier<MM> mapFactory) {
		
		BeanRelationFixer<SRC, KeyValueRecord<KID, V, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
				mapSetter,
				mapGetter::get,
				mapFactory,
				(bean, input, map) -> {
					inMemoryRelationHolder.storeRelation(sourcePersister.getId(bean), input.getKey(), input.getValue());
				});
		
		// we add target subgraph joins to main persister
		// Note that this must be done before joining source persister with record persister in order to
		// let this join be copied in global join tree and participate to entity tree inflation. Else (doing
		// this join after joining source with records) requires to pass it the join node name built by
		// source-record join (no big deal here) but, overall, makes the BeanRelationFixer get the source type
		// as input argument, whereas at runtime it gets the record instances which makes some ClassCastException
		// Here is the wrong approach :
		/*
			keyEntityPersister.joinAsMany(sourcePersister,
				(Key<Table, KID>) keyIdColumnsProjectInAssociationTable,
				primaryKey,
				new BeanRelationFixer<C, K>() {
					@Override
					public void apply(C bean, K input) {
						inMemoryRelationHolder.store(((KeyValueRecord<KID, V, I>) bean).getId().getId(), keyEntityPersister.getId(input), input);
					}
				},
				null, associationTableJoinNodeName, true, false);
		 */
		keyEntityPersister.joinAsMany(ROOT_JOIN_NAME,
				relationRecordPersister,
				Accessors.accessorByMethodReference(KeyValueRecord::getKey),
                (Key<Table, KID>) keyIdColumnsProjectInAssociationTable,
				(PrimaryKey<?, KID>) keyEntityPersister.getMainTable().<KID>getPrimaryKey(),
                (bean, input) -> inMemoryRelationHolder.storeEntity(bean.getId().getId(), keyEntityPersister.getId(input), input), null, true, false);
		
		relationRecordPersister.joinAsMany(
				ROOT_JOIN_NAME,
				sourcePersister,
				mapGetter,
				sourcePK,
				keyValueRecordToSourceForeignKey,
				relationFixer,
				null,
                true, originalMapRelation.isFetchSeparately());
	}
}