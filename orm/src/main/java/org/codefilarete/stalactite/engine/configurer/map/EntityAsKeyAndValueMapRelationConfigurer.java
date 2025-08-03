package org.codefilarete.stalactite.engine.configurer.map;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteCollectionCascader;
import org.codefilarete.stalactite.engine.cascade.BeforeInsertCollectionCascader;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.AfterUpdateTrigger;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.function.Functions.NullProofFunction;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;
import static org.codefilarete.tool.Nullable.nullable;

/**
 * Handles particular case of {@link MapRelationConfigurer} when Map key and value type are entities : it requires some
 * cascading with key-entity and value-entity tables, as well as select handling.
 * Design of this class is to inherit from simple case, letting parent class handle the relation as if it was a usual
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
public class EntityAsKeyAndValueMapRelationConfigurer<SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, MM extends Map<KID, VID>> extends MapRelationConfigurer<SRC, SRCID, KID, VID, MM> {
	
	private static <SRC, K, KID, V, VID, M extends Map<K, V>, MM extends Map<KID, VID>> MapRelation<SRC, KID, VID, MM> convertEntityMapToIdentifierMap(
			MapRelation<SRC, K, V, M> mapRelation,
			ConfiguredRelationalPersister<K, KID> keyEntityPersister,
			ConfiguredRelationalPersister<V, VID> valueEntityPersister
			) {
		ConvertingMapAccessor<SRC, K, V, KID, VID, M, MM> mapAccessor = new ConvertingMapAccessor<>(mapRelation, (k, v, result) -> result.put(keyEntityPersister.getId(k), valueEntityPersister.getId(v)));
		PropertyAccessor<SRC, MM> propertyAccessor = new PropertyAccessor<>(
				mapAccessor,
				(src, mm) -> {
					// No setter need because afterSelect(..) method is in charge of setting the values (too complex to be done here)
					// Don't give null Mutator to avoid NPE later
				}
		);
		return new MapRelation<>(
				propertyAccessor,
				keyEntityPersister.getMapping().getIdMapping().getIdentifierInsertionManager().getIdentifierType(),
				valueEntityPersister.getMapping().getIdMapping().getIdentifierInsertionManager().getIdentifierType());
	}
	
	
	private final MapRelation<SRC, K, V, M> originalMapRelation;
	private final ConfiguredRelationalPersister<K, KID> keyEntityPersister;
	private final ConfiguredRelationalPersister<V, VID> valueEntityPersister;
	private final Function<SRC, M> mapGetter;
	private final InMemoryRelationHolder<SRCID, KID, VID, K, V> inMemoryRelationHolder;
	private Key<?, KID> keyIdColumnsProjectInAssociationTable;
	private Key<?, VID> valueIdColumnsProjectInAssociationTable;
	private final RelationMode keyEntityMaintenanceMode;
	private final RelationMode valueEntityMaintenanceMode;
	private final boolean associationRecordWritable;
	
	public EntityAsKeyAndValueMapRelationConfigurer(
			MapRelation<SRC, K, V, M> mapRelation,
			ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
			ConfiguredRelationalPersister<K, KID> keyEntityPersister,
			ConfiguredRelationalPersister<V, VID> valueEntityPersister,
			ForeignKeyNamingStrategy foreignKeyNamingStrategy,
			ColumnNamingStrategy columnNamingStrategy,
			MapEntryTableNamingStrategy tableNamingStrategy,
			Dialect dialect,
			ConnectionConfiguration connectionConfiguration) {
		super(convertEntityMapToIdentifierMap(mapRelation, keyEntityPersister, valueEntityPersister),
				sourcePersister,
				foreignKeyNamingStrategy,
				columnNamingStrategy,
				tableNamingStrategy,
				dialect,
				connectionConfiguration);
		this.originalMapRelation = mapRelation;
		this.keyEntityPersister = keyEntityPersister;
		this.valueEntityPersister = valueEntityPersister;
		this.mapGetter = originalMapRelation.getMapProvider()::get;
		this.inMemoryRelationHolder = new InMemoryRelationHolder<>();
		this.keyEntityMaintenanceMode = mapRelation.getKeyEntityRelationMode();
		this.valueEntityMaintenanceMode = mapRelation.getValueEntityRelationMode();
		this.associationRecordWritable = this.keyEntityMaintenanceMode != RelationMode.READ_ONLY;
	}
	
	@Override
	public void configure() {
		
		AccessorDefinition mapProviderDefinition = AccessorDefinition.giveDefinition(originalMapRelation.getMapProvider());
		Supplier<M> mapFactory = BeanRelationFixer.giveMapFactory((Class<M>) mapProviderDefinition.getMemberType());
		
		// Put elements into source entities by converting in-memory stored objects as Map entries.
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
					Collection<Duo<K, V>> keyValuePairs = inMemoryRelationHolder.get(sourcePersister.getId(bean));
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
	<TT extends Table<TT>, TARGETTABLE extends Table<TARGETTABLE>>
	ClassMapping<KeyValueRecord<KID, VID, SRCID>, RecordId<KID, SRCID>, TARGETTABLE>
	buildKeyValueRecordMapping(TARGETTABLE targetTable,
							   IdentifierAssembler<SRCID, TT> sourceIdentifierAssembler,
							   Map<Column<TT, ?>, Column<TARGETTABLE, ?>> primaryKeyForeignColumnMapping,
							   EmbeddableMappingConfiguration<KID> keyEmbeddableConfiguration,
							   EmbeddableMappingConfiguration<VID> valueEmbeddableConfiguration) {
		KeyValueRecordMappingBuilder<KID, VID, SRCID, TARGETTABLE, TT> builder
				= new KeyValueRecordMappingBuilder<KID, VID, SRCID, TARGETTABLE, TT>(targetTable, sourceIdentifierAssembler, primaryKeyForeignColumnMapping) {
			
			private final Map<Column<TARGETTABLE, Object>, Column<Table, Object>> mapEntryKeyForeignKeyBootstrap = new HashMap<>();
			private final Map<Column<TARGETTABLE, Object>, Column<Table, Object>> mapEntryValueForeignKeyBootstrap = new HashMap<>();
			
			/** Overridden to capture key column in order to build foreign key */
			@Override
			void withEntryKeyIsSingleProperty(Column<TARGETTABLE, KID> keyColumn) {
				super.withEntryKeyIsSingleProperty(keyColumn);
				Column<Table, Object> column = ((SimpleIdMapping) keyEntityPersister.getMapping().getIdMapping()).getIdentifierAssembler().getColumn();
				mapEntryKeyForeignKeyBootstrap.put((Column<TARGETTABLE, Object>) keyColumn, column);
				keyIdColumnsProjectInAssociationTable = Key.ofSingleColumn(keyColumn);
			}
			
			/** Overridden to capture key columns in order to build foreign key */
			@Override
			void withEntryKeyIsComplexType(EmbeddedClassMapping<KID, TARGETTABLE> entryKeyMapping) {
				super.withEntryKeyIsComplexType(entryKeyMapping);
				KeyBuilder<TARGETTABLE, KID> keyIdColumnsProjectInAssociationTableBuilder = Key.from(targetTable);
				entryKeyMapping.getPropertyToColumn().values().forEach(keyIdColumnsProjectInAssociationTableBuilder::addColumn);
				keyIdColumnsProjectInAssociationTable = keyIdColumnsProjectInAssociationTableBuilder.build();
			}
			
			/** Overridden to capture value column in order to build foreign key */
			@Override
			void withEntryValueIsSingleProperty(Column<TARGETTABLE, VID> valueColumn) {
				super.withEntryValueIsSingleProperty(valueColumn);
				Column<Table, Object> column = ((SimpleIdMapping) valueEntityPersister.getMapping().getIdMapping()).getIdentifierAssembler().getColumn();
				mapEntryValueForeignKeyBootstrap.put((Column<TARGETTABLE, Object>) valueColumn, column);
				valueIdColumnsProjectInAssociationTable = Key.ofSingleColumn(valueColumn);
			}
			
			/** Overridden to capture value columns in order to build foreign key */
			@Override
			void withEntryValueIsComplexType(EmbeddedClassMapping<VID, TARGETTABLE> entryValueMapping) {
				super.withEntryValueIsComplexType(entryValueMapping);
				KeyBuilder<TARGETTABLE, VID> keyIdColumnsProjectInAssociationTableBuilder = Key.from(targetTable);
				entryValueMapping.getPropertyToColumn().values().forEach(keyIdColumnsProjectInAssociationTableBuilder::addColumn);
				valueIdColumnsProjectInAssociationTable = keyIdColumnsProjectInAssociationTableBuilder.build();
			}
			
			/** Overridden to create foreign key between association table and entity key table as well as entity value table */
			@Override
			KeyValueRecordMapping<KID, VID, SRCID, TARGETTABLE> build() {
				KeyBuilder<TARGETTABLE, Object> entryKeyKeyBuilder = Key.from(targetTable);
				KeyBuilder<Table, Object> entryKeyReferencedKeyBuilder = Key.from(keyEntityPersister.getMainTable());
				mapEntryKeyForeignKeyBootstrap.forEach((key, value) -> {
					entryKeyKeyBuilder.addColumn(key);
					entryKeyReferencedKeyBuilder.addColumn(value);
				});
				KeyBuilder<TARGETTABLE, Object> entryValueKeyBuilder = Key.from(targetTable);
				KeyBuilder<Table, Object> entryValueReferencedKeyBuilder = Key.from(valueEntityPersister.getMainTable());
				mapEntryValueForeignKeyBootstrap.forEach((key, value) -> {
					entryValueKeyBuilder.addColumn(key);
					entryValueReferencedKeyBuilder.addColumn(value);
				});
				targetTable.addForeignKey(foreignKeyNamingStrategy::giveName, entryKeyKeyBuilder.build(), entryKeyReferencedKeyBuilder.build());
				targetTable.addForeignKey(foreignKeyNamingStrategy::giveName, entryValueKeyBuilder.build(), entryValueReferencedKeyBuilder.build());
				return super.build();
			}
		};
		
		return super.buildKeyValueRecordMapping(keyEmbeddableConfiguration, targetTable, builder, valueEmbeddableConfiguration);
	}
	
	@Override
	protected void addInsertCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									EntityPersister<KeyValueRecord<KID, VID, SRCID>, RecordId<KID, SRCID>> relationRecordPersister,
									Accessor<SRC, MM> mapAccessor) {
		if (keyEntityMaintenanceMode != RelationMode.READ_ONLY) {
			// key entities must be persisted before association records insertion to satisfy foreign key constraint
			sourcePersister.addInsertListener(new BeforeInsertCollectionCascader<SRC, K>(keyEntityPersister) {
				
				@Override
				protected Collection<K> getTargets(SRC src) {
					return mapGetter.apply(src).keySet();
				}
			});
		}
		if (valueEntityMaintenanceMode != RelationMode.READ_ONLY) {
			// value entities must be persisted before association records insertion to satisfy foreign key constraint
			sourcePersister.addInsertListener(new BeforeInsertCollectionCascader<SRC, V>(valueEntityPersister) {

				@Override
				protected Collection<V> getTargets(SRC src) {
					return mapGetter.apply(src).values();
				}
			});
		}
		if (this.associationRecordWritable) {
			Function<SRC, Collection<KeyValueRecord<KID, VID, SRCID>>> mapProviderForInsert = toRecordCollectionProvider(sourcePersister.getMapping(), false);
			sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(relationRecordPersister, mapProviderForInsert));
		}
	}
	
	@Override
	protected void addUpdateCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									EntityPersister<KeyValueRecord<KID, VID, SRCID>, RecordId<KID, SRCID>> relationRecordPersister) {
		// No direct cascade handling here because it will be done by MapUpdater
		Function<SRC, Set<KeyValueRecord<K, V, SRCID>>> targetEntitiesGetter = src -> {
			SRCID srcId = sourcePersister.getId(src);
			M map = mapGetter.apply(src);
			return map == null ? Collections.emptySet() : map.entrySet().stream().map(entry -> new KeyValueRecord<>(srcId, entry.getKey(), entry.getValue()))
					.collect(Collectors.toSet());
		};
		BiConsumer<Duo<SRC, SRC>, Boolean> mapUpdater = new MapEntryKeyAndValueEntitiesUpdater<>(targetEntitiesGetter,
				keyEntityPersister::getId, valueEntityPersister::getId,
				keyEntityPersister, valueEntityPersister,
				relationRecordPersister, keyEntityMaintenanceMode, valueEntityMaintenanceMode
		);
		sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(mapUpdater));
	}
	
	@Override
	protected void addDeleteCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									EntityPersister<KeyValueRecord<KID, VID, SRCID>, RecordId<KID, SRCID>> relationRecordPersister) {
		// association records must be deleted before referenced entities in order to satisfy foreign key constraint
		if (this.associationRecordWritable) {
			super.addDeleteCascade(sourcePersister, relationRecordPersister);
		}
		
		if (keyEntityMaintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
			Function<SRC, Set<K>> targetEntitiesGetter = new NullProofFunction<>(mapGetter).andThen(Map::entrySet).andThen(entries -> entries.stream().map(Entry::getKey).collect(Collectors.toSet()));
			sourcePersister.addDeleteListener(new BeforeDeleteCollectionCascader<SRC, K>(keyEntityPersister) {
				@Override
				protected Collection<K> getTargets(SRC src) {
					return targetEntitiesGetter.apply(src);
				}
			});
		}
		
		if (valueEntityMaintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
			Function<SRC, Set<V>> targetEntitiesGetter = new NullProofFunction<>(mapGetter).andThen(Map::entrySet).andThen(entries -> entries.stream().map(Entry::getValue).collect(Collectors.toSet()));
			sourcePersister.addDeleteListener(new BeforeDeleteCollectionCascader<SRC, V>(valueEntityPersister) {
				@Override
				protected Collection<V> getTargets(SRC src) {
					return targetEntitiesGetter.apply(src);
				}
			});
		}
	}
	
	@Override
	protected void addSelectCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									SimpleRelationalEntityPersister<KeyValueRecord<KID, VID, SRCID>, RecordId<KID, SRCID>, ?> relationRecordPersister,
									PrimaryKey<?, SRCID> sourcePK,
									ForeignKey<?, ?, SRCID> keyValueRecordToSourceForeignKey,
									BiConsumer<SRC, MM> mapSetter,
									Accessor<SRC, MM> mapGetter,
									Supplier<MM> mapFactory) {
		
		BeanRelationFixer<SRC, KeyValueRecord<KID, VID, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
				mapSetter,
				mapGetter::get,
				mapFactory,
				(bean, input, map) -> {
					inMemoryRelationHolder.storeRelation(input.getId().getId(), input.getKey(), input.getValue());
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
                (bean, input) -> inMemoryRelationHolder.storeKeyEntity(bean.getId().getId(), bean.getKey(), input),
				null,
				true,
				false);

		valueEntityPersister.joinAsMany(ROOT_JOIN_NAME,
				relationRecordPersister,
				Accessors.accessorByMethodReference(KeyValueRecord::getValue),
                (Key<Table, VID>) valueIdColumnsProjectInAssociationTable,
				(PrimaryKey<?, VID>) valueEntityPersister.getMainTable().<VID>getPrimaryKey(),
                (bean, input) -> inMemoryRelationHolder.storeValueEntity(bean.getId().getId(), bean.getValue(), input),
				null,
				true,
				false);
		
		relationRecordPersister.joinAsMany(
				ROOT_JOIN_NAME,
				sourcePersister,
				mapGetter,
				sourcePK,
				keyValueRecordToSourceForeignKey,
				relationFixer,
				null,
                true,
				originalMapRelation.isFetchSeparately());
	}
	
	static class InMemoryRelationHolder<I, ENTRY_KEY, ENTRY_VALUE, KEY_ENTITY, VALUE_ENTITY> {
		
		public class Trio {
			private final Map<ENTRY_KEY, ENTRY_VALUE> entries = new HashMap<>();
			private final Map<ENTRY_KEY, KEY_ENTITY> entity1 = new HashMap<>();
			private final Map<ENTRY_VALUE, VALUE_ENTITY> entity2 = new HashMap<>();
			
			Collection<Duo<KEY_ENTITY, VALUE_ENTITY>> assemble() {
				return this.entries.entrySet().stream().map(entry -> new Duo<>(entity1.get(entry.getKey()), entity2.get(entry.getValue()))).collect(Collectors.toSet());
			}
		}
		
		/**
		 * In memory and temporary Map storage.
		 */
		private final ThreadLocal<Map<I, Trio>> relationCollectionPerEntity = new ThreadLocal<>();
		
		public InMemoryRelationHolder() {
		}
		
		public void storeRelation(I source, ENTRY_KEY keyLookup, ENTRY_VALUE entryValue) {
			Map<I, Trio> srcidcMap = relationCollectionPerEntity.get();
			Trio relatedDuos = srcidcMap.computeIfAbsent(source, id -> new Trio());
			relatedDuos.entries.put(keyLookup, entryValue);
		}
		
		public void storeKeyEntity(I source, ENTRY_KEY keyLookup, KEY_ENTITY entity) {
			Map<I, Trio> srcidcMap = relationCollectionPerEntity.get();
			Trio relatedDuos = srcidcMap.computeIfAbsent(source, id -> new Trio());
			relatedDuos.entity1.put(keyLookup, entity);
		}
		
		public void storeValueEntity(I source, ENTRY_VALUE keyLookup, VALUE_ENTITY entity) {
			Map<I, Trio> srcidcMap = relationCollectionPerEntity.get();
			Trio relatedDuos = srcidcMap.computeIfAbsent(source, id -> new Trio());
			relatedDuos.entity2.put(keyLookup, entity);
		}
		
		public Collection<Duo<KEY_ENTITY, VALUE_ENTITY>> get(I src) {
			Map<I, Trio> currentMap = relationCollectionPerEntity.get();
			return nullable(currentMap).map(m -> m.get(src)).map(Trio::assemble).get();
		}
		
		public void init() {
			this.relationCollectionPerEntity.set(new HashMap<>());
		}
		
		public void clear() {
			this.relationCollectionPerEntity.remove();
		}
	}
}