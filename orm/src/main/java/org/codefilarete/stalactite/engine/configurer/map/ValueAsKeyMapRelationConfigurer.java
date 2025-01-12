package org.codefilarete.stalactite.engine.configurer.map;

import java.util.Collection;
import java.util.HashMap;
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

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;

/**
 * Handle particular case of {@link MapRelationConfigurer} when Map key is an entity : it requires some cascading
 * with key-entity table and select handling.
 * Design of this class is to inherit from simple case letting parent class handle the relation as if it was a usual
 * Map made of simple beans. Then current class "has only" to manage cascading and entity construction at selection time. 
 * 
 * @param <SRC> entity type owning the relation
 * @param <SRCID> entity owning the relation identifier type 
 * @param <K> Map key entity type
 * @param <V> Map value type
 * @param <M> relation Map type
 * @param <MM> redefined Map type to get entity key identifier 
 * @author Guillaume Mary
 */
public class ValueAsKeyMapRelationConfigurer<SRC, SRCID, K, V, VID, M extends Map<K, V>, MM extends Map<K, VID>> extends MapRelationConfigurer<SRC, SRCID, K, VID, MM> {
	
	private static <SRC, K, V, VID, M extends Map<K, V>, MM extends Map<K, VID>> MapRelation<SRC, K, VID, MM> convertEntityMapToIdentifierMap(
			MapRelation<SRC, K, V, M> mapRelation,
			ConfiguredRelationalPersister<V, VID> valueEntityPersister) {
		ConvertingMapAccessor<SRC, K, V, K, VID, M, MM> mapAccessor = new ConvertingMapAccessor<>(mapRelation, (k, v, result) -> result.put(k, valueEntityPersister.getId(v)));
		PropertyAccessor<SRC, MM> propertyAccessor = new PropertyAccessor<>(
				mapAccessor,
				(src, mm) -> {
					// No setter need because afterSelect(..) method is in charge of setting the values (too complex to be done here)
					// Don't give null Mutator to avoir NPE later
				}
		);
		return new MapRelation<>(
				propertyAccessor,
				mapRelation.getKeyType(),
				valueEntityPersister.getMapping().getIdMapping().getIdentifierInsertionManager().getIdentifierType());
	}
	
	
	private final MapRelation<SRC, K, V, M> originalMapRelation;
	private final ConfiguredRelationalPersister<V, VID> valueEntityPersister;
	private final Function<SRC, M> mapGetter;
	private final InMemoryRelationHolder<SRCID, K, VID, V> inMemoryRelationHolder;
	private Key<?, VID> keyIdColumnsProjectInAssociationTable;
	private final RelationMode maintenanceMode;
	
	public ValueAsKeyMapRelationConfigurer(
			MapRelation<SRC, K, V, M> mapRelation,
			ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
			ConfiguredRelationalPersister<V, VID> valueEntityPersister,
			ForeignKeyNamingStrategy foreignKeyNamingStrategy,
			ColumnNamingStrategy columnNamingStrategy,
			MapEntryTableNamingStrategy tableNamingStrategy,
			Dialect dialect,
			ConnectionConfiguration connectionConfiguration) {
		super(convertEntityMapToIdentifierMap(mapRelation, valueEntityPersister),
				sourcePersister,
				foreignKeyNamingStrategy,
				columnNamingStrategy,
				tableNamingStrategy,
				dialect,
				connectionConfiguration);
		this.originalMapRelation = mapRelation;
		this.valueEntityPersister = valueEntityPersister;
		this.mapGetter = originalMapRelation.getMapProvider()::get;
		this.inMemoryRelationHolder = new InMemoryRelationHolder<>(trio -> new Duo<>(trio.getKeyLookup(), trio.getEntity()));
		this.maintenanceMode = mapRelation.getValueEntityRelationMode();
	}
	
	@Override
	public void configure() {
		
		AccessorDefinition mapProviderDefinition = AccessorDefinition.giveDefinition(originalMapRelation.getMapProvider());
		Supplier<M> mapFactory = BeanRelationFixer.giveMapFactory((Class<M>) mapProviderDefinition.getMemberType());
		
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
	<TT extends Table<TT>, TARGETTABLE extends Table<TARGETTABLE>>
	ClassMapping<KeyValueRecord<K, VID, SRCID>, RecordId<K, SRCID>, TARGETTABLE>
	buildKeyValueRecordMapping(TARGETTABLE targetTable,
							   IdentifierAssembler<SRCID, TT> sourceIdentifierAssembler,
							   Map<Column<TT, ?>, Column<TARGETTABLE, ?>> primaryKeyForeignColumnMapping,
							   EmbeddableMappingConfiguration<K> keyEmbeddableConfiguration,
							   EmbeddableMappingConfiguration<VID> valueEmbeddableConfiguration) {
		KeyValueRecordMappingBuilder<K, VID, SRCID, TARGETTABLE, TT> builder
				= new KeyValueRecordMappingBuilder<K, VID, SRCID, TARGETTABLE, TT>(targetTable, sourceIdentifierAssembler, primaryKeyForeignColumnMapping) {
			
			private final Map<Column<TARGETTABLE, Object>, Column<Table, Object>> foreignKeyBootstrap = new HashMap<>();
			
			@Override
			void withEntryValueIsSingleProperty(Column<TARGETTABLE, VID> keyColumn) {
				super.withEntryValueIsSingleProperty(keyColumn);
				Column<Table, Object> column = ((SimpleIdMapping) valueEntityPersister.getMapping().getIdMapping()).getIdentifierAssembler().getColumn();
				foreignKeyBootstrap.put((Column<TARGETTABLE, Object>) keyColumn, column);
				keyIdColumnsProjectInAssociationTable = Key.ofSingleColumn(keyColumn);
			}

			@Override
			void withEntryValueIsComplexType(EmbeddedClassMapping<VID, TARGETTABLE> entryKeyMapping) {
				super.withEntryValueIsComplexType(entryKeyMapping);
				KeyBuilder<TARGETTABLE, VID> keyIdColumnsProjectInAssociationTableBuilder = Key.from(targetTable);
				entryKeyMapping.getPropertyToColumn().values().forEach(keyIdColumnsProjectInAssociationTableBuilder::addColumn);
				keyIdColumnsProjectInAssociationTable = keyIdColumnsProjectInAssociationTableBuilder.build();
			}
			
			@Override
			KeyValueRecordMapping<K, VID, SRCID, TARGETTABLE> build() {
				KeyBuilder<TARGETTABLE, Object> keyBuilder1 = Key.from(targetTable);
				KeyBuilder<Table, Object> keyBuilder2 = Key.from(valueEntityPersister.getMainTable());
				foreignKeyBootstrap.forEach((key, value) -> {
					keyBuilder1.addColumn(key);
					keyBuilder2.addColumn(value);
				});
				targetTable.addForeignKey(foreignKeyNamingStrategy::giveName, keyBuilder1.build(), keyBuilder2.build());
				return super.build();
			}
		};
		
		return super.buildKeyValueRecordMapping(keyEmbeddableConfiguration, targetTable, builder, valueEmbeddableConfiguration);
	}
	
	@Override
	protected void addInsertCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									EntityPersister<KeyValueRecord<K, VID, SRCID>, RecordId<K, SRCID>> relationRecordPersister,
									Accessor<SRC, MM> mapAccessor) {
		if (maintenanceMode != RelationMode.READ_ONLY) {
			sourcePersister.addInsertListener(new BeforeInsertCollectionCascader<SRC, V>(valueEntityPersister) {
				
				@Override
				protected Collection<V> getTargets(SRC src) {
					return mapGetter.apply(src).values();
				}
			});
		}
		if (maintenanceMode != RelationMode.READ_ONLY) {
			Function<SRC, Collection<KeyValueRecord<K, VID, SRCID>>> mapProviderForInsert = toRecordCollectionProvider(sourcePersister.getMapping(), false);
			sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(relationRecordPersister, mapProviderForInsert));
		}
	}
	
	@Override
	protected void addUpdateCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									EntityPersister<KeyValueRecord<K, VID, SRCID>, RecordId<K, SRCID>> relationRecordPersister) {
		Function<SRC, Set<Entry<K, V>>> targetEntitiesGetter = new NullProofFunction<>(mapGetter).andThen(Map::entrySet);
		BiFunction<Entry<K, V>, SRCID, KeyValueRecord<K, VID, SRCID>> entryKeyValueRecordFunction =
				(record, srcId) -> new KeyValueRecord<>(srcId, record.getKey(), valueEntityPersister.getId(record.getValue()));
		BiConsumer<Duo<SRC, SRC>, Boolean> mapUpdater = new MapUpdater<>(targetEntitiesGetter, valueEntityPersister,
				relationRecordPersister, sourcePersister, maintenanceMode,
				Entry::getValue, entryKeyValueRecordFunction
		);
		sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(mapUpdater));
	}
	
	@Override
	protected void addDeleteCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister, EntityPersister<KeyValueRecord<K, VID, SRCID>, RecordId<K, SRCID>> relationRecordPersister) {
		if (maintenanceMode != RelationMode.READ_ONLY) {
			super.addDeleteCascade(sourcePersister, relationRecordPersister);
		}
		
		if (maintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
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
									SimpleRelationalEntityPersister<KeyValueRecord<K, VID, SRCID>, RecordId<K, SRCID>, ?> relationRecordPersister,
									PrimaryKey<?, SRCID> sourcePK,
									ForeignKey<?, ?, SRCID> keyValueRecordToSourceForeignKey,
									BiConsumer<SRC, MM> mapSetter,
									Function<SRC, MM> mapGetter,
									Supplier<MM> mapFactory) {
		
		BeanRelationFixer<SRC, KeyValueRecord<K, VID, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
				mapSetter,
				mapGetter,
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
			valueEntityPersister.joinAsMany(sourcePersister,
				(Key<Table, KID>) keyIdColumnsProjectInAssociationTable,
				primaryKey,
				new BeanRelationFixer<SRC, K>() {
					@Override
					public void apply(SRC bean, K input) {
						inMemoryRelationHolder.store(((KeyValueRecord<K, VID, SRCID>) bean).getId().getId(), (KeyValueRecord<K, VID, SRCID>) bean);
					}
				},
				null, associationTableJoinNodeName, true, false);
		 */
		PrimaryKey<?, VID> primaryKey = valueEntityPersister.getMainTable().getPrimaryKey();
		valueEntityPersister.joinAsMany(relationRecordPersister,
				(Key<Table, VID>) keyIdColumnsProjectInAssociationTable,
				primaryKey,
				(bean, input) -> inMemoryRelationHolder.storeEntity(bean.getId().getId(), bean.getKey(), input),
				null, ROOT_STRATEGY_NAME, true, false);
		
		relationRecordPersister.joinAsMany(
				sourcePersister,
				sourcePK,
				keyValueRecordToSourceForeignKey,
				relationFixer,
				null,
				ROOT_STRATEGY_NAME,
				true,
				originalMapRelation.isFetchSeparately());
	}
}