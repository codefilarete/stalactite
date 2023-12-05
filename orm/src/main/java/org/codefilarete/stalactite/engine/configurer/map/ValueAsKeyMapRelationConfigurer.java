package org.codefilarete.stalactite.engine.configurer.map;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.AccessorDefinitionDefiner;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.engine.cascade.BeforeDeleteCollectionCascader;
import org.codefilarete.stalactite.engine.cascade.BeforeInsertCollectionCascader;
import org.codefilarete.stalactite.engine.diff.AbstractDiff;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
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
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Functions.NullProofFunction;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;
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
 * @param <V> Map value type
 * @param <M> relation Map type
 * @param <MM> redefined Map type to get entity key identifier 
 * @author Guillaume Mary
 */
public class ValueAsKeyMapRelationConfigurer<SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, MM extends Map<K, VID>> extends MapRelationConfigurer<SRC, SRCID, K, VID, MM> {
	
	private static <SRC, K, V, VID, M extends Map<K, V>, MM extends Map<K, VID>> MapRelation<SRC, K, VID, MM> convertEntityMapToIdentifierMap(
			MapRelation<SRC, K, V, M> mapRelation,
			ConfiguredRelationalPersister<V, VID> valueEntityPersister) {
		MapAccessor<SRC, K, V, VID, M, MM> srckvvidmMapMapAccessor = new MapAccessor<>(mapRelation, valueEntityPersister);
		PropertyAccessor<SRC, MM> ctPropertyAccessor = new PropertyAccessor<>(
				srckvvidmMapMapAccessor,
				(src, mm) -> {
					// No setter need because afterSelect(..) method is in charge of setting the values (too complex to be done here)
					// Don't give null Mutator to avoir NPE later
				}
		);
		return new MapRelation<>(
				ctPropertyAccessor,
				mapRelation.getKeyType(),
				valueEntityPersister.getMapping().getIdMapping().getIdentifierInsertionManager().getIdentifierType());
	}
	
	
	private final MapRelation<SRC, K, V, M> originalMapRelation;
	private final ConfiguredRelationalPersister<V, VID> valueEntityPersister;
	private final Function<SRC, M> mapGetter;
	private final InMemoryRelationHolder<SRCID, K, VID, V> inMemoryRelationHolder;
	private Key<?, VID> keyIdColumnsProjectInAssociationTable;
	private final RelationMode maintenanceMode;
	private final boolean maintainAssociationOnly;
	
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
		this.inMemoryRelationHolder = new InMemoryRelationHolder<>();
		
		this.maintenanceMode = mapRelation.getValueEntityRelationMode();
		// selection is always present (else configuration is nonsense !)
		this.maintainAssociationOnly = maintenanceMode == RelationMode.ASSOCIATION_ONLY;
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
	ClassMapping<KeyValueRecord<K, VID, SRCID>, RecordId<K, SRCID>, TARGETTABLE>
	buildKeyValueRecordMapping(TARGETTABLE targetTable,
							   IdentifierAssembler<SRCID, TT> sourceIdentifierAssembler,
							   Map<Column<TT, Object>, Column<TARGETTABLE, Object>> primaryKeyForeignColumnMapping,
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
		BiConsumer<Duo<SRC, SRC>, Boolean> mapUpdater = new MapUpdater<>(targetEntitiesGetter, valueEntityPersister,
				relationRecordPersister, sourcePersister, maintenanceMode);
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
					inMemoryRelationHolder.store(sourcePersister.getId(bean), input);
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
				(bean, input) -> inMemoryRelationHolder.store(bean.getId().getId(), bean.getKey(), input),
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
	
	/**
	 * Class aimed at doing same thing as {@link CollectionUpdater} but for {@link Map} containing entities as keys :
	 * requires to update {@link Entry} as well as propagate insert / update /delete operation to key-entities. 
	 *
	 * @param <SRC> entity type owning the relation
	 * @param <SRCID> entity owning the relation identifier type 
	 * @param <K> Map key entity type
	 * @param <V> Map value type
	 * @param <VID> Map value entity identifier type
	 * @author Guillaume Mary
	 */
	private static class MapUpdater<SRC, SRCID, K, VID, V> extends CollectionUpdater<SRC, Entry<K, V>, Set<Entry<K, V>>> {
		
		private static <K, V, VID> EntityWriter<Entry<K, V>> asEntityWriter(ConfiguredRelationalPersister<V, VID> valueEntityPersister) {
			return new EntityWriter<Entry<K, V>>() {
				
				@Override
				public void update(Iterable<? extends Duo<Entry<K, V>, Entry<K, V>>> differencesIterable, boolean allColumnsStatement) {
					valueEntityPersister.update(Iterables.stream(differencesIterable)
							.map(duo -> new Duo<>(duo.getLeft().getValue(), duo.getRight().getValue()))
							.collect(Collectors.toSet()), allColumnsStatement);
				}
				
				@Override
				public void delete(Iterable<? extends Entry<K, V>> entities) {
					valueEntityPersister.delete(Iterables.stream(entities).map(Entry::getValue).collect(Collectors.toSet()));
				}
				
				@Override
				public void persist(Iterable<? extends Entry<K, V>> entities) {
					valueEntityPersister.persist(Iterables.stream(entities).map(Entry::getValue).collect(Collectors.toSet()));
				}
				
				@Override
				public boolean isNew(Entry<K, V> entity) {
					return valueEntityPersister.isNew(entity.getValue());
				}
				
				@Override
				public void updateById(Iterable<? extends Entry<K, V>> entities) {
					valueEntityPersister.updateById(Iterables.stream(entities).map(Entry::getValue).collect(Collectors.toSet()));
				}
			};
		}
		
		private final EntityPersister<KeyValueRecord<K, VID, SRCID>, RecordId<K, SRCID>> keyValueRecordPersister;
		
		private final ConfiguredRelationalPersister<SRC, SRCID> sourcePersister;
		
		private final ConfiguredRelationalPersister<V, VID> valueEntityPersister;
		private final RelationMode maintenanceMode;
		
		public MapUpdater(Function<SRC, Set<Entry<K, V>>> targetEntitiesGetter,
						  ConfiguredRelationalPersister<V, VID> valueEntityPersister,
						  EntityPersister<KeyValueRecord<K, VID, SRCID>, RecordId<K, SRCID>> keyValueRecordPersister,
						  ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
						  RelationMode maintenanceMode) {
			super(targetEntitiesGetter,
					asEntityWriter(valueEntityPersister),
					(o, i) -> { /* no reverse setter because we store only raw values */ },
					true,
					entry -> valueEntityPersister.getId(entry.getValue()));
//					Entry::getKey);
			this.keyValueRecordPersister = keyValueRecordPersister;
			this.sourcePersister = sourcePersister;
			this.valueEntityPersister = valueEntityPersister;
			this.maintenanceMode = maintenanceMode;
		}
		
		@Override
		protected KeyValueAssociationTableUpdateContext newUpdateContext(Duo<SRC, SRC> updatePayload) {
			return new KeyValueAssociationTableUpdateContext(updatePayload);
		}
		
		@Override
		protected void onAddedElements(UpdateContext updateContext, AbstractDiff<Entry<K, V>> diff) {
			super.onAddedElements(updateContext, diff);
			KeyValueRecord<K, VID, SRCID> associationRecord = newRecord(updateContext.getPayload().getLeft(), diff.getReplacingInstance());
			((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeInserted().add(associationRecord);
		}
		
		@Override
		protected void onHeldElements(CollectionUpdater<SRC, Entry<K, V>, Set<Entry<K, V>>>.UpdateContext updateContext, AbstractDiff<Entry<K, V>> diff) {
			super.onHeldElements(updateContext, diff);
			Duo<KeyValueRecord<K, VID, SRCID>, KeyValueRecord<K, VID, SRCID>> associationRecord = new Duo<>(
					newRecord(updateContext.getPayload().getLeft(), diff.getReplacingInstance()),
					newRecord(updateContext.getPayload().getLeft(), diff.getSourceInstance())
			);
			((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeUpdated().add(associationRecord);
		}
		
		@Override
		protected void onRemovedElements(UpdateContext updateContext, AbstractDiff<Entry<K, V>> diff) {
			super.onRemovedElements(updateContext, diff);
			
			KeyValueRecord<K, VID, SRCID> associationRecord = newRecord(updateContext.getPayload().getLeft(), diff.getSourceInstance());
			((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeDeleted().add(associationRecord);
		}
		
		@Override
		protected void insertTargets(UpdateContext updateContext) {
			// we insert association records after targets to satisfy integrity constraint
			if (maintenanceMode != RelationMode.READ_ONLY && maintenanceMode != RelationMode.ASSOCIATION_ONLY) {
				super.insertTargets(updateContext);
			}
			if (maintenanceMode != RelationMode.READ_ONLY) {
				super.insertTargets(updateContext);
				keyValueRecordPersister.insert(((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeInserted());
			}
		}
		
		@Override
		protected void updateTargets(CollectionUpdater<SRC, Entry<K, V>, Set<Entry<K, V>>>.UpdateContext updateContext, boolean allColumnsStatement) {
			if (maintenanceMode != RelationMode.READ_ONLY && maintenanceMode != RelationMode.ASSOCIATION_ONLY) {
				super.updateTargets(updateContext, allColumnsStatement);
			}
			if (maintenanceMode != RelationMode.READ_ONLY) {
				keyValueRecordPersister.update(((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeUpdated(), allColumnsStatement);
			}
		}
		
		@Override
		protected void deleteTargets(UpdateContext updateContext) {
			// we delete association records before targets to satisfy integrity constraint
			if (maintenanceMode != RelationMode.READ_ONLY) {
				keyValueRecordPersister.delete(((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeDeleted());
			}
			if (maintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL) {
				super.deleteTargets(updateContext);
			}
		}
		
		private KeyValueRecord<K, VID, SRCID> newRecord(SRC e, Entry<K, V> record) {
			return new KeyValueRecord<>(sourcePersister.getId(e), record.getKey(), valueEntityPersister.getId(record.getValue()));
		}
		
		class KeyValueAssociationTableUpdateContext extends UpdateContext {
			
			private final List<KeyValueRecord<K, VID, SRCID>> associationRecordsToBeInserted = new ArrayList<>();
			private final List<Duo<KeyValueRecord<K, VID, SRCID>, KeyValueRecord<K, VID, SRCID>>> associationRecordsToBeUpdated = new ArrayList<>();
			private final List<KeyValueRecord<K, VID, SRCID>> associationRecordsToBeDeleted = new ArrayList<>();
			
			public KeyValueAssociationTableUpdateContext(Duo<SRC, SRC> updatePayload) {
				super(updatePayload);
			}
			
			public List<KeyValueRecord<K, VID, SRCID>> getAssociationRecordsToBeInserted() {
				return associationRecordsToBeInserted;
			}
			
			public List<Duo<KeyValueRecord<K, VID, SRCID>, KeyValueRecord<K, VID, SRCID>>> getAssociationRecordsToBeUpdated() {
				return associationRecordsToBeUpdated;
			}
			
			public List<KeyValueRecord<K, VID, SRCID>> getAssociationRecordsToBeDeleted() {
				return associationRecordsToBeDeleted;
			}
		}
	}
	
	/**
	 * {@link Accessor} that converts Map&lt;K, V&gt; to Map&lt;KID, V&gt; on {@link #get(Object)}.
	 * Could have been an anonymous class but {@link MapRelationConfigurer} requires to call {@link AccessorDefinition#giveDefinition(ValueAccessPoint)}
	 * at some point, which causes {@link UnsupportedOperationException} since the anonymous class is unknown from it.
	 * Though it as to be a named class, moreover {@link AccessorDefinition} has been enhanced take into account classes that provides their
	 * {@link AccessorDefinition} by their own through {@link AccessorDefinitionDefiner}.
	 * 
	 * @param <SRC> entity type owning the relation
	 * @param <K> Map key entity type
	 * @param <V> Map value type
	 * @param <VID> Map value entity identifier type
	 * @param <M> relation Map type
	 * @param <MM> redefined Map type to get entity key identifier 
	 * @author Guillaume Mary
	 */
	private static class MapAccessor<SRC, K, V, VID, M extends Map<K, V>, MM extends Map<K, VID>> implements Accessor<SRC, MM>, AccessorDefinitionDefiner<SRC> {
		
		private final MapRelation<SRC, K, V, M> map;
		
		private final ConfiguredRelationalPersister<V, VID> valueEntityPersister;
		private final AccessorDefinition accessorDefinition;
		
		public MapAccessor(MapRelation<SRC, K, V, M> map, ConfiguredRelationalPersister<V, VID> valueEntityPersister) {
			this.map = map;
			this.valueEntityPersister = valueEntityPersister;
			this.accessorDefinition = AccessorDefinition.giveDefinition(this.map.getMapProvider());
		}
		
		@Override
		public MM get(SRC SRC) {
			M m = map.getMapProvider().get(SRC);
			if (m != null) {
				MM result = (MM) new HashMap<>();    // we can use an HashMap since KID should have equals() + hashCode() implemented since its an identifier
				m.forEach((k, v) -> result.put(k, valueEntityPersister.getId(v)));
				return result;
			} else {
				return null;
			}
		}
		
		@Override
		public AccessorDefinition asAccessorDefinition() {
			return this.accessorDefinition;
		}
	}
	
	/**
	 * Made to store links between :
	 * - source entity and [key-entity-id, value] pairs on one hand
	 * - key-value records and key entity on the other hand
	 * which let caller seam source entity and its [key entity, value] pairs afterward.
	 * This is made necessary due to double join creation between
	 * - source entity table and association table on one hand
	 * - association table and key-entity table on one hand
	 * Look at joinAsMany(..) invocations in {@link ValueAsKeyMapRelationConfigurer#addSelectCascade(ConfiguredRelationalPersister, SimpleRelationalEntityPersister, PrimaryKey, ForeignKey, BiConsumer, Function, Supplier)}  
	 * This is the goal and need, implementation differ due to simplification made after first intent. 
	 * 
	 * Expected to be used in a {@link SelectListener} to {@link #init()} it before select and {@link #clear()} it after select.
	 * 
	 * @param <I>
	 * @param <K>
	 * @param <V>
	 * @param <VID>
	 * @author Guillaume Mary
	 */
	private static class InMemoryRelationHolder<I, K, VID, V> {
		
		private class Trio {
			private K x;
			private VID y;
			private V z;
		}
		
		/**
		 * In memory and temporary Map storage.
		 */
		private final ThreadLocal<Map<I, Set<Trio>>> relationCollectionPerEntity = new ThreadLocal<>();
		
		public InMemoryRelationHolder() {
		}
		
		public void store(I source, KeyValueRecord<K, VID, I> keyValueRecord) {
			Map<I, Set<Trio>> srcidcMap = relationCollectionPerEntity.get();
			Set<Trio> relatedDuos = srcidcMap.computeIfAbsent(source, id -> new HashSet<>());
			Trio trio = relatedDuos.stream().filter(pawn -> pawn.x == keyValueRecord.getKey()).findAny().orElseGet(() -> {
				Trio result = new Trio();
				relatedDuos.add(result);
				return result;
			});
			trio.x = keyValueRecord.getKey();
			trio.y = keyValueRecord.getValue();
		}
		
		public void store(I source, K identifier, V input) {
			Map<I, Set<Trio>> srcidcMap = relationCollectionPerEntity.get();
			Set<Trio> relatedDuos = srcidcMap.computeIfAbsent(source, id -> new HashSet<>());
			Trio trio = relatedDuos.stream().filter(pawn -> Objects.equals(pawn.x, identifier)).findAny().orElseGet(() -> {
				Trio result = new Trio();
				relatedDuos.add(result);
				return result;
			});
			trio.x = identifier;
			trio.z = input;
			// bidirectional assignment
//			preventNull(getReverseSetter(), NOOP_REVERSE_SETTER).accept(input, source);
		}
		
		public Collection<Duo<K, V>> get(I src) {
			Map<I, Set<Trio>> currentMap = relationCollectionPerEntity.get();
			return nullable(currentMap)
					.map(map -> map.get(src))
					.map(map -> map.stream().map(trio -> new Duo<>(trio.x, trio.z))
							.collect(Collectors.toSet()))
					.get();
		}
		
		public void init() {
			this.relationCollectionPerEntity.set(new HashMap<>());
		}
		
		public void clear() {
			this.relationCollectionPerEntity.remove();
		}
	}
}