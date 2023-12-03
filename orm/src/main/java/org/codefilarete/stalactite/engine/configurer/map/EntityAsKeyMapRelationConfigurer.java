package org.codefilarete.stalactite.engine.configurer.map;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.MapEntryTableNamingStrategy;
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
import org.codefilarete.tool.collection.Iterables;

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
		return new MapRelation<>(
				new PropertyAccessor<>(
						new MapAccessor<>(mapRelation, keyEntityPersister),
						(src, mm) -> {
							// No setter need because afterSelect(..) method is in charge of setting the values (too complex to be done here)
							// Don't give null Mutator to avoir NPE later
						}
				),
				keyEntityPersister.getMapping().getIdMapping().getIdentifierInsertionManager().getIdentifierType(),
				mapRelation.getValueType());
	}
	
	
	private final MapRelation<SRC, K, V, M> originalMapRelation;
	private final ConfiguredRelationalPersister<K, KID> keyEntityPersister;
	private final Function<SRC, M> mapGetter;
	private final InMemoryRelationHolder<SRCID, K, KID, V> inMemoryRelationHolder;
	private Key<?, KID> keyIdColumnsProjectInAssociationTable;
	
	public EntityAsKeyMapRelationConfigurer(
			MapRelation<SRC, K, V, M> mapRelation,
			ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
			ConfiguredRelationalPersister<K, KID> keyEntityPersister,
			ForeignKeyNamingStrategy foreignKeyNamingStrategy,
			ColumnNamingStrategy columnNamingStrategy,
			MapEntryTableNamingStrategy tableNamingStrategy,
			Dialect dialect,
			ConnectionConfiguration connectionConfiguration) {
		super(convertEntityMapToIdentifierMap(mapRelation, keyEntityPersister),
				sourcePersister,
				foreignKeyNamingStrategy,
				columnNamingStrategy,
				tableNamingStrategy,
				dialect,
				connectionConfiguration);
		this.originalMapRelation = mapRelation;
		this.keyEntityPersister = keyEntityPersister;
		this.mapGetter = originalMapRelation.getMapProvider()::get;
		this.inMemoryRelationHolder = new InMemoryRelationHolder<>();
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
					keyValuePairs.forEach(duo -> originalRelationFixer.apply(bean, duo));
				});
				
				inMemoryRelationHolder.clear();
			}
		});
		
		super.configure();
	}
	
	@Override
	<TT extends Table<TT>, TARGETTABLE extends Table<TARGETTABLE>>
	ClassMapping<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>, TARGETTABLE>
	buildKeyValueRecordMapping(TARGETTABLE targetTable,
							   IdentifierAssembler<SRCID, TT> sourceIdentifierAssembler,
							   Map<Column<TT, Object>, Column<TARGETTABLE, Object>> primaryKeyForeignColumnMapping,
							   EmbeddableMappingConfiguration<KID> keyEmbeddableConfiguration,
							   EmbeddableMappingConfiguration<V> valueEmbeddableConfiguration) {
		KeyValueRecordMappingBuilder<KID, V, SRCID, TARGETTABLE, TT> builder
				= new KeyValueRecordMappingBuilder<KID, V, SRCID, TARGETTABLE, TT>(targetTable, sourceIdentifierAssembler, primaryKeyForeignColumnMapping) {
			
			private final Map<Column<TARGETTABLE, Object>, Column<Table, Object>> foreignKeyBootstrap = new HashMap<>();
			
			@Override
			void withEntryKeyIsSingleProperty(Column<TARGETTABLE, KID> keyColumn) {
				super.withEntryKeyIsSingleProperty(keyColumn);
				Column<Table, Object> column = ((SimpleIdMapping) keyEntityPersister.getMapping().getIdMapping()).getIdentifierAssembler().getColumn();
				foreignKeyBootstrap.put((Column<TARGETTABLE, Object>) keyColumn, column);
				keyIdColumnsProjectInAssociationTable = Key.ofSingleColumn(keyColumn);
			}
			
			@Override
			void withEntryKeyIsComplexType(EmbeddedClassMapping<KID, TARGETTABLE> entryKeyMapping) {
				super.withEntryKeyIsComplexType(entryKeyMapping);
				KeyBuilder<TARGETTABLE, KID> keyIdColumnsProjectInAssociationTableBuilder = Key.from(targetTable);
				entryKeyMapping.getPropertyToColumn().values().forEach(keyIdColumnsProjectInAssociationTableBuilder::addColumn);
				keyIdColumnsProjectInAssociationTable = keyIdColumnsProjectInAssociationTableBuilder.build();
			}
			
			@Override
			KeyValueRecordMapping<KID, V, SRCID, TARGETTABLE> build() {
				KeyBuilder<TARGETTABLE, Object> keyBuilder1 = Key.from(targetTable);
				KeyBuilder<Table, Object> keyBuilder2 = Key.from(keyEntityPersister.getMainTable());
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
									EntityPersister<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>> mapEntryPersister,
									Accessor<SRC, MM> mapAccessor) {
		Function<SRC, Collection<KeyValueRecord<KID, V, SRCID>>> mapProviderForInsert = toRecordCollectionProvider(sourcePersister.getMapping(), false);
		sourcePersister.addInsertListener(new BeforeInsertCollectionCascader<SRC, K>(keyEntityPersister) {

			@Override
			protected Collection<K> getTargets(SRC src) {
				return mapGetter.apply(src).keySet();
			}
		});
		sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(mapEntryPersister, mapProviderForInsert));
	}
	
	@Override
	protected void addUpdateCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									EntityPersister<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>> elementRecordPersister) {
		Function<SRC, Set<Entry<K, V>>> targetEntitiesGetter = mapGetter.andThen(Map::entrySet);
		BiConsumer<Duo<SRC, SRC>, Boolean> MapUpdater = new MapUpdater<>(targetEntitiesGetter, keyEntityPersister, elementRecordPersister, sourcePersister);
		sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(MapUpdater));
	}
	
	@Override
	protected void addSelectCascade(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									SimpleRelationalEntityPersister<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>, ?> relationRecordPersister,
									PrimaryKey<?, SRCID> sourcePK,
									ForeignKey<?, ?, SRCID> keyValueRecordToSourceForeignKey,
									BiConsumer<SRC, MM> mapSetter,
									Function<SRC, MM> mapGetter,
									Supplier<MM> mapFactory) {
		
		BeanRelationFixer<SRC, KeyValueRecord<KID, V, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
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
			keyEntityPersister.joinAsMany(sourcePersister,
				(Key<Table, KID>) keyIdColumnsProjectInAssociationTable,
				primaryKey,
				new BeanRelationFixer<C, K>() {
					@Override
					public void apply(C bean, K input) {
						inMemoryRelationHolder3.store(((KeyValueRecord<KID, V, I>) bean).getId().getId(), keyEntityPersister.getId(input), input);
					}
				},
				null, associationTableJoinNodeName, true, false);
		 */
		PrimaryKey<?, KID> primaryKey = keyEntityPersister.getMainTable().getPrimaryKey();
		keyEntityPersister.joinAsMany(relationRecordPersister,
				(Key<Table, KID>) keyIdColumnsProjectInAssociationTable,
				primaryKey,
				(bean, input) -> inMemoryRelationHolder.store(bean.getId().getId(), keyEntityPersister.getId(input), input),
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
	 * requires to update {@link Map.Entry} as well as propagate insert / update /delete operation to key-entities. 
	 *
	 * @param <SRC> entity type owning the relation
	 * @param <SRCID> entity owning the relation identifier type 
	 * @param <K> Map key entity type
	 * @param <KID> Map key entity identifier type
	 * @param <V> Map value type
	 * @author Guillaume Mary
	 */
	private static class MapUpdater<SRC, SRCID, K, KID, V> extends CollectionUpdater<SRC, Entry<K, V>, Set<Entry<K, V>>> {
		
		private static <K, V, KID> EntityWriter<Entry<K, V>> asEntityWriter(ConfiguredRelationalPersister<K, KID> keyEntityPersister) {
			return new EntityWriter<Entry<K, V>>() {
				
				@Override
				public void update(Iterable<? extends Duo<Entry<K, V>, Entry<K, V>>> differencesIterable, boolean allColumnsStatement) {
					keyEntityPersister.update(Iterables.stream(differencesIterable)
							.map(duo -> new Duo<>(duo.getLeft().getKey(), duo.getRight().getKey()))
							.collect(Collectors.toSet()), allColumnsStatement);
				}
				
				@Override
				public void delete(Iterable<? extends Entry<K, V>> entities) {
					keyEntityPersister.delete(Iterables.stream(entities).map(Entry::getKey).collect(Collectors.toSet()));
				}
				
				@Override
				public void persist(Iterable<? extends Entry<K, V>> entities) {
					keyEntityPersister.persist(Iterables.stream(entities).map(Entry::getKey).collect(Collectors.toSet()));
				}
				
				@Override
				public boolean isNew(Entry<K, V> entity) {
					return keyEntityPersister.isNew(entity.getKey());
				}
				
				@Override
				public void updateById(Iterable<? extends Entry<K, V>> entities) {
					keyEntityPersister.updateById(Iterables.stream(entities).map(Entry::getKey).collect(Collectors.toSet()));
				}
			};
		}
		
		private final EntityPersister<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>> elementRecordPersister;
		
		private final ConfiguredRelationalPersister<SRC, SRCID> sourcePersister;
		
		private final ConfiguredRelationalPersister<K, KID> keyEntityPersister;
		
		public MapUpdater(Function<SRC, Set<Entry<K, V>>> targetEntitiesGetter,
						  ConfiguredRelationalPersister<K, KID> keyEntityPersister,
						  EntityPersister<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>> elementRecordPersister,
						  ConfiguredRelationalPersister<SRC, SRCID> sourcePersister) {
			super(targetEntitiesGetter, asEntityWriter(keyEntityPersister), (o, i) -> { /* no reverse setter because we store only raw values */ }, true, entry -> keyEntityPersister.getId(entry.getKey()));
			this.elementRecordPersister = elementRecordPersister;
			this.sourcePersister = sourcePersister;
			this.keyEntityPersister = keyEntityPersister;
		}
		
		@Override
		protected KeyValueAssociationTableUpdateContext newUpdateContext(Duo<SRC, SRC> updatePayload) {
			return new KeyValueAssociationTableUpdateContext(updatePayload);
		}
		
		@Override
		protected void onAddedElements(UpdateContext updateContext, AbstractDiff<Entry<K, V>> diff) {
			super.onAddedElements(updateContext, diff);
			KeyValueRecord<KID, V, SRCID> associationRecord = newRecord(updateContext.getPayload().getLeft(), diff.getReplacingInstance());
			((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeInserted().add(associationRecord);
		}
		
		@Override
		protected void onHeldElements(CollectionUpdater<SRC, Entry<K, V>, Set<Entry<K, V>>>.UpdateContext updateContext, AbstractDiff<Entry<K, V>> diff) {
			super.onHeldElements(updateContext, diff);
			Duo<KeyValueRecord<KID, V, SRCID>, KeyValueRecord<KID, V, SRCID>> associationRecord = new Duo<>(
					newRecord(updateContext.getPayload().getLeft(), diff.getReplacingInstance()),
					newRecord(updateContext.getPayload().getLeft(), diff.getSourceInstance())
			);
			((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeUpdated().add(associationRecord);
		}
		
		@Override
		protected void onRemovedElements(UpdateContext updateContext, AbstractDiff<Entry<K, V>> diff) {
			super.onRemovedElements(updateContext, diff);
			
			KeyValueRecord<KID, V, SRCID> associationRecord = newRecord(updateContext.getPayload().getLeft(), diff.getSourceInstance());
			((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeDeleted().add(associationRecord);
		}
		
		@Override
		protected void insertTargets(UpdateContext updateContext) {
			// we insert association records after targets to satisfy integrity constraint
			super.insertTargets(updateContext);
			elementRecordPersister.insert(((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeInserted());
		}
		
		@Override
		protected void updateTargets(CollectionUpdater<SRC, Entry<K, V>, Set<Entry<K, V>>>.UpdateContext updateContext, boolean allColumnsStatement) {
			super.updateTargets(updateContext, allColumnsStatement);
			elementRecordPersister.update(((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeUpdated(), allColumnsStatement);
		}
		
		@Override
		protected void deleteTargets(UpdateContext updateContext) {
			// we delete association records before targets to satisfy integrity constraint
			elementRecordPersister.delete(((KeyValueAssociationTableUpdateContext) updateContext).getAssociationRecordsToBeDeleted());
			super.deleteTargets(updateContext);
		}
		
		private KeyValueRecord<KID, V, SRCID> newRecord(SRC e, Entry<K, V> record) {
			return new KeyValueRecord<>(sourcePersister.getId(e), keyEntityPersister.getId(record.getKey()), record.getValue());
		}
		
		class KeyValueAssociationTableUpdateContext extends UpdateContext {
			
			private final List<KeyValueRecord<KID, V, SRCID>> associationRecordsToBeInserted = new ArrayList<>();
			private final List<Duo<KeyValueRecord<KID, V, SRCID>, KeyValueRecord<KID, V, SRCID>>> associationRecordsToBeUpdated = new ArrayList<>();
			private final List<KeyValueRecord<KID, V, SRCID>> associationRecordsToBeDeleted = new ArrayList<>();
			
			public KeyValueAssociationTableUpdateContext(Duo<SRC, SRC> updatePayload) {
				super(updatePayload);
			}
			
			public List<KeyValueRecord<KID, V, SRCID>> getAssociationRecordsToBeInserted() {
				return associationRecordsToBeInserted;
			}
			
			public List<Duo<KeyValueRecord<KID, V, SRCID>, KeyValueRecord<KID, V, SRCID>>> getAssociationRecordsToBeUpdated() {
				return associationRecordsToBeUpdated;
			}
			
			public List<KeyValueRecord<KID, V, SRCID>> getAssociationRecordsToBeDeleted() {
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
	 * @param <KID> Map key entity identifier type
	 * @param <V> Map value type
	 * @param <M> relation Map type
	 * @param <MM> redefined Map type to get entity key identifier 
	 * @author Guillaume Mary
	 */
	private static class MapAccessor<SRC, K, KID, V, M extends Map<K, V>, MM extends Map<KID, V>> implements Accessor<SRC, MM>, AccessorDefinitionDefiner<SRC> {
		
		private final MapRelation<SRC, K, V, M> map;
		
		private final ConfiguredRelationalPersister<K, KID> keyEntityPersister;
		private final AccessorDefinition accessorDefinition;
		
		public MapAccessor(MapRelation<SRC, K, V, M> map, ConfiguredRelationalPersister<K, KID> keyEntityPersister) {
			this.map = map;
			this.keyEntityPersister = keyEntityPersister;
			this.accessorDefinition = AccessorDefinition.giveDefinition(this.map.getMapProvider());
		}
		
		@Override
		public MM get(SRC SRC) {
			M m = map.getMapProvider().get(SRC);
			if (m != null) {
				MM result = (MM) new HashMap<>();    // we can use an HashMap since KID should have equals() + hashCode() implemented since its an identifier
				m.forEach((k, v) -> result.put(keyEntityPersister.getId(k), v));
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
	 * Look at joinAsMany(..) invocations in {@link EntityAsKeyMapRelationConfigurer#addSelectCascade(ConfiguredRelationalPersister, SimpleRelationalEntityPersister, PrimaryKey, ForeignKey, BiConsumer, Function, Supplier)}  
	 * This is the goal and need, implementation differ due to simplification made after first intent. 
	 * 
	 * Expected to be used in a {@link SelectListener} to {@link #init()} it before select and {@link #clear()} it after select.
	 * 
	 * @param <I>
	 * @param <K>
	 * @param <KID>
	 * @param <V>
	 * @author Guillaume Mary
	 */
	private static class InMemoryRelationHolder<I, K, KID, V> {
		
		/**
		 * In memory and temporary Map storage.
		 */
		private final ThreadLocal<Map<I, Map<KID, Duo<K, V>>>> relationCollectionPerEntity = new ThreadLocal<>();
		
		public InMemoryRelationHolder() {
		}
		
		public void store(I source, KeyValueRecord<KID, V, I> keyValueRecord) {
			Map<I, Map<KID, Duo<K, V>>> srcidcMap = relationCollectionPerEntity.get();
			Map<KID, Duo<K, V>> relatedDuos = srcidcMap.computeIfAbsent(source, id -> new HashMap<>());
			Duo<K, V> duo = nullable(relatedDuos.get(keyValueRecord.getKey())).getOr(() -> {
				Duo<K, V> result = new Duo<>();
				relatedDuos.put(keyValueRecord.getKey(), result);
				return result;
			});
			duo.setRight(keyValueRecord.getValue());
		}
		
		public void store(I source, KID identifier, K input) {
			Map<I, Map<KID, Duo<K, V>>> srcidcMap = relationCollectionPerEntity.get();
			Map<KID, Duo<K, V>> relatedDuos = srcidcMap.computeIfAbsent(source, id -> new HashMap<>());
			Duo<K, V> duo = nullable(relatedDuos.get(identifier)).getOr(() -> {
				Duo<K, V> result = new Duo<>();
				relatedDuos.put(identifier, result);
				return result;
			});
			duo.setLeft(input);
			// bidirectional assignment
//			preventNull(getReverseSetter(), NOOP_REVERSE_SETTER).accept(input, source);
		}
		
		public Collection<Duo<K, V>> get(I src) {
			Map<I, Map<KID, Duo<K, V>>> currentMap = relationCollectionPerEntity.get();
			return nullable(currentMap).map(map -> map.get(src)).map(Map::values).get();
		}
		
		public void init() {
			this.relationCollectionPerEntity.set(new HashMap<>());
		}
		
		public void clear() {
			this.relationCollectionPerEntity.remove();
		}
	}
}