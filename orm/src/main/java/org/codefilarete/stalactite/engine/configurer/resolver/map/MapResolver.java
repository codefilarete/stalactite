package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecordMapping;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecordMapping.KeyValueRecordIdMapping;
import org.codefilarete.stalactite.engine.configurer.map.RecordId;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.AfterUpdateTrigger;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.Nullable.nullable;

public class MapResolver {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public MapResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	public <SRC, SRCID, K, V, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>>
	KeyValueRecordPersister<K, V, SRCID, MAPTABLE> resolve(ResolvedMapRelation<SRC, K, V, M, SRCID, LEFTTABLE, MAPTABLE> resolvedRelation,
	                                                       ConfiguredRelationalPersister<SRC, SRCID> sourcePersister) {
		
		KeyValueRecordMapping<K, V, SRCID, MAPTABLE> relationRecordMapping = buildKeyValueRecordMapping(resolvedRelation, sourcePersister);
		KeyValueRecordPersister<K, V, SRCID, MAPTABLE> relationRecordPersister =
				new KeyValueRecordPersister<>(relationRecordMapping, dialect, connectionConfiguration);
		
		Accessor<SRC, Collection<KeyValueRecord<K, V, SRCID>>> collectionProviderForInsert =
				toRecordCollectionProvider(resolvedRelation, sourcePersister.getMapping(), false);
		sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(relationRecordPersister, collectionProviderForInsert));
		
		Accessor<SRC, Collection<KeyValueRecord<K, V, SRCID>>> collectionProviderAsPersistedInstances =
				toRecordCollectionProvider(resolvedRelation, sourcePersister.getMapping(), true);
		Mutator<Duo<SRC, SRC>, Boolean> collectionUpdater = new CollectionUpdater<SRC, KeyValueRecord<K, V, SRCID>, Collection<KeyValueRecord<K, V, SRCID>>>(
				collectionProviderAsPersistedInstances,
				relationRecordPersister,
				(o, i) -> { /* no reverse setter because we store only raw values */ },
				true,
				// We use source id + entry key as the row identity to compute map diffs.
				KeyValueRecord::footprint) {
			@Override
			protected void insertTargets(UpdateContext updateContext) {
				relationRecordPersister.insert(updateContext.getAddedElements());
			}
		};
		sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(collectionUpdater));
		
		sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(
				relationRecordPersister,
				collectionProviderAsPersistedInstances));
		
		return relationRecordPersister;
	}
	
	private <SRC, SRCID, K, V, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>>
	KeyValueRecordMapping<K, V, SRCID, MAPTABLE> buildKeyValueRecordMapping(ResolvedMapRelation<SRC, K, V, M, SRCID, LEFTTABLE, MAPTABLE> resolvedRelation,
	                                                                        ConfiguredRelationalPersister<SRC, SRCID> sourcePersister) {
		IdentifierAssembler<SRCID, LEFTTABLE> sourceIdentifierAssembler = sourcePersister.getMapping().getIdMapping().getIdentifierAssembler();
		Map<ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, ?>, Column<MAPTABLE, ?>> keyColumnMapping = new HashMap<>();
		resolvedRelation.getColumnMapping().forEach((accessor, column) -> {
			if (column.isPrimaryKey()) {
				keyColumnMapping.put(accessor, column);
			}
		});
		
		Function<ColumnedRow, K> entryKeyAssembler = columnedRow -> {
			KeyValueRecord<K, V, SRCID> keyValueRecord = new KeyValueRecord<>();
			keyColumnMapping.forEach((accessor, column) -> ((ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, Object>) accessor)
					.set(keyValueRecord, columnedRow.get(column)));
			return keyValueRecord.getKey();
		};
		Function<K, Map<Column<MAPTABLE, ?>, ?>> entryKeyColumnValueProvider = key -> {
			KeyValueRecord<K, V, SRCID> keyValueRecord = new KeyValueRecord<>(null, key, null);
			Map<Column<MAPTABLE, ?>, Object> result = new HashMap<>();
			keyColumnMapping.forEach((accessor, column) -> result.put(
					column,
					((ReadWritePropertyAccessPoint<KeyValueRecord<K, V, SRCID>, Object>) accessor).get(keyValueRecord)));
			return result;
		};
		
		KeyValueRecordIdMapping<K, SRCID, MAPTABLE> idMapping = new KeyValueRecordIdMapping<>(
				resolvedRelation.getJoin().getRightKey().getTable(),
				entryKeyAssembler,
				entryKeyColumnValueProvider,
				sourceIdentifierAssembler,
				resolvedRelation.getPrimaryKeyForeignKeyColumnMapping());
		return new KeyValueRecordMapping<>(
				resolvedRelation.getJoin().getRightKey().getTable(),
				resolvedRelation.getColumnMapping(),
				idMapping);
	}
	
	private static <SRC, SRCID, K, V, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>>
	Accessor<SRC, Collection<KeyValueRecord<K, V, SRCID>>> toRecordCollectionProvider(ResolvedMapRelation<SRC, K, V, M, SRCID, LEFTTABLE, MAPTABLE> resolvedRelation,
	                                                                                  IdAccessor<SRC, SRCID> idAccessor,
	                                                                                  boolean markAsPersisted) {
		return src -> Iterables.collect(
				nullable(resolvedRelation.getAccessor().get(src)).getOr(() -> (M) Collections.emptyMap()).entrySet(),
				entry -> new KeyValueRecord<>(idAccessor.getId(src), entry.getKey(), entry.getValue()).setPersisted(markAsPersisted),
				HashSet::new);
	}
	
	private static class TargetInstancesInsertCascader<SRC, K, V, SRCID> extends AfterInsertCollectionCascader<SRC, KeyValueRecord<K, V, SRCID>> {
		
		private final Accessor<SRC, ? extends Collection<KeyValueRecord<K, V, SRCID>>> mapGetter;
		
		private TargetInstancesInsertCascader(EntityPersister<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>> targetPersister,
		                                      Accessor<SRC, ? extends Collection<KeyValueRecord<K, V, SRCID>>> mapGetter) {
			super(targetPersister);
			this.mapGetter = mapGetter;
		}
		
		@Override
		protected void postTargetInsert(Iterable<? extends KeyValueRecord<K, V, SRCID>> entities) {
			// Nothing to do. Identified#isPersisted flag should be fixed by target persister.
		}
		
		@Override
		protected Collection<KeyValueRecord<K, V, SRCID>> getTargets(SRC source) {
			return mapGetter.get(source);
		}
	}
	
	public static class KeyValueRecordPersister<K, V, SRCID, T extends Table<T>>
			extends SimpleRelationalEntityPersister<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>, T> {
		
		public KeyValueRecordPersister(KeyValueRecordMapping<K, V, SRCID, T> keyValueRecordMapping,
		                               Dialect dialect,
		                               ConnectionConfiguration connectionConfiguration) {
			super(keyValueRecordMapping, dialect, connectionConfiguration);
		}
	}
}

