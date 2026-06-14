package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.map.InMemoryRelationHolder;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.map.RecordId;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.AssemblyPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

public class AggregateMapAppender {
	
	private final MapResolver mapResolver;
	
	public AggregateMapAppender(SkeletonAggregateResolver skeletonAggregateResolver, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.mapResolver = new MapResolver(dialect, connectionConfiguration, skeletonAggregateResolver);
	}
	
	public <SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
	void append(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	            ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> resolvedRelation,
	            AssemblyPoint<SRC, SRCID, ?, LEFTTABLE> assemblyPawn) {
		
		ReadWritePropertyAccessPoint<SRC, M> mapAccessor = resolvedRelation.getAccessor();
		
		Holder<ConfiguredRelationalPersister<K, KID>> keyEntityPersisterHolder = new Holder<>();
		Holder<ConfiguredRelationalPersister<V, VID>> valueEntityPersisterHolder = new Holder<>();
		
		KeyValueRecordPersister<?, ?, SRCID, MAPTABLE> keyValueRecordPersister = mapResolver.resolve(
				resolvedRelation,
				assemblyPawn.getRelationOwnerPersister(),
				keyEntityPersisterHolder::set,
				valueEntityPersisterHolder::set);
		
		if (resolvedRelation.getKeyEntity() != null) {
			appendEntityAsKey(rootPersister,
					assemblyPawn,
					mapAccessor,
					(KeyValueRecordPersister<KID, V, SRCID, MAPTABLE>) keyValueRecordPersister,
					keyEntityPersisterHolder.get(),
					resolvedRelation.getComponentFactory(),
					resolvedRelation.getJoin(),
					resolvedRelation.getKeyEntityForeignKey());
		}
		
		if (resolvedRelation.getValueEntity() != null) {
			appendEntityAsValue(rootPersister,
					assemblyPawn,
					mapAccessor,
					(KeyValueRecordPersister<K, VID, SRCID, MAPTABLE>) keyValueRecordPersister,
					valueEntityPersisterHolder.get(),
					resolvedRelation.getComponentFactory(),
					resolvedRelation.getJoin(),
					resolvedRelation.getValueEntityForeignKey());
		}
		
		if (resolvedRelation.getKeyEntity() == null && resolvedRelation.getValueEntity() == null) {
			appendKeyAndValue(rootPersister,
					assemblyPawn,
					mapAccessor,
					(KeyValueRecordPersister<K, V, SRCID, MAPTABLE>) keyValueRecordPersister,
					resolvedRelation.getComponentFactory(),
					resolvedRelation.getJoin());
		}
	}
	
	private <SRC, SRCID, K, KID, V, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>>
	void appendEntityAsKey(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                       AssemblyPoint<SRC, SRCID, ?, LEFTTABLE> assemblyPawn,
	                       ReadWritePropertyAccessPoint<SRC, M> mapAccessor,
	                       KeyValueRecordPersister<KID, V, SRCID, MAPTABLE> keyValueRecordPersister,
	                       ConfiguredRelationalPersister<K, KID> keyEntityPersister,
	                       Supplier<M> componentFactory,
						   DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID> join,
						   ForeignKey<MAPTABLE, KTABLE, KID> keyEntityReferenceMapping) {
		InMemoryRelationHolder<SRCID, KID, V, K> inMemoryRelationHolder = new InMemoryRelationHolder<>(trio -> new Duo<>(trio.getEntity(), trio.getEntryValue()));
		BeanRelationFixer<SRC, KeyValueRecord<KID, V, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
				mapAccessor,
				mapAccessor,
				componentFactory,
				(bean, input, map) -> {
					inMemoryRelationHolder.storeRelation(input.getId().getId(), input.getKey(), input.getValue());
				});
		
		EntityMappingAdapter<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>, MAPTABLE> inflater = new EntityMappingAdapter<>(keyValueRecordPersister.getMapping());
		String mapJoinNodeName = rootPersister.getEntityJoinTree().addRelationJoin(
				assemblyPawn.getParentJoinPoint(),
				inflater,
				mapAccessor,
				join.getLeftKey(),
				join.getRightKey(),
				null,
				OUTER,
				relationFixer,
				Collections.emptySet(),
				null);
		
		// Finally put elements into source entities by converting in-memory stored objects as Map entries.
		rootPersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				inMemoryRelationHolder.init();
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				BeanRelationFixer<SRC, Duo<K, V>> originalRelationFixer = BeanRelationFixer.ofMapAdapter(
						mapAccessor,
						mapAccessor,
						componentFactory,
						(bean, duo, map) -> map.put(duo.getLeft(), duo.getRight()));
				result.forEach(bean -> {
					Collection<Duo<K, V>> keyValuePairs = (Collection) inMemoryRelationHolder.get(rootPersister.getId(bean));
					if (keyValuePairs != null) {
						keyValuePairs.forEach(duo -> originalRelationFixer.apply(bean, duo));
					} // else : no association record
				});
				
				inMemoryRelationHolder.clear();
			}
		});
		
		rootPersister.getEntityJoinTree().addRelationJoin(
				mapJoinNodeName,
				new EntityMappingAdapter<>(keyEntityPersister.<KTABLE>getMapping()),
				mapAccessor,
				keyEntityReferenceMapping.getSourceKey(),
				keyEntityReferenceMapping.getReferencedKey(),
				null,
				OUTER,
				(bean, input) -> {
					KeyValueRecord<KID, V, SRCID> record = (KeyValueRecord<KID, V, SRCID>) bean;
					inMemoryRelationHolder.storeEntity(record.getId().getId(), record.getKey(), input);
				},
				Collections.emptySet(),
				null);
	}
	
	private <SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, VTABLE extends Table<VTABLE>>
	void appendEntityAsValue(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                       AssemblyPoint<SRC, SRCID, ?, LEFTTABLE> assemblyPawn,
	                       ReadWritePropertyAccessPoint<SRC, M> mapAccessor,
	                       KeyValueRecordPersister<K, VID, SRCID, MAPTABLE> keyValueRecordPersister,
	                       ConfiguredRelationalPersister<V, VID> valueEntityPersister,
	                       Supplier<M> componentFactory,
	                       DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID> join,
	                       ForeignKey<MAPTABLE, VTABLE, KID> keyEntityReferenceMapping) {
		InMemoryRelationHolder<SRCID, K, VID, V> inMemoryRelationHolder = new InMemoryRelationHolder<>(trio -> new Duo<>(trio.getKeyLookup(), trio.getEntity()));
		BeanRelationFixer<SRC, KeyValueRecord<K, VID, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
				mapAccessor,
				mapAccessor,
				componentFactory,
				(bean, input, map) -> {
					inMemoryRelationHolder.storeRelation(input.getId().getId(), input.getKey(), input.getValue());
				});
		
		EntityMappingAdapter<KeyValueRecord<K, VID, SRCID>, RecordId<K, SRCID>, MAPTABLE> inflater = new EntityMappingAdapter<>(keyValueRecordPersister.getMapping());
		String mapJoinNodeName = rootPersister.getEntityJoinTree().addRelationJoin(
				assemblyPawn.getParentJoinPoint(),
				inflater,
				mapAccessor,
				join.getLeftKey(),
				join.getRightKey(),
				null,
				OUTER,
				relationFixer,
				Collections.emptySet(),
				null);
		
		// Finally put elements into source entities by converting in-memory stored objects as Map entries.
		rootPersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				inMemoryRelationHolder.init();
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				BeanRelationFixer<SRC, Duo<K, V>> originalRelationFixer = BeanRelationFixer.ofMapAdapter(
						mapAccessor,
						mapAccessor,
						componentFactory,
						(bean, duo, map) -> map.put(duo.getLeft(), duo.getRight()));
				result.forEach(bean -> {
					Collection<Duo<K, V>> keyValuePairs = (Collection) inMemoryRelationHolder.get(rootPersister.getId(bean));
					if (keyValuePairs != null) {
						keyValuePairs.forEach(duo -> originalRelationFixer.apply(bean, duo));
					} // else : no association record
				});
				
				inMemoryRelationHolder.clear();
			}
		});
		
		rootPersister.getEntityJoinTree().addRelationJoin(
				mapJoinNodeName,
				new EntityMappingAdapter<>(valueEntityPersister.<VTABLE>getMapping()),
				mapAccessor,
				keyEntityReferenceMapping.getSourceKey(),
				keyEntityReferenceMapping.getReferencedKey(),
				null,
				OUTER,
				(bean, input) -> {
					KeyValueRecord<K, VID, SRCID> record = (KeyValueRecord<K, VID, SRCID>) bean;
					inMemoryRelationHolder.storeEntity(record.getId().getId(), record.getKey(), input);
				},
				Collections.emptySet(),
				null);
	}
	
	private <SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, VTABLE extends Table<VTABLE>>
	void appendKeyAndValue(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                         AssemblyPoint<SRC, SRCID, ?, LEFTTABLE> assemblyPawn,
	                         ReadWritePropertyAccessPoint<SRC, M> mapAccessor,
	                         KeyValueRecordPersister<K, V, SRCID, MAPTABLE> keyValueRecordPersister,
	                         Supplier<M> componentFactory,
	                         DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID> join) {
		InMemoryRelationHolder<SRCID, K, V, V> inMemoryRelationHolder = new InMemoryRelationHolder<>(trio -> new Duo<>(trio.getKeyLookup(), trio.getEntryValue()));
		BeanRelationFixer<SRC, KeyValueRecord<K, V, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
				mapAccessor,
				mapAccessor,
				componentFactory,
				(bean, input, map) -> {
					inMemoryRelationHolder.storeRelation(input.getId().getId(), input.getKey(), input.getValue());
				});
		
		EntityMappingAdapter<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>, MAPTABLE> inflater = new EntityMappingAdapter<>(keyValueRecordPersister.getMapping());
		String mapJoinNodeName = rootPersister.getEntityJoinTree().addRelationJoin(
				assemblyPawn.getParentJoinPoint(),
				inflater,
				mapAccessor,
				join.getLeftKey(),
				join.getRightKey(),
				null,
				OUTER,
				relationFixer,
				Collections.emptySet(),
				null);
		
		// Finally put elements into source entities by converting in-memory stored objects as Map entries.
		rootPersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				inMemoryRelationHolder.init();
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				BeanRelationFixer<SRC, Duo<K, V>> originalRelationFixer = BeanRelationFixer.ofMapAdapter(
						mapAccessor,
						mapAccessor,
						componentFactory,
						(bean, duo, map) -> map.put(duo.getLeft(), duo.getRight()));
				result.forEach(bean -> {
					Collection<Duo<K, V>> keyValuePairs = (Collection) inMemoryRelationHolder.get(rootPersister.getId(bean));
					if (keyValuePairs != null) {
						keyValuePairs.forEach(duo -> originalRelationFixer.apply(bean, duo));
					} // else : no association record
				});
				
				inMemoryRelationHolder.clear();
			}
		});
	}
}

