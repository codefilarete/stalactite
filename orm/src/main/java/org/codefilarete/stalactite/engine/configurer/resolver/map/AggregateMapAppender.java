package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
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
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.tool.Nullable.nullable;

public class AggregateMapAppender {
	
	private final MapResolver mapResolver;
	
	public AggregateMapAppender(SkeletonAggregateResolver skeletonAggregateResolver, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.mapResolver = new MapResolver(dialect, connectionConfiguration, skeletonAggregateResolver);
	}
	
	public <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>, VTABLE extends Table<VTABLE>>
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
		
		InMemoryRelationHolder<SRCID, X, Y> inMemoryRelationHolder = new InMemoryRelationHolder<>();
		
		// Function expected to provide the values to be put into the map of the source entity after the select.
		// It is expected to be based on the content of the in memory relation holder that is filled during the select
		// with the association table content and eventually with the key and value entities if they are defined
		Function<SRCID, Set<Duo<K, V>>> inMemoryRelationAdapter;
		
		SelectListener<SRC, SRCID> memoryHolderInitializer = new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				inMemoryRelationHolder.init();
			}
		};
		
		SelectListener<SRC, SRCID> memoryHolderClearer = new SelectListener<SRC, SRCID>() {
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				inMemoryRelationHolder.clear();
			}
		};
		
		if (resolvedRelation.getKeyEntity() != null && resolvedRelation.getValueEntity() != null) {
			InMemoryRelationHolder<SRCID, KID, K> inMemoryKeyRelationHolder = new InMemoryRelationHolder<>();
			InMemoryRelationHolder<SRCID, VID, V> inMemoryValueRelationHolder = new InMemoryRelationHolder<>();
			
			BeanRelationFixer<SRC, KeyValueRecord<KID, K, SRCID>> keyRelationFixer = BeanRelationFixer.ofMapAdapter(
					mapAccessor,
					mapAccessor,
					resolvedRelation.getComponentFactory(),
					(bean, input, map) -> {
						inMemoryKeyRelationHolder.storeRelation(input.getId().getId(), input.getKey(), input.getValue());
					});
			
			BeanRelationFixer<SRC, KeyValueRecord<VID, V, SRCID>> valueRelationFixer = BeanRelationFixer.ofMapAdapter(
					mapAccessor,
					mapAccessor,
					resolvedRelation.getComponentFactory(),
					(bean, input, map) -> {
						inMemoryValueRelationHolder.storeRelation(input.getId().getId(), input.getKey(), input.getValue());
					});
			
			inMemoryRelationAdapter = new Function<SRCID, Set<Duo<K, V>>>() {
				@Override
				public Set<Duo<K, V>> apply(SRCID srcid) {
					Collection<Duo<X, Y>> duos = inMemoryRelationHolder.get(srcid);
					if (duos != null) {
						Map<KID, K> keyEntities = Iterables.map(inMemoryKeyRelationHolder.get(srcid), Duo::getLeft, Duo::getRight);
						Map<VID, V> valueEntities = Iterables.map(inMemoryValueRelationHolder.get(srcid), Duo::getLeft, Duo::getRight);
						if (keyEntities != null) {
							Set<Duo<K, V>> result = new HashSet<>();
							duos.forEach(duo -> {
								result.add(new Duo<>(keyEntities.get(duo.getLeft()), valueEntities.get(duo.getRight())));
							});
							return result;
						} else {
							return null;
						}
					} else {
						return null;
					}
				}
			};
			
			memoryHolderInitializer = memoryHolderInitializer.then(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					inMemoryKeyRelationHolder.init();
					inMemoryValueRelationHolder.init();
				}
			});
			memoryHolderClearer = memoryHolderClearer.then(new SelectListener<SRC, SRCID>() {
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					inMemoryKeyRelationHolder.clear();
					inMemoryValueRelationHolder.clear();
				}
			});
			
			appendEntityAsKeyJoin(rootPersister,
					assemblyPawn,
					mapAccessor,
					(KeyValueRecordPersister<KID, V, SRCID, MAPTABLE>) keyValueRecordPersister,
					keyEntityPersisterHolder.get(),
					resolvedRelation.getComponentFactory(),
					resolvedRelation.getJoin(),
					resolvedRelation.getKeyEntityForeignKey(),
					keyRelationFixer,
					inMemoryKeyRelationHolder);
			
			appendEntityAsValueJoin(rootPersister,
					assemblyPawn,
					mapAccessor,
					(KeyValueRecordPersister<K, VID, SRCID, MAPTABLE>) keyValueRecordPersister,
					valueEntityPersisterHolder.get(),
					resolvedRelation.getComponentFactory(),
					resolvedRelation.getJoin(),
					resolvedRelation.getValueEntityForeignKey(),
					valueRelationFixer,
					inMemoryValueRelationHolder);
			
		} else if (resolvedRelation.getKeyEntity() != null) {
			InMemoryRelationHolder<SRCID, KID, K> inMemoryKeyRelationHolder = new InMemoryRelationHolder<>();
			
			BeanRelationFixer<SRC, KeyValueRecord<KID, K, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
					mapAccessor,
					mapAccessor,
					resolvedRelation.getComponentFactory(),
					(bean, input, map) -> {
						inMemoryKeyRelationHolder.storeRelation(input.getId().getId(), input.getKey(), input.getValue());
					});
			
			inMemoryRelationAdapter = new Function<SRCID, Set<Duo<K, V>>>() {
				@Override
				public Set<Duo<K, V>> apply(SRCID srcid) {
					Collection<Duo<X, Y>> duos = inMemoryRelationHolder.get(srcid);
					if (duos != null) {
						Map<KID, K> entities = Iterables.map(inMemoryKeyRelationHolder.get(srcid), Duo::getLeft, Duo::getRight);
						if (entities != null) {
							Set<Duo<K, V>> result = new HashSet<>();
							duos.forEach(duo -> {
								result.add(new Duo<>(entities.get(duo.getLeft()), (V) duo.getRight()));
							});
							return result;
						} else {
							return null;
						}
					} else {
						return null;
					}
				}
			};
			
			memoryHolderInitializer = memoryHolderInitializer.then(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					inMemoryKeyRelationHolder.init();
				}
			});	
			memoryHolderClearer = memoryHolderClearer.then(new SelectListener<SRC, SRCID>() {
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					inMemoryKeyRelationHolder.clear();
				}
			});
			
			appendEntityAsKeyJoin(rootPersister,
					assemblyPawn,
					mapAccessor,
					(KeyValueRecordPersister<KID, V, SRCID, MAPTABLE>) keyValueRecordPersister,
					keyEntityPersisterHolder.get(),
					resolvedRelation.getComponentFactory(),
					resolvedRelation.getJoin(),
					resolvedRelation.getKeyEntityForeignKey(),
					relationFixer,
					inMemoryKeyRelationHolder);
		} else if (resolvedRelation.getValueEntity() != null) {
			InMemoryRelationHolder<SRCID, VID, V> inMemoryValueRelationHolder = new InMemoryRelationHolder<>();
			
			BeanRelationFixer<SRC, KeyValueRecord<VID, V, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
					mapAccessor,
					mapAccessor,
					resolvedRelation.getComponentFactory(),
					(bean, input, map) -> {
						inMemoryValueRelationHolder.storeRelation(input.getId().getId(), input.getKey(), input.getValue());
					});
			
			inMemoryRelationAdapter = new Function<SRCID, Set<Duo<K, V>>>() {
				@Override
				public Set<Duo<K, V>> apply(SRCID srcid) {
					Collection<Duo<X, Y>> duos = inMemoryRelationHolder.get(srcid);
					if (duos != null) {
						Map<VID, V> entities = Iterables.map(inMemoryValueRelationHolder.get(srcid), Duo::getLeft, Duo::getRight);
						if (entities != null) {
							Set<Duo<K, V>> result = new HashSet<>();
							duos.forEach(duo -> {
								result.add(new Duo<>((K) duo.getLeft(), entities.get((V) duo.getRight())));
							});
							return result;
						} else {
							return null;
						}
					} else {
						return null;
					}
				}
			};
			
			memoryHolderInitializer = memoryHolderInitializer.then(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					inMemoryValueRelationHolder.init();
				}
			});
			memoryHolderClearer = memoryHolderClearer.then(new SelectListener<SRC, SRCID>() {
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					inMemoryValueRelationHolder.clear();
				}
			});
			
			appendEntityAsValueJoin(rootPersister,
					assemblyPawn,
					mapAccessor,
					(KeyValueRecordPersister<K, VID, SRCID, MAPTABLE>) keyValueRecordPersister,
					valueEntityPersisterHolder.get(),
					resolvedRelation.getComponentFactory(),
					resolvedRelation.getJoin(),
					resolvedRelation.getValueEntityForeignKey(),
					relationFixer,
					inMemoryValueRelationHolder);
		} else if (resolvedRelation.getKeyEntity() == null && resolvedRelation.getValueEntity() == null) {
			inMemoryRelationAdapter = new Function<SRCID, Set<Duo<K, V>>>() {
				@Override
				public Set<Duo<K, V>> apply(SRCID srcid) {
					Collection<Duo<X, Y>> duos = inMemoryRelationHolder.get(srcid);
					if (duos != null) {
						return duos.stream().map(duo -> {
							return new Duo<>(((K) duo.getLeft()), ((V) duo.getRight()));
						}).collect(Collectors.toSet());
					} else {
						return null;
					}
				}
			};
		} else {
			inMemoryRelationAdapter = null;
		}
		
		appendAssociationTableJoin(rootPersister, resolvedRelation, assemblyPawn, mapAccessor, inMemoryRelationHolder, (KeyValueRecordPersister<K, V, SRCID, MAPTABLE>) keyValueRecordPersister);
		
		rootPersister.addSelectListener(memoryHolderInitializer.then(
				new SelectListener<SRC, SRCID>() {
					
					@Override
					public void afterSelect(Set<? extends SRC> result) {
						BeanRelationFixer<SRC, Duo<K, V>> originalRelationFixer = BeanRelationFixer.ofMapAdapter(
								mapAccessor,
								mapAccessor,
								resolvedRelation.getComponentFactory(),
								(bean, duo, map) -> map.put(duo.getLeft(), duo.getRight()));
						result.forEach(bean -> {
							Collection<Duo<K, V>> keyValuePairs = inMemoryRelationAdapter.apply(rootPersister.getId(bean));
							if (keyValuePairs != null) {
								keyValuePairs.forEach(duo -> originalRelationFixer.apply(bean, duo));
							} // else : no association record
						});
					}
				}).then(memoryHolderClearer));
	}
	
	private <X, Y, SRC, SRCID, K, KID, V, VID, M extends Map<K, V>,
			LEFTTABLE extends Table<LEFTTABLE>,
			MAPTABLE extends Table<MAPTABLE>,
			KTABLE extends Table<KTABLE>,
			VTABLE extends Table<VTABLE>>
	void appendAssociationTableJoin(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                                ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> resolvedRelation,
	                                AssemblyPoint<SRC, SRCID, ?, LEFTTABLE> assemblyPawn,
	                                ReadWritePropertyAccessPoint<SRC, M> mapAccessor,
	                                InMemoryRelationHolder<SRCID, X, Y> inMemoryRelationHolder,
	                                KeyValueRecordPersister<K, V, SRCID, MAPTABLE> keyValueRecordPersister) {
		BeanRelationFixer<SRC, KeyValueRecord<X, Y, SRCID>> relationFixer = BeanRelationFixer.ofMapAdapter(
				mapAccessor,
				mapAccessor,
				resolvedRelation.getComponentFactory(),
				(bean, input, map) -> {
					inMemoryRelationHolder.storeRelation(input.getId().getId(), input.getKey(), input.getValue());
				});
		
		EntityMappingAdapter<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>, MAPTABLE> recordInflater = new EntityMappingAdapter<>(keyValueRecordPersister.getMapping());
		EntityMappingAdapter<KeyValueRecord<X, Y, SRCID>, RecordId<X, SRCID>, MAPTABLE> inflater = (EntityMappingAdapter) recordInflater;
		String mapJoinNodeName = rootPersister.getEntityJoinTree().addRelationJoin(
				assemblyPawn.getParentJoinPoint(),
				inflater,
				mapAccessor,
				resolvedRelation.getJoin().getLeftKey(),
				resolvedRelation.getJoin().getRightKey(),
				null,
				OUTER,
				relationFixer,
				Collections.emptySet(),
				null);
	}
	
	private <SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, KTABLE extends Table<KTABLE>>
	void appendEntityAsKeyJoin(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                           AssemblyPoint<SRC, SRCID, ?, LEFTTABLE> assemblyPawn,
	                           ReadWritePropertyAccessPoint<SRC, M> mapAccessor,
	                           KeyValueRecordPersister<KID, V, SRCID, MAPTABLE> keyValueRecordPersister,
	                           ConfiguredRelationalPersister<K, KID> keyEntityPersister,
	                           Supplier<M> componentFactory,
	                           DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID> join,
	                           ForeignKey<MAPTABLE, KTABLE, KID> keyEntityReferenceMapping,
	                           BeanRelationFixer<SRC, KeyValueRecord<KID, K, SRCID>> relationFixer,
	                           InMemoryRelationHolder<SRCID, KID, K> inMemoryRelationHolder) {
		
		EntityMappingAdapter<KeyValueRecord<KID, V, SRCID>, RecordId<KID, SRCID>, MAPTABLE> recordInflater = new EntityMappingAdapter<>(keyValueRecordPersister.getMapping());
		EntityMappingAdapter<KeyValueRecord<KID, K, SRCID>, RecordId<KID, SRCID>, MAPTABLE> inflater = (EntityMappingAdapter) recordInflater;
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
					inMemoryRelationHolder.storeRelation(record.getId().getId(), record.getKey(), input);
				},
				Collections.emptySet(),
				null);
	}
	
	private <SRC, SRCID, K, KID, V, VID, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>, VTABLE extends Table<VTABLE>>
	void appendEntityAsValueJoin(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                             AssemblyPoint<SRC, SRCID, ?, LEFTTABLE> assemblyPawn,
	                             ReadWritePropertyAccessPoint<SRC, M> mapAccessor,
	                             KeyValueRecordPersister<K, VID, SRCID, MAPTABLE> keyValueRecordPersister,
	                             ConfiguredRelationalPersister<V, VID> valueEntityPersister,
	                             Supplier<M> componentFactory,
	                             DirectRelationJoin<LEFTTABLE, MAPTABLE, SRCID> join,
	                             ForeignKey<MAPTABLE, VTABLE, VID> keyEntityReferenceMapping,
	                             BeanRelationFixer<SRC, KeyValueRecord<VID, V, SRCID>> relationFixer,
	                             InMemoryRelationHolder<SRCID, VID, V> inMemoryRelationHolder) {
		
		EntityMappingAdapter<KeyValueRecord<K, VID, SRCID>, RecordId<K, SRCID>, MAPTABLE> recordInflater = new EntityMappingAdapter<>(keyValueRecordPersister.getMapping());
		EntityMappingAdapter<KeyValueRecord<VID, V, SRCID>, RecordId<K, SRCID>, MAPTABLE> inflater = (EntityMappingAdapter) recordInflater;
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
					inMemoryRelationHolder.storeRelation(record.getId().getId(), record.getValue(), input);
				},
				Collections.emptySet(),
				null);
	}
	
	public static class InMemoryRelationHolder<I, K, V> {
		
		/**
		 * In memory and temporary Map storage.
		 */
		private final ThreadLocal<Map<I, Set<Duo<K, V>>>> relationCollectionPerEntity = new ThreadLocal<>();
		
		
		public void storeRelation(I source, K key, V value) {
			Map<I, Set<Duo<K, V>>> srcidcMap = relationCollectionPerEntity.get();
			Set<Duo<K, V>> relatedDuos = srcidcMap.computeIfAbsent(source, id -> new HashSet<>());
			Duo<K, V> duo = relatedDuos.stream().filter(pawn -> Objects.equals(pawn.getLeft(), key)).findAny().orElseGet(() -> {
				Duo<K, V> result = new Duo<>();
				relatedDuos.add(result);
				return result;
			});
			duo.setLeft(key);
			duo.setRight(value);
		}
		
		public Collection<Duo<K, V>> get(I src) {
			Map<I, Set<Duo<K, V>>> currentMap = relationCollectionPerEntity.get();
			return nullable(currentMap)
					.map(map -> map.get(src))
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

