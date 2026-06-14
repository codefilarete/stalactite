package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Map;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class MapResolver {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	
	public MapResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration, SkeletonAggregateResolver skeletonAggregateResolver) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.skeletonAggregateResolver = skeletonAggregateResolver;
	}
	
	public <SRC, SRCID, K, KID, V, VID, M extends Map<K, V>,
			LEFTTABLE extends Table<LEFTTABLE>,
			MAPTABLE extends Table<MAPTABLE>,
			KTABLE extends Table<KTABLE>,
			VTABLE extends Table<VTABLE>>
	KeyValueRecordPersister<?, ?, SRCID, MAPTABLE> resolve(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> resolvedRelation,
	                                                       ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
	                                                       Consumer<ConfiguredRelationalPersister<K, KID>> createdKeyPersisterConsumer,
	                                                       Consumer<ConfiguredRelationalPersister<V, VID>> createdValuePersisterConsumer) {
		
		ConfiguredRelationalPersister<K, KID> keyEntityPersister = null;
		if (resolvedRelation.getKeyEntity() != null) {
			keyEntityPersister = skeletonAggregateResolver.buildPersister(resolvedRelation.getKeyEntity());
			createdKeyPersisterConsumer.accept(keyEntityPersister);
		}
		
		ConfiguredRelationalPersister<V, VID> valueEntityPersister = null;
		if (resolvedRelation.getValueEntity() != null) {
			valueEntityPersister = skeletonAggregateResolver.buildPersister(resolvedRelation.getValueEntity());
			createdValuePersisterConsumer.accept(valueEntityPersister);
		}
		
		EntryMapResolver keyEntityMapResolver = new EntryMapResolver(dialect, connectionConfiguration);
		return keyEntityMapResolver.resolve(resolvedRelation, sourcePersister, keyEntityPersister, valueEntityPersister);
	}
}

