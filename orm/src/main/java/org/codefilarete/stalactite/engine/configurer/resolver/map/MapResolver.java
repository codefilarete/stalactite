package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Map;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class MapResolver {
	
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public MapResolver(SkeletonAggregateResolver skeletonAggregateResolver, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.skeletonAggregateResolver = skeletonAggregateResolver;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	public <SRC, SRCID, K, KID, V, VID, M extends Map<K, V>,
			LEFTTABLE extends Table<LEFTTABLE>,
			MAPTABLE extends Table<MAPTABLE>,
			KTABLE extends Table<KTABLE>,
			VTABLE extends Table<VTABLE>>
	KeyValueRecordPersister<?, ?, SRCID, MAPTABLE> resolve(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> resolvedRelation,
	                                                       EntityWriteExecutor<SRC, SRCID> sourcePersister,
	                                                       Consumer<EntityWriteExecutor<K, KID>> createdKeyPersisterConsumer,
	                                                       Consumer<EntityWriteExecutor<V, VID>> createdValuePersisterConsumer) {
		
		EntityWriteExecutor<K, KID> keyEntityPersister = null;
		if (resolvedRelation.getKeyEntityDefinition() != null) {
			keyEntityPersister = skeletonAggregateResolver.buildPersister(resolvedRelation.getKeyEntityDefinition().getEntity());
			createdKeyPersisterConsumer.accept(keyEntityPersister);
		}
		
		EntityWriteExecutor<V, VID> valueEntityPersister = null;
		if (resolvedRelation.getValueEntityDefinition() != null) {
			valueEntityPersister = skeletonAggregateResolver.buildPersister(resolvedRelation.getValueEntityDefinition().getEntity());
			createdValuePersisterConsumer.accept(valueEntityPersister);
		}
		
		EntryMapResolver keyEntityMapResolver = new EntryMapResolver(dialect, connectionConfiguration);
		return keyEntityMapResolver.resolve(resolvedRelation, sourcePersister, keyEntityPersister, valueEntityPersister);
	}
}

