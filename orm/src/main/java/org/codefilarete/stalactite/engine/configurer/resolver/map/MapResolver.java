package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Map;

import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.CreatedPersisterCollector;
import org.codefilarete.stalactite.engine.configurer.resolver.MapCreatedPersisterCollector;
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
	void resolve(ResolvedMapRelation<SRC, SRCID, K, KID, V, VID, M, LEFTTABLE, MAPTABLE, KTABLE, VTABLE> resolvedRelation,
	                                                       EntityWriteExecutor<SRC, SRCID> sourcePersister,
	                                                       MapCreatedPersisterCollector<SRCID, K, KID, V, VID, MAPTABLE, Object, Object> mapCreatedPersisterCollector) {
		
		EntityReadWriteExecutor<K, KID> keyEntityPersister = null;
		if (resolvedRelation.getKeyEntityDefinition() != null) {
			mapCreatedPersisterCollector.setKeyPersisterCollector(new CreatedPersisterCollector<>());
			keyEntityPersister = skeletonAggregateResolver.buildPersister(resolvedRelation.getKeyEntityDefinition().getEntity(), mapCreatedPersisterCollector.getKeyPersisterCollector());
		}
		
		EntityReadWriteExecutor<V, VID> valueEntityPersister = null;
		if (resolvedRelation.getValueEntityDefinition() != null) {
			mapCreatedPersisterCollector.setValuePersisterCollector(new CreatedPersisterCollector<>());
			valueEntityPersister = skeletonAggregateResolver.buildPersister(resolvedRelation.getValueEntityDefinition().getEntity(), mapCreatedPersisterCollector.getValuePersisterCollector());
		}
		
		EntryMapResolver keyEntityMapResolver = new EntryMapResolver(dialect, connectionConfiguration);
		KeyValueRecordPersister<Object, Object, SRCID, MAPTABLE> result = keyEntityMapResolver.resolve(resolvedRelation, sourcePersister, keyEntityPersister, valueEntityPersister);
		mapCreatedPersisterCollector.setKeyValueRecordPersister(result);
	}
}

