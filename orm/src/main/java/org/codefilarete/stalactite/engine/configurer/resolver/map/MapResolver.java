package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Map;

import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.CreatedPersisterCollector;
import org.codefilarete.stalactite.engine.configurer.resolver.EntitySkeletonResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.MapCreatedPersisterCollector;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class MapResolver {
	
	private final EntitySkeletonResolver entitySkeletonResolver;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public MapResolver(EntitySkeletonResolver entitySkeletonResolver, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.entitySkeletonResolver = entitySkeletonResolver;
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
			keyEntityPersister = entitySkeletonResolver.resolve(
					resolvedRelation.getKeyEntityDefinition().getEntity(),
					mapCreatedPersisterCollector.getKeyPersisterCollector());
		}
		
		EntityReadWriteExecutor<V, VID> valueEntityPersister = null;
		if (resolvedRelation.getValueEntityDefinition() != null) {
			mapCreatedPersisterCollector.setValuePersisterCollector(new CreatedPersisterCollector<>());
			valueEntityPersister = entitySkeletonResolver.resolve(
					resolvedRelation.getValueEntityDefinition().getEntity(),
					mapCreatedPersisterCollector.getValuePersisterCollector());
		}
		
		EntryMapResolver entryMapResolver = new EntryMapResolver(dialect, connectionConfiguration);
		KeyValueRecordPersister<Object, Object, SRCID, MAPTABLE> result = entryMapResolver.resolve(resolvedRelation, sourcePersister, keyEntityPersister, valueEntityPersister);
		mapCreatedPersisterCollector.setKeyValueRecordPersister(result);
	}
}

