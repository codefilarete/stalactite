package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class MapCreatedPersisterCollector<SRCID, K, KID, V, VID,
		MAPTABLE extends Table<MAPTABLE>,
		X, Y> {
	
	private KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> keyValueRecordPersister;
	
	private CreatedPersisterCollector<K, KID> keyPersisterCollector;
	
	private CreatedPersisterCollector<V, VID> valuePersisterCollector;
	
	public KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> getKeyValueRecordPersister() {
		return keyValueRecordPersister;
	}
	
	public void setKeyValueRecordPersister(KeyValueRecordPersister<X, Y, SRCID, MAPTABLE> keyValueRecordPersister) {
		this.keyValueRecordPersister = keyValueRecordPersister;
	}
	
	public CreatedPersisterCollector<K, KID> getKeyPersisterCollector() {
		return keyPersisterCollector;
	}
	
	public void setKeyPersisterCollector(CreatedPersisterCollector<K, KID> keyPersisterCollector) {
		this.keyPersisterCollector = keyPersisterCollector;
	}
	
	public CreatedPersisterCollector<V, VID> getValuePersisterCollector() {
		return valuePersisterCollector;
	}
	
	public void setValuePersisterCollector(CreatedPersisterCollector<V, VID> valuePersisterCollector) {
		this.valuePersisterCollector = valuePersisterCollector;
	}
}
