package org.codefilarete.stalactite.engine.configurer.map;

import java.util.Objects;

/**
 * Identifier of a {@link KeyValueRecord} table : since the relation is a kind of one-to-many, with entry key as
 * additional identifier, this class is composed of those elements :
 * - key element (Map.Entry.key)
 * - identifier of source entity
 * 
 * @param <K>
 * @param <SRCID>
 * @author Guillaume Mary
 */
public class RecordId<K, SRCID> {
	
	private SRCID srcId;
	private K key;
	
	/**
	 * Default constructor for select instantiation
	 */
	public RecordId() {
	}
	
	public RecordId(SRCID srcId, K key) {
		setId(srcId);
		this.key = key;
	}
	
	public SRCID getId() {
		return srcId;
	}
	
	public void setId(SRCID srcId) {
		this.srcId = srcId;
	}
	
	public K getKey() {
		return key;
	}
	
	public void setKey(K key) {
		this.key = key;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RecordId<?, ?> recordId = (RecordId<?, ?>) o;
		return Objects.equals(srcId, recordId.srcId) && Objects.equals(key, recordId.key);
	}
	
	@Override
	public int hashCode() {
		int result = srcId.hashCode();
		result = 31 * result + key.hashCode();
		return result;
	}
}
