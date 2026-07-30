package org.codefilarete.stalactite.engine.configurer.map;

import java.util.Objects;

import org.codefilarete.stalactite.engine.diff.CollectionDiffer;

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
	
	/**
	 * Identifier for {@link CollectionDiffer} support (update use case), because it compares beans
	 * through their "footprint" which is their id in default/entity case, but since we are value type, we must provide a dedicated footprint.
	 * Could be hashCode() if it was implemented on identifier + element, but implementing it would require implementing equals() (to comply
	 * with best practices) which is not our case nor required by {@link CollectionDiffer}.
	 * Note : name of this method is not important
	 */
//	public int footprint() {
//		int result = id.hashCode();
//		result = 31 * result + key.hashCode();
//		return result;
//	}
}
