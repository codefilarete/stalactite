package org.codefilarete.stalactite.engine.configurer.map;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.diff.CollectionDiffer;

/**
 * Represents a line in table storage. Acts as a wrapper of map entry (key and value) with source bean identifier addition
 * (to store foreign key to source entity)
 *
 * @param <K> Map entry key type
 * @param <V> Map entry value type
 * @param <ID> source bean identifier type
 */
class KeyValueRecord<K, V, ID> {
	
	static final PropertyAccessor<KeyValueRecord<Object, Object, Object>, Object> KEY_ACCESSOR = PropertyAccessor.fromMethodReference(
			KeyValueRecord::getKey,
			KeyValueRecord::setKey);
	
	static final PropertyAccessor<KeyValueRecord<Object, Object, Object>, Object> VALUE_ACCESSOR = PropertyAccessor.fromMethodReference(
			KeyValueRecord::getValue,
			KeyValueRecord::setValue);
	
	private ID id;
	private K key;
	private V value;
	private boolean persisted = false;
	
	/**
	 * Default constructor for select instantiation
	 */
	public KeyValueRecord() {
	}
	
	public KeyValueRecord(ID id, K key, V value) {
		setId(id);
		this.key = key;
		this.value = value;
	}
	
	public boolean isNew() {
		return !persisted;
	}
	
	public boolean isPersisted() {
		return persisted;
	}
	
	public void markAsPersisted() {
		this.persisted = true;
	}
	
	public KeyValueRecord<K, V, ID> setPersisted(boolean persisted) {
		this.persisted = persisted;
		return this;
	}
	
	public ID getId() {
		return id;
	}
	
	public void setId(ID id) {
		this.id = id;
		this.persisted = true;
	}
	
	public K getKey() {
		return key;
	}
	
	public void setKey(K key) {
		this.key = key;
	}
	
	public V getValue() {
		return value;
	}
	
	public void setValue(V value) {
		this.value = value;
	}
	
	/**
	 * Identifier for {@link CollectionDiffer} support (update use case), because it compares beans
	 * through their "footprint" which is their id in default/entity case, but since we are value type, we must provide a dedicated footprint.
	 * Could be hashCode() if it was implemented on identifier + element, but implementing it would require implementing equals() (to comply
	 * with best practices) which is not our case nor required by {@link CollectionDiffer}.
	 * Note : name of this method is not important
	 */
	public int footprint() {
		int result = id.hashCode();
		result = 31 * result + key.hashCode();
		result = 31 * result + value.hashCode();
		return result;
	}
}
