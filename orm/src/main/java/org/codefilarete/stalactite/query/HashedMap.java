package org.codefilarete.stalactite.query;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.codefilarete.tool.collection.PairIterator.EmptyIterator;

/**
 * An abstract implementation of a hash-based map which provides numerous points for
 * subclasses to override.
 * <p>
 * This class implements all the features necessary for a subclass hash-based map.
 * Key-value entries are stored in instances of the {@code HashEntry} class,
 * which can be overridden and replaced. The iterators can similarly be replaced,
 * without the need to replace the KeySet, EntrySet and Values view classes.
 * <p>
 * Overridable methods are provided to change the default hashing behavior, and
 * to change how entries are added to and removed from the map. Hopefully, all you
 * need for unusual subclasses is here.
 * <p>
 * NOTE: From Commons Collections 3.1 this class extends AbstractMap.
 * This is to provide backwards compatibility for ReferenceMap between v3.0 and v3.1.
 * This extends clause will be removed in v5.0.
 *
 * @param <K> the type of the keys in this map
 * @param <V> the type of the values in this map
 * @since 3.0
 */
public class HashedMap<K, V> extends AbstractMap<K, V> {
	
	protected static final String NO_NEXT_ENTRY = "No next() entry in the iteration";
	protected static final String REMOVE_INVALID = "remove() can only be called once after next()";
	
	/** The default capacity to use */
	protected static final int DEFAULT_CAPACITY = 16;
	/** The default threshold to use */
	protected static final int DEFAULT_THRESHOLD = 12;
	/** The default load factor to use */
	protected static final float DEFAULT_LOAD_FACTOR = 0.75f;
	/** The maximum capacity allowed */
	protected static final int MAXIMUM_CAPACITY = 1 << 30;
	/** An object for masking null */
	protected static final Object NULL = new Object();
	
	/** Load factor, normally 0.75 */
	protected float loadFactor;
	/** The size of the map */
	protected int size;
	/** Map entries */
	protected HashEntry<K, V>[] data;
	/** Size at which to rehash */
	protected int threshold;
	/** Modification count for iterators */
	protected int modCount;
	/** Entry set */
	protected EntrySet<K, V> entrySet;
	/** Key set */
	protected KeySet<K> keySet;
	/** Values */
	protected Values<V> values;
	
	/**
	 * Constructs a new empty map with default size and load factor.
	 */
	public HashedMap() {
		this(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_THRESHOLD);
	}
	
	/**
	 * Constructor which performs no validation on the passed in parameters.
	 *
	 * @param initialCapacity  the initial capacity, must be a power of two
	 * @param loadFactor  the load factor, must be &gt; 0.0f and generally &lt; 1.0f
	 * @param threshold  the threshold, must be sensible
	 */
	@SuppressWarnings("unchecked")
	protected HashedMap(int initialCapacity, float loadFactor, int threshold) {
		this.loadFactor = loadFactor;
		this.data = new HashEntry[initialCapacity];
		this.threshold = threshold;
	}
	
	/**
	 * Constructs a new, empty map with the specified initial capacity and
	 * default load factor.
	 *
	 * @param initialCapacity  the initial capacity
	 * @throws IllegalArgumentException if the initial capacity is negative
	 */
	protected HashedMap(int initialCapacity) {
		this(initialCapacity, DEFAULT_LOAD_FACTOR);
	}
	
	/**
	 * Constructs a new, empty map with the specified initial capacity and
	 * load factor.
	 *
	 * @param initialCapacity  the initial capacity
	 * @param loadFactor  the load factor
	 * @throws IllegalArgumentException if the initial capacity is negative
	 * @throws IllegalArgumentException if the load factor is less than or equal to zero
	 */
	@SuppressWarnings("unchecked")
	protected HashedMap(int initialCapacity, float loadFactor) {
		if (initialCapacity < 0) {
			throw new IllegalArgumentException("Initial capacity must be a non negative number");
		}
		if (loadFactor <= 0.0f || Float.isNaN(loadFactor)) {
			throw new IllegalArgumentException("Load factor must be greater than 0");
		}
		this.loadFactor = loadFactor;
		initialCapacity = calculateNewCapacity(initialCapacity);
		this.threshold = calculateThreshold(initialCapacity, loadFactor);
		this.data = new HashEntry[initialCapacity];
	}
	
	/**
	 * Constructor copying elements from another map.
	 *
	 * @param map  the map to copy
	 * @throws NullPointerException if the map is null
	 */
	protected HashedMap(Map<? extends K, ? extends V> map) {
		this(Math.max(2 * map.size(), DEFAULT_CAPACITY), DEFAULT_LOAD_FACTOR);
		_putAll(map);
	}
	
	/**
	 * Gets the value mapped to the key specified.
	 *
	 * @param key  the key
	 * @return the mapped value, null if no match
	 */
	@Override
	public V get(Object key) {
		int hashCode = hash(key);
		HashEntry<K, V> entry = data[hashIndex(hashCode, data.length)]; // no local for hash index
		while (entry != null) {
			if (entry.hashCode == hashCode && isEqualKey(key, entry.key)) {
				return entry.getValue();
			}
			entry = entry.next;
		}
		return null;
	}
	
	/**
	 * Gets the size of the map.
	 *
	 * @return the size
	 */
	@Override
	public int size() {
		return size;
	}
	
	/**
	 * Checks whether the map is currently empty.
	 *
	 * @return true if the map is currently size zero
	 */
	@Override
	public boolean isEmpty() {
		return size == 0;
	}
	
	/**
	 * Checks whether the map contains the specified key.
	 *
	 * @param key  the key to search for
	 * @return true if the map contains the key
	 */
	@Override
	public boolean containsKey(Object key) {
		int hashCode = hash(key);
		HashEntry<K, V> entry = data[hashIndex(hashCode, data.length)]; // no local for hash index
		while (entry != null) {
			if (entry.hashCode == hashCode && isEqualKey(key, entry.key)) {
				return true;
			}
			entry = entry.next;
		}
		return false;
	}
	
	/**
	 * Checks whether the map contains the specified value.
	 *
	 * @param value  the value to search for
	 * @return true if the map contains the value
	 */
	@Override
	public boolean containsValue(Object value) {
		if (value == null) {
			for (HashEntry<K, V> element : data) {
				HashEntry<K, V> entry = element;
				while (entry != null) {
					if (entry.getValue() == null) {
						return true;
					}
					entry = entry.next;
				}
			}
		} else {
			for (HashEntry<K, V> element : data) {
				HashEntry<K, V> entry = element;
				while (entry != null) {
					if (isEqualValue(value, entry.getValue())) {
						return true;
					}
					entry = entry.next;
				}
			}
		}
		return false;
	}
	
	/**
	 * Puts a key-value mapping into this map.
	 *
	 * @param key  the key to add
	 * @param value  the value to add
	 * @return the value previously mapped to this key, null if none
	 */
	@Override
	public V put(K key, V value) {
		int hashCode = hash(key);
		int index = hashIndex(hashCode, data.length);
		HashEntry<K, V> entry = data[index];
		while (entry != null) {
			if (entry.hashCode == hashCode && isEqualKey(key, entry.key)) {
				V oldValue = entry.getValue();
				updateEntry(entry, value);
				return oldValue;
			}
			entry = entry.next;
		}
		
		addMapping(index, hashCode, key, value);
		return null;
	}
	
	/**
	 * Puts all the values from the specified map into this map.
	 * <p>
	 * This implementation iterates around the specified map and
	 * uses {@link #put(Object, Object)}.
	 *
	 * @param map  the map to add
	 * @throws NullPointerException if the map is null
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		_putAll(map);
	}
	
	/**
	 * Puts all the values from the specified map into this map.
	 * <p>
	 * This implementation iterates around the specified map and
	 * uses {@link #put(Object, Object)}.
	 * <p>
	 * It is private to allow the constructor to still call it
	 * even when putAll is overridden.
	 *
	 * @param map  the map to add
	 * @throws NullPointerException if the map is null
	 */
	private void _putAll(Map<? extends K, ? extends V> map) {
		int mapSize = map.size();
		if (mapSize == 0) {
			return;
		}
		int newSize = (int) ((size + mapSize) / loadFactor + 1);
		ensureCapacity(calculateNewCapacity(newSize));
		for (Map.Entry<? extends K, ? extends V> entry: map.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}
	
	/**
	 * Removes the specified mapping from this map.
	 *
	 * @param key  the mapping to remove
	 * @return the value mapped to the removed key, null if key not in map
	 */
	@Override
	public V remove(Object key) {
		int hashCode = hash(key);
		int index = hashIndex(hashCode, data.length);
		HashEntry<K, V> entry = data[index];
		HashEntry<K, V> previous = null;
		while (entry != null) {
			if (entry.hashCode == hashCode && isEqualKey(key, entry.key)) {
				V oldValue = entry.getValue();
				removeMapping(entry, index, previous);
				return oldValue;
			}
			previous = entry;
			entry = entry.next;
		}
		return null;
	}
	
	/**
	 * Clears the map, resetting the size to zero and nullifying references
	 * to avoid garbage collection issues.
	 */
	@Override
	public void clear() {
		modCount++;
		HashEntry<K, V>[] data = this.data;
		Arrays.fill(data, null);
		size = 0;
	}
	
	/**
	 * Gets the hash code for the key specified.
	 * This implementation uses the additional hashing routine from JDK1.4.
	 * Subclasses can override this to return alternate hash codes.
	 *
	 * @param key  the key to get a hash code for
	 * @return the hash code
	 */
	protected int hash(Object key) {
		int h;
		return key == null ? 0 : (h = key.hashCode()) ^ (h >>> 16);
	}
	
	/**
	 * Compares two keys, in internal converted form, to see if they are equal.
	 * This implementation uses the equals method and assumes neither key is null.
	 * Subclasses can override this to match differently.
	 *
	 * @param key1  the first key to compare passed in from outside
	 * @param key2  the second key extracted from the entry via {@code entry.key}
	 * @return true if equal
	 */
	protected boolean isEqualKey(Object key1, Object key2) {
		return key1 == key2 || key1.equals(key2);
	}
	
	/**
	 * Compares two values, in external form, to see if they are equal.
	 * This implementation uses the equals method and assumes neither value is null.
	 * Subclasses can override this to match differently.
	 *
	 * @param value1  the first value to compare passed in from outside
	 * @param value2  the second value extracted from the entry via {@code getValue()}
	 * @return true if equal
	 */
	protected boolean isEqualValue(Object value1, Object value2) {
		return value1 == value2 || value1.equals(value2);
	}
	
	/**
	 * Gets the index into the data storage for the hashCode specified.
	 * This implementation uses the least significant bits of the hashCode.
	 * Subclasses can override this to return alternate bucketing.
	 *
	 * @param hashCode  the hash code to use
	 * @param dataSize  the size of the data to pick a bucket from
	 * @return the bucket index
	 */
	protected int hashIndex(int hashCode, int dataSize) {
		return hashCode & dataSize - 1;
	}
	
	/**
	 * Gets the entry mapped to the key specified.
	 * <p>
	 * This method exists for subclasses that may need to perform a multi-step
	 * process accessing the entry. The public methods in this class don't use this
	 * method to gain a small performance boost.
	 *
	 * @param key  the key
	 * @return the entry, null if no match
	 */
	protected HashEntry<K, V> getEntry(Object key) {
		int hashCode = hash(key);
		HashEntry<K, V> entry = data[hashIndex(hashCode, data.length)]; // no local for hash index
		while (entry != null) {
			if (entry.hashCode == hashCode && isEqualKey(key, entry.key)) {
				return entry;
			}
			entry = entry.next;
		}
		return null;
	}
	
	/**
	 * Updates an existing key-value mapping to change the value.
	 * <p>
	 * This implementation calls {@code setValue()} on the entry.
	 * Subclasses could override to handle changes to the map.
	 *
	 * @param entry  the entry to update
	 * @param newValue  the new value to store
	 */
	protected void updateEntry(HashEntry<K, V> entry, V newValue) {
		entry.setValue(newValue);
	}
	
	/**
	 * Adds a new key-value mapping into this map.
	 * <p>
	 * This implementation calls {@code createEntry()}, {@code addEntry()}
	 * and {@code checkCapacity()}.
	 * It also handles changes to {@code modCount} and {@code size}.
	 * Subclasses could override to fully control adds to the map.
	 *
	 * @param hashIndex  the index into the data array to store at
	 * @param hashCode  the hash code of the key to add
	 * @param key  the key to add
	 * @param value  the value to add
	 */
	protected void addMapping(int hashIndex, int hashCode, K key, V value) {
		modCount++;
		HashEntry<K, V> entry = createEntry(data[hashIndex], hashCode, key, value);
		addEntry(entry, hashIndex);
		size++;
		checkCapacity();
	}
	
	/**
	 * Creates an entry to store the key-value data.
	 * <p>
	 * This implementation creates a new HashEntry instance.
	 * Subclasses can override this to return a different storage class,
	 * or implement caching.
	 *
	 * @param next  the next entry in sequence
	 * @param hashCode  the hash code to use
	 * @param key  the key to store
	 * @param value  the value to store
	 * @return the newly created entry
	 */
	protected HashEntry<K, V> createEntry(HashEntry<K, V> next, int hashCode, K key, V value) {
		return new HashEntry<>(next, hashCode, key, value);
	}
	
	/**
	 * Adds an entry into this map.
	 * <p>
	 * This implementation adds the entry to the data storage table.
	 * Subclasses could override to handle changes to the map.
	 *
	 * @param entry  the entry to add
	 * @param hashIndex  the index into the data array to store at
	 */
	protected void addEntry(HashEntry<K, V> entry, int hashIndex) {
		data[hashIndex] = entry;
	}
	
	/**
	 * Removes a mapping from the map.
	 * <p>
	 * This implementation calls {@code removeEntry()} and {@code destroyEntry()}.
	 * It also handles changes to {@code modCount} and {@code size}.
	 * Subclasses could override to fully control removals from the map.
	 *
	 * @param entry  the entry to remove
	 * @param hashIndex  the index into the data structure
	 * @param previous  the previous entry in the chain
	 */
	protected void removeMapping(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
		modCount++;
		removeEntry(entry, hashIndex, previous);
		size--;
		destroyEntry(entry);
	}
	
	/**
	 * Removes an entry from the chain stored in a particular index.
	 * <p>
	 * This implementation removes the entry from the data storage table.
	 * The size is not updated.
	 * Subclasses could override to handle changes to the map.
	 *
	 * @param entry  the entry to remove
	 * @param hashIndex  the index into the data structure
	 * @param previous  the previous entry in the chain
	 */
	protected void removeEntry(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
		if (previous == null) {
			data[hashIndex] = entry.next;
		} else {
			previous.next = entry.next;
		}
	}
	
	/**
	 * Kills an entry ready for the garbage collector.
	 * <p>
	 * This implementation prepares the HashEntry for garbage collection.
	 * Subclasses can override this to implement caching (override clear as well).
	 *
	 * @param entry  the entry to destroy
	 */
	protected void destroyEntry(HashEntry<K, V> entry) {
		entry.next = null;
		entry.key = null;
		entry.value = null;
	}
	
	/**
	 * Checks the capacity of the map and enlarges it if necessary.
	 * <p>
	 * This implementation uses the threshold to check if the map needs enlarging
	 */
	protected void checkCapacity() {
		if (size >= threshold) {
			int newCapacity = data.length * 2;
			if (newCapacity <= MAXIMUM_CAPACITY) {
				ensureCapacity(newCapacity);
			}
		}
	}
	
	/**
	 * Changes the size of the data structure to the capacity proposed.
	 *
	 * @param newCapacity  the new capacity of the array (a power of two, less or equal to max)
	 */
	@SuppressWarnings("unchecked")
	protected void ensureCapacity(int newCapacity) {
		int oldCapacity = data.length;
		if (newCapacity <= oldCapacity) {
			return;
		}
		if (size == 0) {
			threshold = calculateThreshold(newCapacity, loadFactor);
			data = new HashEntry[newCapacity];
		} else {
			HashEntry<K, V>[] oldEntries = data;
			HashEntry<K, V>[] newEntries = new HashEntry[newCapacity];
			
			modCount++;
			for (int i = oldCapacity - 1; i >= 0; i--) {
				HashEntry<K, V> entry = oldEntries[i];
				if (entry != null) {
					oldEntries[i] = null;  // gc
					do {
						HashEntry<K, V> next = entry.next;
						int index = hashIndex(entry.hashCode, newCapacity);
						entry.next = newEntries[index];
						newEntries[index] = entry;
						entry = next;
					} while (entry != null);
				}
			}
			threshold = calculateThreshold(newCapacity, loadFactor);
			data = newEntries;
		}
	}
	
	/**
	 * Calculates the new capacity of the map.
	 * This implementation normalizes the capacity to a power of two.
	 *
	 * @param proposedCapacity  the proposed capacity
	 * @return the normalized new capacity
	 */
	protected int calculateNewCapacity(int proposedCapacity) {
		int newCapacity = 1;
		if (proposedCapacity > MAXIMUM_CAPACITY) {
			newCapacity = MAXIMUM_CAPACITY;
		} else {
			while (newCapacity < proposedCapacity) {
				newCapacity <<= 1;  // multiply by two
			}
			if (newCapacity > MAXIMUM_CAPACITY) {
				newCapacity = MAXIMUM_CAPACITY;
			}
		}
		return newCapacity;
	}
	
	/**
	 * Calculates the new threshold of the map, where it will be resized.
	 * This implementation uses the load factor.
	 *
	 * @param newCapacity  the new capacity
	 * @param factor  the load factor
	 * @return the new resize threshold
	 */
	protected int calculateThreshold(int newCapacity, float factor) {
		return (int) (newCapacity * factor);
	}
	
	/**
	 * Gets the entrySet view of the map.
	 * Changes made to the view affect this map.
	 *
	 * @return the entrySet view
	 */
	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		if (entrySet == null) {
			entrySet = new EntrySet<>(this);
		}
		return entrySet;
	}
	
	/**
	 * Creates an entry set iterator.
	 * Subclasses can override this to return iterators with different properties.
	 *
	 * @return the entrySet iterator
	 */
	protected Iterator<Map.Entry<K, V>> createEntrySetIterator() {
		if (isEmpty()) {
			return new EmptyIterator<>();
		}
		return new EntrySetIterator<>(this);
	}
	
	/**
	 * EntrySet implementation.
	 *
	 * @param <K> the type of the keys in the map
	 * @param <V> the type of the values in the map
	 */
	protected static class EntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {
		/** The parent map */
		private final HashedMap<K, V> parent;
		
		protected EntrySet(HashedMap<K, V> parent) {
			this.parent = parent;
		}
		
		@Override
		public int size() {
			return parent.size();
		}
		
		@Override
		public void clear() {
			parent.clear();
		}
		
		@Override
		public boolean contains(Object entry) {
			if (entry instanceof Map.Entry) {
				Map.Entry<?, ?> e = (Map.Entry<?, ?>) entry;
				Entry<K, V> match = parent.getEntry(e.getKey());
				return match != null && match.equals(e);
			}
			return false;
		}
		
		@Override
		public boolean remove(Object obj) {
			if (!(obj instanceof Map.Entry)) {
				return false;
			}
			if (!contains(obj)) {
				return false;
			}
			Map.Entry<?, ?> entry = (Map.Entry<?, ?>) obj;
			parent.remove(entry.getKey());
			return true;
		}
		
		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return parent.createEntrySetIterator();
		}
	}
	
	/**
	 * EntrySet iterator.
	 *
	 * @param <K> the type of the keys in the map
	 * @param <V> the type of the values in the map
	 */
	protected static class EntrySetIterator<K, V> extends HashIterator<K, V> implements Iterator<Map.Entry<K, V>> {
		
		protected EntrySetIterator(HashedMap<K, V> parent) {
			super(parent);
		}
		
		@Override
		public Map.Entry<K, V> next() {
			return super.nextEntry();
		}
	}
	
	/**
	 * Gets the keySet view of the map.
	 * Changes made to the view affect this map.
	 *
	 * @return the keySet view
	 */
	@Override
	public Set<K> keySet() {
		if (keySet == null) {
			keySet = new KeySet<>(this);
		}
		return keySet;
	}
	
	/**
	 * Creates a key set iterator.
	 * Subclasses can override this to return iterators with different properties.
	 *
	 * @return the keySet iterator
	 */
	protected Iterator<K> createKeySetIterator() {
		if (isEmpty()) {
			return new EmptyIterator<>();
		}
		return new KeySetIterator<>(this);
	}
	
	/**
	 * KeySet implementation.
	 *
	 * @param <K> the type of elements maintained by this set
	 */
	protected static class KeySet<K> extends AbstractSet<K> {
		/** The parent map */
		private final HashedMap<K, ?> parent;
		
		protected KeySet(HashedMap<K, ?> parent) {
			this.parent = parent;
		}
		
		@Override
		public int size() {
			return parent.size();
		}
		
		@Override
		public void clear() {
			parent.clear();
		}
		
		@Override
		public boolean contains(Object key) {
			return parent.containsKey(key);
		}
		
		@Override
		public boolean remove(Object key) {
			boolean result = parent.containsKey(key);
			parent.remove(key);
			return result;
		}
		
		@Override
		public Iterator<K> iterator() {
			return parent.createKeySetIterator();
		}
	}
	
	/**
	 * KeySet iterator.
	 *
	 * @param <K> the type of elements maintained by this set
	 */
	protected static class KeySetIterator<K> extends HashIterator<K, Object> implements Iterator<K> {
		
		@SuppressWarnings("unchecked")
		protected KeySetIterator(HashedMap<K, ?> parent) {
			super((HashedMap<K, Object>) parent);
		}
		
		@Override
		public K next() {
			return super.nextEntry().getKey();
		}
	}
	
	/**
	 * Gets the values view of the map.
	 * Changes made to the view affect this map.
	 *
	 * @return the values view
	 */
	@Override
	public Collection<V> values() {
		if (values == null) {
			values = new Values<>(this);
		}
		return values;
	}
	
	/**
	 * Creates a values iterator.
	 * Subclasses can override this to return iterators with different properties.
	 *
	 * @return the values iterator
	 */
	protected Iterator<V> createValuesIterator() {
		if (isEmpty()) {
			return new EmptyIterator<>();
		}
		return new ValuesIterator<>(this);
	}
	
	/**
	 * Values implementation.
	 *
	 * @param <V> the type of elements maintained by this collection
	 */
	protected static class Values<V> extends AbstractCollection<V> {
		/** The parent map */
		private final HashedMap<?, V> parent;
		
		protected Values(HashedMap<?, V> parent) {
			this.parent = parent;
		}
		
		@Override
		public int size() {
			return parent.size();
		}
		
		@Override
		public void clear() {
			parent.clear();
		}
		
		@Override
		public boolean contains(Object value) {
			return parent.containsValue(value);
		}
		
		@Override
		public Iterator<V> iterator() {
			return parent.createValuesIterator();
		}
	}
	
	/**
	 * Values iterator.
	 *
	 * @param <V> the type of elements maintained by this collection
	 */
	protected static class ValuesIterator<V> extends HashIterator<Object, V> implements Iterator<V> {
		
		@SuppressWarnings("unchecked")
		protected ValuesIterator(HashedMap<?, V> parent) {
			super((HashedMap<Object, V>) parent);
		}
		
		@Override
		public V next() {
			return super.nextEntry().getValue();
		}
	}
	
	/**
	 * HashEntry used to store the data.
	 * <p>
	 * If you subclass {@code AbstractHashedMap} but not {@code HashEntry}
	 * then you will not be able to access the protected fields.
	 * The {@code entryXxx()} methods on {@code AbstractHashedMap} exist
	 * to provide the necessary access.
	 *
	 * @param <K> the type of the keys
	 * @param <V> the type of the values
	 */
	protected static class HashEntry<K, V> implements Map.Entry<K, V> {
		/** The next entry in the hash chain */
		protected HashEntry<K, V> next;
		/** The hash code of the key */
		protected int hashCode;
		/** The key */
		protected Object key;
		/** The value */
		protected Object value;
		
		protected HashEntry(HashEntry<K, V> next, int hashCode, Object key, V value) {
			this.next = next;
			this.hashCode = hashCode;
			this.key = key;
			this.value = value;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public K getKey() {
			if (key == NULL) {
				return null;
			}
			return (K) key;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public V getValue() {
			return (V) value;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public V setValue(V value) {
			Object old = this.value;
			this.value = value;
			return (V) old;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (!(obj instanceof Map.Entry)) {
				return false;
			}
			Map.Entry<?, ?> other = (Map.Entry<?, ?>) obj;
			return
					(getKey() == null ? other.getKey() == null : getKey().equals(other.getKey())) &&
							(getValue() == null ? other.getValue() == null : getValue().equals(other.getValue()));
		}
		
		@Override
		public int hashCode() {
			return (getKey() == null ? 0 : getKey().hashCode()) ^
					(getValue() == null ? 0 : getValue().hashCode());
		}
		
		@Override
		public String toString() {
			return String.valueOf(getKey()) + '=' + getValue();
		}
	}
	
	/**
	 * Base Iterator
	 *
	 * @param <K> the type of the keys in the map
	 * @param <V> the type of the values in the map
	 */
	protected abstract static class HashIterator<K, V> {
		
		/** The parent map */
		private final HashedMap<K, V> parent;
		/** The current index into the array of buckets */
		private int hashIndex;
		/** The last returned entry */
		private HashEntry<K, V> last;
		/** The next entry */
		private HashEntry<K, V> next;
		/** The modification count expected */
		private int expectedModCount;
		
		protected HashIterator(HashedMap<K, V> parent) {
			this.parent = parent;
			HashEntry<K, V>[] data = parent.data;
			int i = data.length;
			HashEntry<K, V> next = null;
			while (i > 0 && next == null) {
				next = data[--i];
			}
			this.next = next;
			this.hashIndex = i;
			this.expectedModCount = parent.modCount;
		}
		
		public boolean hasNext() {
			return next != null;
		}
		
		protected HashEntry<K, V> nextEntry() {
			if (parent.modCount != expectedModCount) {
				throw new ConcurrentModificationException();
			}
			HashEntry<K, V> newCurrent = next;
			if (newCurrent == null)  {
				throw new NoSuchElementException(HashedMap.NO_NEXT_ENTRY);
			}
			HashEntry<K, V>[] data = parent.data;
			int i = hashIndex;
			HashEntry<K, V> n = newCurrent.next;
			while (n == null && i > 0) {
				n = data[--i];
			}
			next = n;
			hashIndex = i;
			last = newCurrent;
			return newCurrent;
		}
		
		protected HashEntry<K, V> currentEntry() {
			return last;
		}
		
		public void remove() {
			if (last == null) {
				throw new IllegalStateException(HashedMap.REMOVE_INVALID);
			}
			if (parent.modCount != expectedModCount) {
				throw new ConcurrentModificationException();
			}
			parent.remove(last.getKey());
			last = null;
			expectedModCount = parent.modCount;
		}
		
		@Override
		public String toString() {
			if (last != null) {
				return "Iterator[" + last.getKey() + "=" + last.getValue() + "]";
			}
			return "Iterator[]";
		}
	}
	
	/**
	 * Clones the map without cloning the keys or values.
	 * <p>
	 * To implement {@code clone()}, a subclass must implement the
	 * {@code Cloneable} interface and make this method public.
	 *
	 * @return a shallow clone
	 * @throws InternalError if {@link AbstractMap#clone()} failed
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected HashedMap<K, V> clone() {
		try {
			HashedMap<K, V> cloned = (HashedMap<K, V>) super.clone();
			cloned.data = new HashEntry[data.length];
			cloned.entrySet = null;
			cloned.keySet = null;
			cloned.values = null;
			cloned.modCount = 0;
			cloned.size = 0;
			cloned.putAll(this);
			return cloned;
		} catch (CloneNotSupportedException ex) {
			throw new UnsupportedOperationException(ex);
		}
	}
	
	/**
	 * Compares this map with another.
	 *
	 * @param obj  the object to compare to
	 * @return true if equal
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof Map)) {
			return false;
		}
		Map<?, ?> map = (Map<?, ?>) obj;
		if (map.size() != size()) {
			return false;
		}
		Iterator<Map.Entry<K, V>> it = entrySet().iterator();
		try {
			while (it.hasNext()) {
				Map.Entry<K, V> entry = it.next();
				Object key = entry.getKey();
				Object value = entry.getValue();
				if (value == null) {
					if (map.get(key) != null || !map.containsKey(key)) {
						return false;
					}
				} else {
					if (!value.equals(map.get(key))) {
						return false;
					}
				}
			}
		} catch (ClassCastException | NullPointerException ignored) {
			return false;
		}
		return true;
	}
	
	/**
	 * Gets the standard Map hashCode.
	 *
	 * @return the hash code defined in the Map interface
	 */
	@Override
	public int hashCode() {
		int total = 0;
		Iterator<Map.Entry<K, V>> it = createEntrySetIterator();
		while (it.hasNext()) {
			total += it.next().hashCode();
		}
		return total;
	}
	
	/**
	 * Gets the map as a String.
	 *
	 * @return a string version of the map
	 */
	@Override
	public String toString() {
		if (isEmpty()) {
			return "{}";
		}
		StringBuilder buf = new StringBuilder(32 * size());
		buf.append('{');
		
		Iterator<Map.Entry<K, V>> it = entrySet().iterator();
		boolean hasNext = it.hasNext();
		while (hasNext) {
			Entry<K, V> entry = it.next();
			K key = entry.getKey();
			V value = entry.getValue();
			buf.append(key == this ? "(this Map)" : key)
					.append('=')
					.append(value == this ? "(this Map)" : value);
			
			hasNext = it.hasNext();
			if (hasNext) {
				buf.append(',').append(' ');
			}
		}
		
		buf.append('}');
		return buf.toString();
	}
}