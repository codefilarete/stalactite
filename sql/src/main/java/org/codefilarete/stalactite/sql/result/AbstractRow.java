package org.codefilarete.stalactite.sql.result;

import java.util.Map;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractRow<KEY> {

	private final Map<KEY, Object> content;

	protected AbstractRow(Map<KEY, Object> content) {
		this.content = content;
	}

	public Map<KEY, Object> getContent() {
		return content;
	}

	/**
	 * Put a key-value pair to this instance
	 * @param key the key of the value
	 * @param object the value
	 */
	public void put(KEY key, Object object) {
		content.put(key, object);
	}

	/**
	 * Fluent API equivalent to {@link #put(Object, Object)}
	 * @param key the key of the value
	 * @param object the value
	 * @return this
	 */
	public AbstractRow<KEY> add(KEY key, Object object) {
		put(key, object);
		return this;
	}

	/**
	 * 
	 * @param key the key to return value from
	 * @return the key data
	 */
	public Object get(KEY key) {
		return content.get(key);
	}
}
