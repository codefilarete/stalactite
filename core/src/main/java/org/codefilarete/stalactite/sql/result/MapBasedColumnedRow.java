package org.codefilarete.stalactite.sql.result;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Selectable.SimpleSelectable;

/**
 * Default implementation of a {@link ColumnedRow} that stores {@link java.sql.ResultSet} data in memory through a {@link Map}.
 * This class is not expected to be exposed to end users, the {@link ColumnedRow} interface must be exposed instead.
 * 
 * @author Guillaume Mary
 */
public class MapBasedColumnedRow extends AbstractRow<Selectable<?>> implements ColumnedRow {
	
	public MapBasedColumnedRow() {
		this(new HashMap<>());
	}
	
	protected MapBasedColumnedRow(Map<Selectable<?>, Object> content) {
		super(content);
	}
	
	/**
	 * Equivalent to {@link super#put(Object, Object)} with better typing
	 *
	 * @param key    the column to set a value for
	 * @param object the column value
	 * @param <E>    the column data type
	 */
	public <E> void put(Selectable<E> key, E object) {
		super.put(key, object);
	}
	
	/**
	 * Equivalent to {@link super#add(Object, Object)} with better typing
	 *
	 * @param key    the column to set a value for
	 * @param object the column value
	 * @param <E>    the column data type
	 */
	public <E> MapBasedColumnedRow add(Selectable<E> key, E object) {
		put(key, object);
		return this;
	}
	
	/**
	 * Facility method that adds given key as a {@link SimpleSelectable}
	 * @param key the alias of the column to add a value for
	 * @param object the column value
	 * @return this
	 * @param <E> data type
	 */
	public <E> MapBasedColumnedRow add(String key, E object) {
		return add(new SimpleSelectable<E>(key, (Class<E>) (object == null ? Object.class : object.getClass())), object);
	}
	
	/**
	 * Equivalent to {@link super#get(Object)} with better typing
	 * @param key the column to get a value from
	 * @return the column data
	 * @param <E> the column data type
	 */
	public <E> E get(Selectable<E> key) {
		return (E) super.get(key);
	}
}