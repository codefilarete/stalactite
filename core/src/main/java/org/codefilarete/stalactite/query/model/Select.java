package org.codefilarete.stalactite.query.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * A support for the select part of a SQL query
 *
 * @author Guillaume Mary
 */
public class Select implements Iterable<Object /* String, Column or AliasedColumn */>, SelectChain<Select> {
	
	/** String, Column or {@link AliasedColumn} */
	private final List<Object> columns = new ArrayList<>(5);
	
	private boolean distinct = false;
	
	private Select add(Object column) {
		this.columns.add(column);
		return this;
	}
	
	@Override
	public Select add(Object selectable, Object... selectables) {
		add(selectable);
		for (Object col : selectables) {
			add(col);
		}
		return this;
	}
	
	@Override
	public Select add(Column column, String alias) {
		return add(new AliasedColumn(column, alias));
	}
	
	@Override
	public Select add(Map<Column, String> aliasedColumns) {
		for (Entry<Column, String> aliasedColumn : aliasedColumns.entrySet()) {
			add(new AliasedColumn(aliasedColumn.getKey(), aliasedColumn.getValue()));
		}
		return this;
	}
	
	/**
	 * Gives column aliases : works for {@link Column} declared through {@link #add(Column, String)} and {@link #add(Map)}.
	 * 
	 * @return {@link Column} aliases of this instance, an empty {@link Map} if no {@link Column} was added to this instance
	 */
	public Map<Column, String> giveColumnAliases() {
		Map<Column, String> aliases = new HashMap<>();
		for (Object column : columns) {
			if (column instanceof AliasedColumn) {
				aliases.put(((AliasedColumn) column).getColumn(), ((AliasedColumn) column).getAlias());
			}
		}
		return aliases;
	}
	
	public boolean isDistinct() {
		return distinct;
	}
	
	@Override
	public Select distinct() {
		this.distinct = true;
		return this;
	}
	
	public Object remove(int index) {
		return this.columns.remove(index);
	}
	
	public List<Object> clear() {
		List<Object> result = Iterables.copy(this.columns);
		this.columns.clear();
		return result;
	}
	
	public Object get(int index) {
		return this.columns.get(index);
	}
	
	public void set(int index, Object object) {
		this.columns.set(index, object);
	}
	
	@Override
	public Iterator<Object> iterator() {
		return this.columns.iterator();
	}
	
	public static class AliasedColumn extends Aliased {
		
		private final Column column;
		
		public AliasedColumn(Column column, String alias) {
			super(alias);
			this.column = column;
		}
		
		public Column getColumn() {
			return column;
		}
	}
}
