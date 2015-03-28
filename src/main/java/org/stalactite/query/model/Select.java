package org.stalactite.query.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class Select implements Iterable<Object> {
	/** String, Column ou AliasedColumn */
	private List<Object> columns = new ArrayList<>(5);

	private Select add(Object column) {
		this.columns.add(column);
		return this;
	}

	public Select add(Column column) {
		return add((Object) column);
	}

	public Select add(Column... columns) {
		for (Column col : columns) {
			add(col);
		}
		return this;
	}

	public Select add(String... columns) {
		for (String col : columns) {
			add(col);
		}
		return this;
	}

	public Select add(Column column, String alias) {
		return add(new AliasedColumn(column, alias));
	}

	public Select add(Map<Column, String> aliasedColumns) {
		for (Entry<Column, String> aliasedColumn : aliasedColumns.entrySet()) {
			add(new AliasedColumn(aliasedColumn.getKey(), aliasedColumn.getValue()));
		}
		return this;
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

		public AliasedColumn(Column column) {
			this.column = column;
		}

		public AliasedColumn(Column column, String alias) {
			super(alias);
			this.column = column;
		}

		public Column getColumn() {
			return column;
		}
	}
}
