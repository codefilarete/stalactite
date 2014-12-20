package org.stalactite.persistence.structure;

import org.stalactite.lang.collection.KeepOrderSet;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class Index {
	private KeepOrderSet<Column> columns;
	private String name;
	private boolean unique = false;

	public Index(Column column, String name) {
		this(new KeepOrderSet<Column>(column), name);
	}

	public Index(KeepOrderSet<Column> columns, String name) {
		this.columns = columns;
		this.name = name;
	}

	public KeepOrderSet<Column> getColumns() {
		return columns;
	}

	public String getName() {
		return name;
	}

	public boolean isUnique() {
		return unique;
	}

	public void setUnique(boolean unique) {
		this.unique = unique;
	}
}
