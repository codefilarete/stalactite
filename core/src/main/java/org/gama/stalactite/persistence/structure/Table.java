package org.gama.stalactite.persistence.structure;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.Finder;
import org.gama.lang.collection.KeepOrderSet;
import org.gama.stalactite.persistence.structure.Database.Schema;

/**
 * Representation of a database Table, not exhaustive but sufficient for our need.
 * Primary key is design to concern only one Column, but not foreign keys ... not really logical for now !
 * Primary key as a only-one-column design is primarly intend to simplify query and persistence conception.
 *
 * @author Guillaume Mary
 */
public class Table {
	
	private Schema schema;
	
	private String name;
	
	private String absoluteName;
	
	private KeepOrderSet<Column> columns = new KeepOrderSet<>();
	
	private Column primaryKey;
	
	private Set<Index> indexes = new HashSet<>();
	
	private Set<ForeignKey> foreignKeys = new HashSet<>();
	
	public Table(String name) {
		this(null, name);
	}
	
	public Table(Schema schema, String name) {
		this.schema = schema;
		this.name = name;
		this.absoluteName = (schema == null ? "" : (schema.getName() + ".")) + name;
	}
	
	public Schema getSchema() {
		return schema;
	}
	
	public String getName() {
		return name;
	}
	
	public String getAbsoluteName() {
		return absoluteName;
	}
	
	public Set<Column> getColumns() {
		return Collections.unmodifiableSet(columns.asSet());
	}
	
	public Set<Column> getColumnsNoPrimaryKey() {
		LinkedHashSet<Column> columns = this.columns.asSet();
		columns.remove(getPrimaryKey());
		return columns;
	}
	
	public <T> Column<T> addColumn(String name, Class<T> javaType) {
		Column<T> column = new Column<>(this, name, javaType);
		this.columns.add(column);
		return column;
	}
	
	public <T> Column<T> addColumn(String name, Class<T> javaType, int size) {
		Column<T> column = new Column<>(this, name, javaType, size);
		this.columns.add(column);
		return column;
	}
	
	public Map<String, Column> mapColumnsOnName() {
		Map<String, Column> mapColumnsOnName = new LinkedHashMap<>(columns.size());
		for (Column column : columns) {
			mapColumnsOnName.put(column.getName(), column);
		}
		return mapColumnsOnName;
	}
	
	public Column getPrimaryKey() {
		if (primaryKey == null) {
			this.primaryKey = Iterables.filter(columns, new Finder<Column>() {
				@Override
				public boolean accept(Column column) {
					return column.isPrimaryKey();
				}
			});
		}
		return primaryKey;
	}
	
	public Set<Index> getIndexes() {
		return Collections.unmodifiableSet(indexes);
	}
	
	public Index addIndex(String name, Column column, Column ... columns) {
		LinkedHashSet<Column> indexedColumns = Arrays.asSet(column);
		indexedColumns.addAll(java.util.Arrays.asList(columns));
		Index newIndex = new Index(name, indexedColumns);
		this.indexes.add(newIndex);
		return newIndex;
	}
	
	public Set<ForeignKey> getForeignKeys() {
		return Collections.unmodifiableSet(foreignKeys);
	}
	
	public ForeignKey addForeignKey(String name, Column column, Column targetColumn) {
		ForeignKey newForeignKey = new ForeignKey(name, column, targetColumn);
		this.foreignKeys.add(newForeignKey);
		return newForeignKey;
	}
	
	public ForeignKey addForeignKey(String name, List<Column> columns, List<Column> targetColumns) {
		ForeignKey newForeignKey = new ForeignKey(name, new LinkedHashSet<>(columns), new LinkedHashSet<>(targetColumns));
		this.foreignKeys.add(newForeignKey);
		return newForeignKey;
	}
	
	/**
	 * Implémentation basée sur la comparaison du nom. Surchargé pour les comparaisons dans les Collections
	 *
	 * @param o un Object
	 * @return true si le nom de la colonne comparée est le même que celui de celle-ci, insensible à la casse
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		
		Table table = (Table) o;
		return name.equalsIgnoreCase(table.name);
	}
	
	@Override
	public int hashCode() {
		return name.toUpperCase().hashCode();
	}
	
}
