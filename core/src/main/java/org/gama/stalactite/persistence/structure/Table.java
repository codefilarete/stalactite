package org.gama.stalactite.persistence.structure;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.KeepOrderSet;
import org.gama.stalactite.persistence.structure.Database.Schema;

import static org.gama.lang.Nullable.nullable;

/**
 * Representation of a database Table, not exhaustive but sufficient for our need.
 * Primary key is design to concern only one Column, but not foreign keys ... not really logical for now !
 * Primary key as a only-one-column design is primarly intend to simplify query and persistence conception.
 *
 * @author Guillaume Mary
 */
public class Table<SELF extends Table<SELF>> {
	
	private Schema schema;
	
	private String name;
	
	private String absoluteName;
	
	private KeepOrderSet<Column<SELF, Object>> columns = new KeepOrderSet<>();
	
	private PrimaryKey<SELF> primaryKey;
	
	private Set<Index> indexes = new HashSet<>();
	
	private Set<ForeignKey<SELF, ? extends Table<?>>> foreignKeys = new HashSet<>();
	
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
	
	public Set<Column<SELF, Object>> getColumns() {
		return Collections.unmodifiableSet(columns.asSet());
	}
	
	public Set<Column<SELF, Object>> getColumnsNoPrimaryKey() {
		LinkedHashSet<Column<SELF, Object>> result = this.columns.asSet();
		result.remove(getPrimaryKey());
		return result;
	}
	
	public <O> Column<SELF, O> addColumn(String name, Class<O> javaType) {
		Column<SELF, O> column = new Column<>((SELF) this, name, javaType);
		this.columns.add((Column<SELF, Object>) column);
		return column;
	}
	
	public <O> Column<SELF, O> addColumn(String name, Class<O> javaType, int size) {
		Column<SELF, O> column = new Column<>((SELF) this, name, javaType, size);
		this.columns.add((Column<SELF, Object>) column);
		return column;
	}
	
	public Map<String, Column<SELF, Object>> mapColumnsOnName() {
		Map<String, Column<SELF, Object>> mapColumnsOnName = new LinkedHashMap<>(columns.size());
		for (Column<SELF, Object> column : columns) {
			mapColumnsOnName.put(column.getName(), column);
		}
		return mapColumnsOnName;
	}
	
	/**
	 * Finds a column by its name (strict equality).
	 * 
	 * @param columnName an expected matching column name
	 * @return null if not found
	 */
	public Column<SELF, Object> findColumn(String columnName) {
		return nullable(Iterables.find(columns, Column::getName, columnName::equals)).apply(Duo::getLeft).get();
	}
	
	/**
	 * Returns the {@link PrimaryKey} of this table if any {@link Column} was marked as primary key, else will return null.
	 * Lazyly initialize an attribute, hence multiple calls to this method may return the very first attempt even if another {@link Column} was
	 * marked as primary key between calls.
	 * 
	 * @return the {@link PrimaryKey} of this table if any {@link Column} was marked as primary key, else null
	 */
	@Nullable
	public PrimaryKey<SELF> getPrimaryKey() {
		if (primaryKey == null) {
			Set<Column<SELF, Object>> pkColumns = Iterables.collect(columns.asSet(), Column::isPrimaryKey, Function.identity(), LinkedHashSet::new);
			primaryKey = pkColumns.isEmpty() ? null : new PrimaryKey<>(pkColumns);
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
	
	public Set<ForeignKey<SELF, Table>> getForeignKeys() {
		return (Set<ForeignKey<SELF, Table>>) (Set) Collections.unmodifiableSet(foreignKeys);
	}
	
	public <T extends Table<T>, I> ForeignKey addForeignKey(String name, Column<SELF, I> column, Column<T, I> targetColumn) {
		ForeignKey<SELF, T> newForeignKey = new ForeignKey<>(name, column, targetColumn);
		this.foreignKeys.add(newForeignKey);
		return newForeignKey;
	}
	
	public <T extends Table<T>> ForeignKey addForeignKey(String name, List<Column<SELF, ?>> columns, List<Column<T, ?>> targetColumns) {
		ForeignKey<SELF, T> newForeignKey = new ForeignKey<>(name, new LinkedHashSet<>(columns), new LinkedHashSet<>(targetColumns));
		this.foreignKeys.add(newForeignKey);
		return newForeignKey;
	}
	
	/**
	 * Implementation based on name comparison. Override for comparison in Collections.
	 *
	 * @param o an Object
	 * @return true if this table name equals the other table name, case insensitive
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof Table)) {
			return false;
		}
		
		Table table = (Table) o;
		return name.equalsIgnoreCase(table.name);
	}
	
	/**
	 * Implemented to be compliant with equals override
	 * @return a hash code based on table name
	 */
	@Override
	public int hashCode() {
		return name.toUpperCase().hashCode();
	}
	
}
