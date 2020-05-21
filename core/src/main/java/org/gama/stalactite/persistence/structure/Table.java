package org.gama.stalactite.persistence.structure;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.KeepOrderSet;
import org.gama.lang.function.Predicates;
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
	
	private final Schema schema;
	
	private final String name;
	
	private final String absoluteName;
	
	private final KeepOrderSet<Column<SELF, Object>> columns = new KeepOrderSet<>();
	
	private PrimaryKey<SELF> primaryKey;
	
	private final Set<Index> indexes = new HashSet<>();
	
	private final Set<ForeignKey<SELF, ? extends Table<?>>> foreignKeys = new HashSet<>();
	
	private final Map<String, Column<SELF, ?>> columnsPerName = new HashMap<>();
	
	/** Made to avoid name & uppercase computation at each invokation of hashCode(). Made by principle, not for any performance issue observation */
	private final int hashCode;
	
	public Table(String name) {
		this(null, name);
	}
	
	public Table(Schema schema, String name) {
		this.schema = schema;
		this.name = name;
		this.absoluteName = (schema == null ? "" : (schema.getName() + ".")) + name;
		this.hashCode = name.toUpperCase().hashCode();
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
		result.removeAll(org.gama.lang.Nullable.nullable(getPrimaryKey()).map(PrimaryKey::getColumns).getOr(Collections.emptySet()));
		return result;
	}
	
	/**
	 * Adds a column to this table.
	 * May do nothing if a column already exists with same name and type.
	 * Will throw an exception if a column with same name but with different type already exists.
	 * 
	 * @param name column name
	 * @param javaType column type
	 * @param <O> column type
	 * @return the created column or the existing one
	 */
	public <O> Column<SELF, O> addColumn(String name, Class<O> javaType) {
		return addertColumn(new Column<>((SELF) this, name, javaType));
	}
	
	/**
	 * Adds a column to this table.
	 * May do nothing if a column already exists with same name, size and type.
	 * Will throw an exception if a column with same name but with different type or size already exists.
	 *
	 * @param name column name
	 * @param javaType column type
	 * @param size column type size, null if type doesn't require any size ({@link #addColumn(String, Class)} should be prefered in this case)
	 *             but let as such for column copy use case
	 * @param <O> column type
	 * @return the created column or the existing one
	 */
	public <O> Column<SELF, O> addColumn(String name, Class<O> javaType, Integer size) {
		return addertColumn(new Column<>((SELF) this, name, javaType, size));
	}
	
	/**
	 * Adds with presence assertion (add + assert = addert, poor naming)
	 * 
	 * @param column the column to be added
	 * @param <O> column type
	 * @return given column
	 */
	private <O> Column<SELF, O> addertColumn(Column<SELF, O> column) {
		Column<SELF, O> existingColumn = getColumn(column.getName());
		if (existingColumn != null
				&& (!existingColumn.getJavaType().equals(column.getJavaType())
				|| !Predicates.equalOrNull(existingColumn.getSize(), column.getSize()))
		) {
			throw new IllegalArgumentException("Trying to add a column that already exists with a different type : "
					+ column.getAbsoluteName() + " " + toString(existingColumn) + " vs " + toString(column));
		}
		if (existingColumn == null) {
			columns.add((Column<SELF, Object>) column);
			columnsPerName.put(column.getName(), column);
			return column;
		} else {
			return existingColumn;
		}
	}
	
	public Map<String, Column<SELF, Object>> mapColumnsOnName() {
		return new HashMap<>((Map) columnsPerName);
	}
	
	public <C> Column<SELF, C> getColumn(String columnName) {
		return (Column<SELF, C>) columnsPerName.get(columnName);
	}
	
	/**
	 * Finds a column by its name (strict equality).
	 * 
	 * @param columnName an expected matching column name
	 * @return null if not found
	 */
	public Column<SELF, Object> findColumn(String columnName) {
		return nullable(Iterables.find(columns, Column::getName, columnName::equals)).map(Duo::getLeft).get();
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
	
	public Set<ForeignKey<SELF, ?>> getForeignKeys() {
		return Collections.unmodifiableSet(foreignKeys);
	}
	
	public <T extends Table<T>, I> ForeignKey addForeignKey(BiFunction<Column, Column, String> namingFunction,
															Column<SELF, I> column, Column<T, I> targetColumn) {
		return this.addForeignKey(namingFunction.apply(column, targetColumn), column, targetColumn);
	}
	
	/**
	 * Adds a foreign key to this table.
	 * May do nothing if a foreign key already exists with same name and columns.
	 * Will throw an exception if a foreign key with same name but with different columns already exists.
	 *
	 * @param name column name
	 * @param column source column
	 * @param targetColumn referenced column
	 * @param <T> referenced table type
	 * @return the created foreign key or the existing one
	 */
	public <T extends Table<T>, I> ForeignKey addForeignKey(String name, Column<SELF, I> column, Column<T, I> targetColumn) {
		return addForeignKey(name, Collections.<Column<SELF, ?>>singletonList(column), Collections.<Column<T, ?>>singletonList(targetColumn));
	}
	
	/**
	 * Adds a foreign key to this table.
	 * May do nothing if a foreign key already exists with same name and columns.
	 * Will throw an exception if a foreign key with same name but with different columns already exists.
	 *
	 * @param name column name
	 * @param columns source columns
	 * @param targetColumns referenced columns
	 * @param <T> referenced table type
	 * @return the created foreign key or the existing one
	 */
	public <T extends Table<T>> ForeignKey addForeignKey(String name, List<? extends Column<SELF, ?>> columns, List<? extends Column<T, ?>> targetColumns) {
		return addertForeignKey(new ForeignKey<>(name, new LinkedHashSet<>(columns), new LinkedHashSet<>(targetColumns)));
	}
	
	/**
	 * Adds with presence assertion (add + assert = addert, poor naming)
	 *
	 * @param foreignKey the column to be added
	 * @param <T> target table type
	 * @return given column
	 */
	private <T extends Table<T>> ForeignKey<SELF, T> addertForeignKey(ForeignKey<SELF, T> foreignKey) {
		ForeignKey<SELF, T> existingForeignKey = (ForeignKey<SELF, T>) Iterables.find(this.foreignKeys, fk -> fk.getName().equals(foreignKey.getName()));
		if (existingForeignKey != null
				&& (!existingForeignKey.getColumns().equals(foreignKey.getColumns())
				|| !existingForeignKey.getTargetColumns().equals(foreignKey.getTargetColumns()))
		) {
			throw new IllegalArgumentException("Trying to add a foreign key that already exists with different columns : "
					+ foreignKey.getName() + " " + toString(existingForeignKey) + " vs " + toString(foreignKey));
		}
		if (existingForeignKey == null) {
			this.foreignKeys.add(foreignKey);
			return foreignKey;
		} else {
			return existingForeignKey;
		}
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
		return hashCode;
	}
	
	private static String toString(Column column) {
		return Reflections.toString(column.getJavaType()) + (column.getSize() != null ? "(" + column.getSize() + ")" : "");
	}
	
	private static String toString(ForeignKey column) {
		StringAppender result = new StringAppender() {
			@Override
			public StringAppender cat(Object s) {
				if (s instanceof Column) {
					return super.cat(((Column) s).getAbsoluteName());
				} else {
					return super.cat(s);
				}
			}
		};
		result
				.ccat(column.getColumns(), ", ")
				.cat(" -> ")
				.ccat(column.getTargetColumns(), ", ");
		return result.toString();
	}
}
