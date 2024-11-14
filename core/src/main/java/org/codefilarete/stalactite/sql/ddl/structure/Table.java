package org.codefilarete.stalactite.sql.ddl.structure;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Predicates;

/**
 * Representation of a database Table, not exhaustive but sufficient for our need.
 * Primary key is design to concern only one Column, but not foreign keys ... not really logical for now !
 * Primary key as a only-one-column design is primarly intend to simplify query and persistence conception.
 *
 * @author Guillaume Mary
 */
public class Table<SELF extends Table<SELF>> implements Fromable {
	
	private final Schema schema;
	
	private final String name;
	
	private final String absoluteName;
	
	private final KeepOrderSet<Column<SELF, Object>> columns = new KeepOrderSet<>();
	
	private PrimaryKey<SELF, ?> primaryKey;
	
	private final Set<Index> indexes = new HashSet<>();
	
	private final Set<UniqueConstraint> uniqueConstraints = new HashSet<>();
	
	private final Set<ForeignKey<SELF, ? extends Table<?>, ?>> foreignKeys = new HashSet<>();
	
	private final Map<String, Column<SELF, ?>> columnsPerName = new HashMap<>();
	
	/** Made to avoid name & uppercase computation at each invocation of hashCode(). Made by principle, not for any performance issue observation */
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
	
	/**
	 * Gives columns of this table as an unmodifiable set.
	 * 
	 * @return an unmodifiable Set&lt;Column&gt;
	 */
	public Set<Column<SELF, ?>> getColumns() {
		return Collections.unmodifiableSet(columns);
	}
	
	@Override
	public Map<Selectable<?>, String> getAliases() {
		Map<Column<SELF, ?>, String> result = new HashMap<>();
		columnsPerName.forEach((key, value) -> result.put(value, key));
		return Collections.unmodifiableMap(result);
	}
	
	public Set<Column<SELF, Object>> getColumnsNoPrimaryKey() {
		LinkedHashSet<Column<SELF, Object>> result = new LinkedHashSet<>(this.columns);
		result.removeAll(org.codefilarete.tool.Nullable.nullable(getPrimaryKey()).map(PrimaryKey::getColumns).getOr(new KeepOrderSet<>()));
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
			throw new IllegalArgumentException("Trying to add column '"+column.getName()+"' to '" + this.getAbsoluteName() + "' but it already exists with a different type : "
					+ typeToString(existingColumn) + " vs " + typeToString(column));
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
	@Override
	public Column<SELF, ?> findColumn(String columnName) {
		return getColumn(columnName);
	}
	
	/**
	 * Returns the {@link PrimaryKey} of this table if any {@link Column} was marked as primary key, else will return null.
	 * Lazyly initialize an attribute, hence multiple calls to this method may return the very first attempt even if another {@link Column} was
	 * marked as primary key between calls.
	 * 
	 * @return the {@link PrimaryKey} of this table if any {@link Column} was marked as primary key, else null
	 */
	public <ID> PrimaryKey<SELF, ID> getPrimaryKey() {
		if (primaryKey == null) {
			Set<Column<SELF, Object>> pkColumns = Iterables.collect(columns, Column::isPrimaryKey, Function.identity(), LinkedHashSet::new);
			primaryKey = pkColumns.isEmpty() ? null : new PrimaryKey<>(pkColumns);
		}
		return (PrimaryKey<SELF, ID>) primaryKey;
	}
	
	public Set<Index> getIndexes() {
		return Collections.unmodifiableSet(indexes);
	}
	
	public Index addIndex(String name, Column<SELF, ?> column, Column<SELF, ?> ... columns) {
		Index newIndex = new Index(name, column, columns);
		this.indexes.add(newIndex);
		return newIndex;
	}
	
	public Set<ForeignKey<SELF, ?, ?>> getForeignKeys() {
		return Collections.unmodifiableSet(foreignKeys);
	}
	
	public <T extends Table<T>, I> ForeignKey<SELF, T, I> addForeignKey(BiFunction<Column<SELF, I>, Column<T, I>, String> namingFunction,
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
	public <T extends Table<T>, I> ForeignKey<SELF, T, I> addForeignKey(String name, Column<SELF, I> column, Column<T, I> targetColumn) {
		return addssertForeignKey(new ForeignKey<>(name, column, targetColumn));
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
	public <T extends Table<T>, I> ForeignKey<SELF, T, I> addForeignKey(String name, List<? extends Column<SELF, Object>> columns, List<? extends Column<T, Object>> targetColumns) {
		return addssertForeignKey(new ForeignKey<>(name, new KeepOrderSet<>(columns), new KeepOrderSet<>(targetColumns)));
	}
	
	public <T extends Table<T>, I> ForeignKey<SELF, T, I> addForeignKey(String name, Key<SELF, I> columns, Key<T, I> targetColumns) {
		ForeignKey<SELF, T, I> foreignKey = new ForeignKey<>(
				name,
				(KeepOrderSet<? extends Column<SELF, Object>>) columns.getColumns(),
				(KeepOrderSet<? extends Column<T, Object>>) targetColumns.getColumns());
		return addssertForeignKey(foreignKey);
	}
	
	public <T extends Table<T>, I, K1 extends Key<SELF, I>, K2 extends Key<T, I>> ForeignKey<SELF, T, I> addForeignKey(
			BiFunction<K1, K2, String> namingFunction,
			K1 sourceColumns,
			K2 targetColumns) {
		ForeignKey<SELF, T, I> foreignKey = new ForeignKey<>(
				namingFunction.apply(sourceColumns, targetColumns),
				(KeepOrderSet<? extends Column<SELF, Object>>) sourceColumns.getColumns(),
				(KeepOrderSet<? extends Column<T, Object>>) targetColumns.getColumns());
		return addssertForeignKey(foreignKey);
	}
	
	/**
	 * Adds with presence assertion (add + assert = addssert, poor naming)
	 *
	 * @param foreignKey the column to be added
	 * @param <T> target table type
	 * @return given column
	 */
	private <T extends Table<T>, I> ForeignKey<SELF, T, I> addssertForeignKey(ForeignKey<SELF, T, I> foreignKey) {
		ForeignKey<SELF, T, I> existingForeignKey = assertDoesNotExistOnSameName(foreignKey);
		if (existingForeignKey == null) {
			assertDoesNotExistOnSameColumns(foreignKey);
			this.foreignKeys.add(foreignKey);
			return foreignKey;
		} else {
			return existingForeignKey;
		}
	}
	
	private <T extends Table<T>, I> ForeignKey<SELF, T, I> assertDoesNotExistOnSameName(ForeignKey<SELF, T, I> foreignKey) {
		ForeignKey<SELF, T, I> existingForeignKey = (ForeignKey<SELF, T, I>) Iterables.find(this.foreignKeys, fk -> fk.getName().equals(foreignKey.getName()));
		if (existingForeignKey != null
				&& (!existingForeignKey.getColumns().equals(foreignKey.getColumns())
				|| !existingForeignKey.getTargetColumns().equals(foreignKey.getTargetColumns()))
		) {
			throw new IllegalArgumentException("Trying to add a foreign key with same name than another with different columns : "
					+ foreignKey.getName() + " " + toString(existingForeignKey) + " vs " + toString(foreignKey));
		}
		return existingForeignKey;
	}
	
	private <T extends Table<T>, I> ForeignKey<SELF, T, I> assertDoesNotExistOnSameColumns(ForeignKey<SELF, T, I> foreignKey) {
		ForeignKey<SELF, T, I> existingForeignKeyWithSameColumns = (ForeignKey<SELF, T, I>) Iterables.find(this.foreignKeys, fk -> fk.getColumns().equals(foreignKey.getColumns()));
		if (existingForeignKeyWithSameColumns != null
				&& !existingForeignKeyWithSameColumns.getTargetColumns().equals(foreignKey.getTargetColumns())) {
			throw new IllegalArgumentException("A foreign key with same source columns but different referenced columns already exist : "
					+ "'" + existingForeignKeyWithSameColumns.getName() + "' <" + toString(existingForeignKeyWithSameColumns) + ">"
					+ " vs wanted new one '" + foreignKey.getName() + "' <" + toString(foreignKey) + ">");
		}
		return existingForeignKeyWithSameColumns;
	}
	
	public Set<UniqueConstraint> getUniqueConstraints() {
		return uniqueConstraints;
	}
	
	public UniqueConstraint addUniqueConstraint(String name, Column<SELF, ?> column, Column<SELF, ?> ... columns) {
		UniqueConstraint newUniqueConstraint = new UniqueConstraint(name, column, columns);
		this.uniqueConstraints.add(newUniqueConstraint);
		return newUniqueConstraint;
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
		return getName().equalsIgnoreCase(table.getName());
	}
	
	/**
	 * Implemented to be compliant with equals override
	 * @return a hash code based on table name
	 */
	@Override
	public int hashCode() {
		return hashCode;
	}
	
	private static String typeToString(Column column) {
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
