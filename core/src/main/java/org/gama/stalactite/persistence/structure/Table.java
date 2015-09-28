package org.gama.stalactite.persistence.structure;

import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.Finder;
import org.gama.lang.collection.KeepOrderSet;
import org.gama.stalactite.persistence.structure.Database.Schema;

import java.util.*;

/**
 * Representation of a database Table, not exhaustive but sufficient for our need.
 * Column is an inner class of Table so its easy to find owning relationship, whereas a Column can't be transfered to
 * another table.
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
	
	public String getName() {
		return name;
	}
	
	public String getAbsoluteName() {
		return absoluteName;
	}
	
	public KeepOrderSet<Column> getColumns() {
		return columns;
	}
	
	public Set<Column> getColumnsNoPrimaryKey() {
		LinkedHashSet<Column> columns = this.columns.asSet();
		columns.remove(getPrimaryKey());
		return columns;
	}
	
	public void add(Column column) {
		this.columns.add(column);
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
		return indexes;
	}
	
	public void add(Index index) {
		this.indexes.add(index);
	}
	
	public Set<ForeignKey> getForeignKeys() {
		return foreignKeys;
	}
	
	public void add(ForeignKey foreignKey) {
		this.foreignKeys.add(foreignKey);
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
	
	/**
	 * Column of a table. Always wired to its table by the fact that's an inner class
	 */
	public class Column {
		private final String name;
		private final String absoluteName;
		private final String alias;
		private final Class javaType;
		private boolean primaryKey;
		
		/**
		 * Build a column and add it to the outer table
		 */
		public Column(String name, Class javaType) {
			this.name = name;
			this.javaType = javaType;
			this.absoluteName = getTable().getName() + "." + getName();
			this.alias = getTable().getName() + "_" + getName();
			getTable().add(this);
		}
		
		public Table getTable() {
			return Table.this;
		}
		
		public String getName() {
			return name;
		}
		
		/**
		 * Gives the column name prefixed by table name. Allows column identification in a schema.
		 *
		 * @return getTable().getName() + "." + getName()
		 */
		public String getAbsoluteName() {
			return absoluteName;
		}
		
		/**
		 * Provides a default alias usable for select clause
		 * @return getTable().getName() +"_" + getName()
		 */
		public String getAlias() {
			return alias;
		}
		
		public Class getJavaType() {
			return javaType;
		}
		
		public boolean isNullable() {
			return !javaType.isPrimitive();
		}
		
		public boolean isPrimaryKey() {
			return primaryKey;
		}
		
		public void setPrimaryKey(boolean primaryKey) {
			this.primaryKey = primaryKey;
		}
		
		/**
		 * Implementation based on absolute name comparison. Done for Collections comparison.
		 *
		 * @param o un Object
		 * @return true if absolute name of both Column (this and o) are the same ignoring case.
		 */
		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			
			Column column = (Column) o;
			return getAbsoluteName().equalsIgnoreCase(column.getAbsoluteName());
		}
		
		@Override
		public int hashCode() {
			return getAbsoluteName().toUpperCase().hashCode();
		}
		
		/**
		 * Overriden only for simple print (debug)
		 */
		@Override
		public String toString() {
			return getAbsoluteName();
		}
	}
	
	public class SizedColumn extends Column {
		private int size;
		
		public SizedColumn(String name, Class javaType, int size) {
			super(name, javaType);
			this.size = size;
		}
		
		public int getSize() {
			return size;
		}
	}
	
	/**
	 * @author Guillaume Mary
	 */
	public class Index {
		private KeepOrderSet<Column> columns;
		private String name;
		private boolean unique = false;
		
		public Index(Column column, String name) {
			this(new KeepOrderSet<>(column), name);
		}
		
		public Index(KeepOrderSet<Column> columns, String name) {
			this.columns = columns;
			this.name = name;
			getTable().add(this);
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
		
		public Table getTable() {
			return Table.this;
		}
	}
	
	/**
	 * @author Guillaume Mary
	 */
	public class ForeignKey {
		private KeepOrderSet<Column> columns;
		private String name;
		private KeepOrderSet<Column> targetColumns;
		private Table targetTable;
		
		public ForeignKey(Column column, String name, Column targetColumn) {
			this(new KeepOrderSet<>(column), name, new KeepOrderSet<>(targetColumn));
		}
		
		public ForeignKey(KeepOrderSet<Column> columns, String name, KeepOrderSet<Column> targetColumns) {
			this.columns = columns;
			this.name = name;
			this.targetColumns = targetColumns;
			this.targetTable = Iterables.first(targetColumns).getTable();
			getTable().add(this);
		}
		
		public KeepOrderSet<Column> getColumns() {
			return columns;
		}
		
		public String getName() {
			return name;
		}
		
		public KeepOrderSet<Column> getTargetColumns() {
			return targetColumns;
		}
		
		public Table getTable() {
			return Table.this;
		}
		
		public Table getTargetTable() {
			return targetTable;
		}
	}
}
