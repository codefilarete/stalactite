package org.stalactite.persistence.structure;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.Finder;
import org.stalactite.lang.collection.KeepOrderSet;
import org.stalactite.persistence.structure.Database.Schema;

/**
 * @author mary
 */
public class Table {
	
	private Schema schema;

	private String name;

	private KeepOrderSet<Column> columns = new KeepOrderSet<>();

	private Column primaryKey;
	
	private Set<Index> indexes = new HashSet<>();
	
	private Set<ForeignKey> foreignKeys = new HashSet<>();
	
	public Table(Schema schema, String name) {
		this.schema = schema;
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	public KeepOrderSet<Column> getColumns() {
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
	 * Représentation d'une colonne d'une table. Toujours liée à sa table englobante par le fait d'être une inner class.
	 */
	public class Column {
		private String name;
		private Class javaType;
		private boolean primaryKey;

		/**
		 * Crée une colonne et l'ajoute à la table englobante
		 */
		public Column(String name, Class javaType) {
			this.name = name;
			this.javaType = javaType;
			getTable().add(this);
		}

		public Table getTable() {
			return Table.this;
		}

		public String getName() {
			return name;
		}

		/**
		 * Renvoie le nom de la colonne préfixé par le nom de la table. Permet d'identifier une colonne au sein d'un schéma
		 * @return getTable().getName() + "." + getName()
		 */
		public String getAbsoluteName() {
			return getTable().getName() + "." + getName();
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
		 * Implémentation basée sur la comparaison du nom. Surchargé pour les comparaisons dans les Collections
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

			Column column = (Column) o;
			return getTable().equals(column.getTable()) && name.equalsIgnoreCase(column.name);
		}

		@Override
		public int hashCode() {
			int result = getTable().hashCode();
			result = 31 * result + name.toUpperCase().hashCode();
			return result;
		}
		
		/** Overriden only for simple print (debug) */
		@Override
		public String toString() {
			return getTable().getName() + "." + getName();
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
	 * @author mary
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
	 * @author mary
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
