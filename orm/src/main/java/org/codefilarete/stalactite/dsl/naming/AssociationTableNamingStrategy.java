package org.codefilarete.stalactite.dsl.naming;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Strings;

/**
 * Contract for giving a name to an association table (one-to-many cases)
 * 
 * @author Guillaume Mary
 */
public interface AssociationTableNamingStrategy {
	
	/**
	 * Gives association table name
	 * @param accessorDefinition a representation of the method (getter or setter) that gives the collection to be persisted,
	 * given to contextualize method call and help to decide which naming to apply
	 * @param source columns that maps "one" side (on source table)
	 * @param target columns that maps "many" side (on target table)
	 * @return table name for association table
	 */
	<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	String giveName(AccessorDefinition accessorDefinition, PrimaryKey<LEFTTABLE, ?> source, PrimaryKey<RIGHTTABLE, ?> target);
	
	/**
	 * Gives column names referenced by given keys
	 *
	 * @param accessorDefinition a representation of the method (getter or setter) that gives the collection to be persisted,
	 * given to contextualize method call and help to decide which naming to apply
	 * @param leftPrimaryKey primary key of left-side-entity table 
	 * @param rightPrimaryKey primary key of right-side-entity table
	 * @return a mapping between each left and right primary key columns and the names of the ones that should be created in association table 
	 */
	<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, LEFTID, RIGHTID> ReferencedColumnNames<LEFTTABLE, RIGHTTABLE>
	giveColumnNames(AccessorDefinition accessorDefinition, PrimaryKey<LEFTTABLE, LEFTID> leftPrimaryKey, PrimaryKey<RIGHTTABLE, RIGHTID> rightPrimaryKey);
	
	/**
	 * Small structure that stores column names of association with their matching exported key column in left and right tables 
	 * 
	 * @param <LEFTTABLE> left table type
	 * @param <RIGHTTABLE> right table type
	 */
	class ReferencedColumnNames<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> {
		
		private final Map<Column<LEFTTABLE, Object>, String> leftColumnNames = new HashMap<>();
		
		private final Map<Column<RIGHTTABLE, Object>, String> rightColumnNames = new HashMap<>();
		
		/**
		 * Set left column name in association table that matches given left-table column
		 * @param column the column coming from left table
		 * @param name name of association table column for given left-table column
		 */
		public void setLeftColumnName(Column<LEFTTABLE, ?> column, String name) {
			this.leftColumnNames.put((Column<LEFTTABLE, Object>) column, name);
		}
		
		/**
		 * Give left column name in association table that matches given left-table column
		 * @param column the left-table column 
		 */
		public String getLeftColumnName(Column<LEFTTABLE, ?> column) {
			return leftColumnNames.get(column);
		}
		
		/**
		 * Exposes all column references to simplify column name modification : without this access the user has to use {@link #getLeftColumnName(Column)}
		 * which implies to have the {@link Column} object, this is cumbersome because it means the user must have declared its {@link Table}.
		 * @return all column references
		 */
		public Map<Column<LEFTTABLE, Object>, String> getLeftColumnNames() {
			return leftColumnNames;
		}
		
		/**
		 * Set right column name in association table that matches given right-table column
		 * @param column the column coming from left table
		 * @param name name of association table column for given left-table column
		 */
		public void setRightColumnName(Column<RIGHTTABLE, ?> column, String name) {
			this.rightColumnNames.put((Column<RIGHTTABLE, Object>) column, name);
		}
		
		/**
		 * Give right column name in association table that matches given right-table column
		 * @param column the right-table column
		 */
		public String getRightColumnName(Column<RIGHTTABLE, ?> column) {
			return rightColumnNames.get(column);
		}
		
		/**
		 * Exposes all column references to simplify column name modification : without this access the user has to use {@link #getRightColumnName(Column)}
		 * which implies to have the {@link Column} object, this is cumbersome because it means the user must have declared its {@link Table}.
		 * @return all column references
		 */
		public Map<Column<RIGHTTABLE, Object>, String> getRightColumnNames() {
			return rightColumnNames;
		}
	}
	
	
	AssociationTableNamingStrategy DEFAULT = new DefaultAssociationTableNamingStrategy();
	
	AssociationTableNamingStrategy HIBERNATE = new HibernateAssociationTableNamingStrategy();
	
	/**
	 * Default implementation of the {@link AssociationTableNamingStrategy} interface.
	 * Will use relation property name for it, prefixed with source table name.
	 * For instance: for a Country entity with the getCities() getter to retrieve country cities,
	 * the association table will be named "Country_cities".
	 * If property cannot be deduced from getter (it doesn't start with "get") then target table name will be used, suffixed by "s".
	 * For instance: for a Country entity with the giveCities() getter to retrieve country cities,
	 * the association table will be named "Country_Citys".
	 */
	class DefaultAssociationTableNamingStrategy implements AssociationTableNamingStrategy {
		
		@Override
		public <LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		String giveName(AccessorDefinition accessor, PrimaryKey<LEFTTABLE, ?> source, PrimaryKey<RIGHTTABLE, ?> target) {
			return accessor.getDeclaringClass().getSimpleName() + "_" + accessor.getName().replace('.', '_');
		}
		
		@Override
		public <LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, LEFTID, RIGHTID>
		ReferencedColumnNames<LEFTTABLE, RIGHTTABLE> giveColumnNames(AccessorDefinition accessorDefinition,
																	 PrimaryKey<LEFTTABLE, LEFTID> leftPrimaryKey,
																	 PrimaryKey<RIGHTTABLE, RIGHTID> rightPrimaryKey) {
			
			ReferencedColumnNames<LEFTTABLE, RIGHTTABLE> result = new ReferencedColumnNames<>();
			
			// columns pointing to left table get same names as original ones
			leftPrimaryKey.getColumns().forEach(column -> {
				String leftColumnName = Strings.uncapitalize(leftPrimaryKey.getTable().getName()) + "_" + column.getName();
				result.setLeftColumnName(column, leftColumnName);
			});
			Set<String> existingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
			existingColumns.addAll(result.leftColumnNames.values());
			
			// columns pointing to right table get a name that contains accessor definition name
			String rightSideColumnNamePrefix = accessorDefinition.getName().replace('.', '_');
			rightPrimaryKey.getColumns().forEach(column -> {
				String rightColumnName = Strings.uncapitalize(rightSideColumnNamePrefix + "_" + column.getName());
				if (existingColumns.contains(rightColumnName)) {
					throw new MappingConfigurationException("Identical column names in association table of collection "
							+ accessorDefinition + " : " + rightColumnName);
				}
				result.setRightColumnName(column, rightColumnName);
			});
			return result;
		}
	}
	
	/**
	 * Hibernate implementation of the {@link AssociationTableNamingStrategy} interface.
	 * The table name is made of left table one and right table one, separated by an underscore.
	 * @author Guillaume Mary
	 */
	class HibernateAssociationTableNamingStrategy implements AssociationTableNamingStrategy {
		
		@Override
		public <LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		String giveName(AccessorDefinition accessor, PrimaryKey<LEFTTABLE, ?> source, PrimaryKey<RIGHTTABLE, ?> target) {
			return source.getTable().getName() + "_" + target.getTable().getName();
		}
		
		@Override
		public <LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, LEFTID, RIGHTID>
		ReferencedColumnNames<LEFTTABLE, RIGHTTABLE> giveColumnNames(AccessorDefinition accessor,
																	 PrimaryKey<LEFTTABLE, LEFTID> leftPrimaryKey,
																	 PrimaryKey<RIGHTTABLE, RIGHTID> rightPrimaryKey) {
			
			ReferencedColumnNames<LEFTTABLE, RIGHTTABLE> result = new ReferencedColumnNames<>();
			
			// columns pointing to left table get same names as original ones
			leftPrimaryKey.getColumns().forEach(column -> {
				String leftColumnName = Strings.uncapitalize(leftPrimaryKey.getTable().getName()) + "_" + column.getName();
				result.setLeftColumnName(column, leftColumnName);
			});
			Set<String> existingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
			existingColumns.addAll(result.leftColumnNames.values());
			
			// columns pointing to right table get a name that contains accessor definition name
			rightPrimaryKey.getColumns().forEach(column -> {
				String rightColumnName = column.getName();
				if (existingColumns.contains(rightColumnName)) {
					throw new MappingConfigurationException("Identical column names in association table of collection "
							+ accessor + " : " + rightColumnName);
				}
				result.setRightColumnName(column, rightColumnName);
			});
			return result;
		}
	}
}
