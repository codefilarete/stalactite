package org.codefilarete.stalactite.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.codefilarete.reflection.AccessorDefinition;
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
	
	class ReferencedColumnNames<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> {
		
		private final Map<Column<LEFTTABLE, Object>, String> leftColumnNames = new HashMap<>();
		
		private final Map<Column<RIGHTTABLE, Object>, String> rightColumnNames = new HashMap<>();
		
		public void setLeftColumnName(Column<LEFTTABLE, ?> column, String name) {
			this.leftColumnNames.put((Column<LEFTTABLE, Object>) column, name);
		}
		
		public String getLeftColumnName(Column<LEFTTABLE, ?> column) {
			return leftColumnNames.get(column);
		}
		
		public void foreachLeftColumn(Consumer<Entry<Column<LEFTTABLE, Object>, String>> consumer) {
			this.leftColumnNames.entrySet().forEach(consumer);
		}
		
		public void setRightColumnName(Column<RIGHTTABLE, ?> column, String name) {
			this.rightColumnNames.put((Column<RIGHTTABLE, Object>) column, name);
		}
		
		public String getRightColumnName(Column<RIGHTTABLE, ?> column) {
			return rightColumnNames.get(column);
		}
		
		public void foreachRightColumn(Consumer<Entry<Column<RIGHTTABLE, Object>, String>> consumer) {
			this.rightColumnNames.entrySet().forEach(consumer);
		}
	}
	
	
	AssociationTableNamingStrategy DEFAULT = new DefaultAssociationTableNamingStrategy();
	
	/**
	 * Default implementation of the {@link AssociationTableNamingStrategy} interface.
	 * Will use relationship property name for it, prefixed with source table name.
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
			return accessor.getDeclaringClass().getSimpleName() + "_" + accessor.getName();
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
			String rightSideColumnNamePrefix = accessorDefinition.getName();
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
}
