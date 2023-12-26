package org.codefilarete.stalactite.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.Strings;

import static org.codefilarete.tool.Reflections.GET_SET_PREFIX_REMOVER;
import static org.codefilarete.tool.Reflections.IS_PREFIX_REMOVER;

/**
 * Table naming strategy contract for {@link java.util.Map} element table
 * 
 * @author Guillaume Mary
 */
public interface MapEntryTableNamingStrategy {
	
	/**
	 * Gives association table name
	 * @param accessorDefinition a representation of the method (getter or setter) that gives the map to be persisted
	 * @return table name for {@link java.util.Map} element table
	 */
	String giveTableName(AccessorDefinition accessorDefinition, Class<?> keyType, Class<?> valueType);
	
	<RIGHTTABLE extends Table<RIGHTTABLE>, RIGHTID> Map<Column<RIGHTTABLE, ?>, String>
	giveMapKeyColumnNames(AccessorDefinition accessorDefinition,
						  Class entityType,
						  PrimaryKey<RIGHTTABLE, RIGHTID> rightPrimaryKey,
						  Set<String> existingColumnNames);
	
	MapEntryTableNamingStrategy DEFAULT = new DefaultMapEntryTableNamingStrategy();
	
	/**
	 * Default implementation of the {@link MapEntryTableNamingStrategy} interface.
	 * Will use relation property name for it, prefixed with source table name.
	 * For instance: for a Country entity with the getCities() getter to retrieve country cities,
	 * the association table will be named "Country_cities".
	 * If property cannot be deduced from getter (it doesn't start with "get") then target table name will be used, suffixed by "s".
	 * For instance: for a Country entity with the giveCities() getter to retrieve country cities,
	 * the association table will be named "Country_citys".
	 */
	class DefaultMapEntryTableNamingStrategy implements MapEntryTableNamingStrategy {
		
		@Override
		public String giveTableName(AccessorDefinition accessorDefinition, Class<?> keyType, Class<?> valueType) {
			String suffix = Reflections.onJavaBeanPropertyWrapperNameGeneric(accessorDefinition.getName(), accessorDefinition.getName(),
					GET_SET_PREFIX_REMOVER.andThen(Strings::uncapitalize),
					GET_SET_PREFIX_REMOVER.andThen(Strings::uncapitalize),
					IS_PREFIX_REMOVER.andThen(Strings::uncapitalize),
					methodName -> methodName);	// on non-compliant Java Bean Naming Convention, method name is returned as table name
			return accessorDefinition.getDeclaringClass().getSimpleName() + "_" + suffix;
		}
		
		@Override
		public <RIGHTTABLE extends Table<RIGHTTABLE>, RIGHTID> Map<Column<RIGHTTABLE, ?>, String>
		giveMapKeyColumnNames(AccessorDefinition accessorDefinition,
							  Class entityType,
							  PrimaryKey<RIGHTTABLE, RIGHTID> rightPrimaryKey,
							  Set<String> existingColumnNames) {
			Map<Column<RIGHTTABLE, ?>, String> result = new HashMap<>();
			
			Set<String> existingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
			existingColumns.addAll(existingColumnNames);
			
			// columns pointing to right table get a name that contains accessor definition name
			String rightSideColumnNamePrefix = entityType.getSimpleName();
			rightPrimaryKey.getColumns().forEach(column -> {
				String rightColumnName = Strings.uncapitalize(rightSideColumnNamePrefix + "_" + column.getName());
				if (existingColumns.contains(rightColumnName)) {
					throw new MappingConfigurationException("Identical column names in association table of collection "
							+ accessorDefinition + " : " + rightColumnName);
				}
				result.put(column, rightColumnName);
			});
			return result;
		}
	}
}