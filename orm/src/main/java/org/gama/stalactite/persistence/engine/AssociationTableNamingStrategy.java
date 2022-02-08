package org.gama.stalactite.persistence.engine;

import javax.annotation.Nonnull;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.Strings;
import org.codefilarete.reflection.AccessorDefinition;
import org.gama.stalactite.persistence.structure.Column;

/**
 * Contract for giving a name to an association table (one-to-many cases)
 * 
 * @author Guillaume Mary
 */
public interface AssociationTableNamingStrategy {
	
	/**
	 * Gives association table name
	 * @param accessorDefinition a representation of the method (getter or setter) that gives the collection to be persisted
	 * @param source column that maps "one" side (on source table)
	 * @param target column that maps "many" side (on target table)
	 * @return table name for association table
	 */
	String giveName(@Nonnull AccessorDefinition accessorDefinition, @Nonnull Column source, @Nonnull Column target);
	
	Duo<String, String> giveColumnNames(@Nonnull AccessorDefinition accessorDefinition, @Nonnull Column leftPrimaryKey, @Nonnull Column rightPrimaryKey);
	
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
		public String giveName(@Nonnull AccessorDefinition accessor, @Nonnull Column source, @Nonnull Column target) {
			return accessor.getDeclaringClass().getSimpleName() + "_" + accessor.getName();
		}
		
		@Override
		public Duo<String, String> giveColumnNames(@Nonnull AccessorDefinition accessorDefinition, @Nonnull Column leftPrimaryKey, @Nonnull Column rightPrimaryKey) {
			String oneSideColumnName = Strings.uncapitalize(leftPrimaryKey.getTable().getName()) + "_" + leftPrimaryKey.getName();
			String manySideColumnName = Strings.uncapitalize(rightPrimaryKey.getTable().getName()) + "_" + rightPrimaryKey.getName();
			if (manySideColumnName.equalsIgnoreCase(oneSideColumnName)) {
				String propertyName = accessorDefinition.getName();
				// removing ending "s" if present, for good english if project speaks english (not a strong rule, could be removed, overall because
				// the strategy can be replaced by whatever needed)
				if (propertyName.endsWith("s")) {
					propertyName = Strings.cutTail(propertyName, 1).toString();
				}
				manySideColumnName = propertyName + "_" + rightPrimaryKey.getName();
				
				if (manySideColumnName.equalsIgnoreCase(oneSideColumnName)) {
					throw new MappingConfigurationException("Identical column names in association table of collection "
							+ Reflections.toString(accessorDefinition.getDeclaringClass()) + "." + accessorDefinition.getName());
				}
			}
			return new Duo<>(oneSideColumnName, manySideColumnName);
		}
	}
}
