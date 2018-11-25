package org.gama.stalactite.persistence.engine;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;

import org.gama.lang.Reflections;
import org.gama.lang.Reflections.MemberNotFoundException;
import org.gama.stalactite.persistence.structure.Column;

/**
 * Contract for giving a name to an association table (one-to-many cases)
 * 
 * @author Guillaume Mary
 */
public interface AssociationTableNamingStrategy {
	
	/**
	 * Gives association table name
	 * @param getter one-to-many getter 
	 * @param source column that maps "one" side (on source table)
	 * @param target column that maps "many" side (on target table)
	 * @return table name for association table
	 */
	String giveName(@Nonnull Method getter, @Nonnull Column source, @Nonnull Column target);
	
	String giveOneSideColumnName(@Nonnull Column source);
	
	default String giveManySideColumnName(@Nonnull Column source) {
		return giveOneSideColumnName(source);
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
		public String giveName(@Nonnull Method member, @Nonnull Column source, @Nonnull Column target) {
			String suffix;
			try {
				suffix = Reflections.propertyName(member);
			} catch (MemberNotFoundException e) {
				suffix = target.getTable().getName() + "s";
			}
			return source.getTable().getName() + "_" + suffix;
		}
		
		@Override
		public String giveOneSideColumnName(@Nonnull Column source) {
			return source.getTable().getName() + "_" + source.getName();
		}
	}
}
