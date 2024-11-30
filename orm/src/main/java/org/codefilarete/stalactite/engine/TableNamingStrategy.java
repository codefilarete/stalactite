package org.codefilarete.stalactite.engine;

/**
 * Contract for giving a name to an entity table
 * 
 * @author Guillaume Mary
 */
public interface TableNamingStrategy {
	
	/**
	 * Gives entity table name
	 * @param persistedClass the class to be persisted 
	 * @return table name for persisted class
	 */
	String giveName(Class persistedClass);
	
	TableNamingStrategy DEFAULT = Class::getSimpleName;
	
}
