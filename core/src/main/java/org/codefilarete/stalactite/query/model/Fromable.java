package org.codefilarete.stalactite.query.model;

/**
 * Contract for elements to be put in a From clause, in order to be transformed to SQL
 * 
 * @author Guillaume Mary
 */
public interface Fromable extends SelectablesPod {
	
	String getName();
	
	String getAbsoluteName();
}
