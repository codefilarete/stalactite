package org.codefilarete.stalactite.query.builder;

/**
 * A simplistic contract to print some SQL
 * 
 * @author Guillaume Mary
 */
public interface SQLBuilder {
	
	CharSequence toSQL();
}
