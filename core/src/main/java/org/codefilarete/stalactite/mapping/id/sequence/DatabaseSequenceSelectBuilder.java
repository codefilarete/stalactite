package org.codefilarete.stalactite.mapping.id.sequence;

/**
 * @author Guillaume Mary
 */
public interface DatabaseSequenceSelectBuilder {
	
	String buildSelect(String sequenceName);
	
}