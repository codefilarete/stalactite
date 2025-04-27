package org.codefilarete.stalactite.sql.order;

/**
 * Contract for statements that support criteria as a where clause, and thus, may require to be able to set values for placeholders
 * 
 * @author Guillaume Mary
 */
public interface WherableStatement {
	
	<O> void setValue(String placeholderName, O value);
}
