package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelectBuilder;

/**
 * @author Guillaume Mary
 */
public class H2SequenceSelectBuilder implements DatabaseSequenceSelectBuilder {
	
	@Override
	public String buildSelect(String sequenceName) {
		return "SELECT NEXT VALUE FOR " + sequenceName;
	}
}
