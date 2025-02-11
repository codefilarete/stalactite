package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelectBuilder;

/**
 * @author Guillaume Mary
 */
public class HSQLDBSequenceSelectBuilder implements DatabaseSequenceSelectBuilder {
	
	@Override
	public String buildSelect(String sequenceName) {
		return "CALL NEXT VALUE FOR " + sequenceName;
	}
}
