package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelectBuilder;

/**
 * @author Guillaume Mary
 */
public class MariaDBSequenceSelectBuilder implements DatabaseSequenceSelectBuilder {
	
	@Override
	public String buildSelect(String sequenceName) {
		return "select next value for " + sequenceName;
	}
}
