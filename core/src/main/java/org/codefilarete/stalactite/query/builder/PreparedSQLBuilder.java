package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.sql.statement.PreparedSQL;

/**
 * @author Guillaume Mary
 */
public interface PreparedSQLBuilder {
	
	ExpandableSQLAppender toPreparedSQL();
}
