package org.codefilarete.stalactite.sql.statement;

import javax.sql.DataSource;
import java.sql.SQLTransientException;
import java.util.function.Predicate;

import org.codefilarete.stalactite.sql.test.MariaDBTestDataSourceSelector;
import org.codefilarete.tool.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
class SQLOperationMariaDBTest extends SQLOperationITTest {
	
	private static final DataSource DATASOURCE = new MariaDBTestDataSourceSelector().giveDataSource();
	
	@Override
	public DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	String giveLockStatement() {
		return "lock table Toto WRITE";
	}
	
	@Override
	Predicate<Throwable> giveCancelOperationPredicate() {
		// MySQL throws an exception on query cancelation (https://mariadb.com/kb/en/library/multi-threading-and-statementcancel/), we check it.
		return t -> Exceptions.findExceptionInCauses(t, SQLTransientException.class, m -> m.contains("Query execution was interrupted")) != null;
	}
}
