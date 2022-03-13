package org.codefilarete.stalactite.sql.statement;

import javax.sql.DataSource;
import java.util.function.Predicate;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.PostgreSQLDatabaseHelper;
import org.codefilarete.stalactite.sql.test.PostgreSQLTestDataSourceSelector;
import org.codefilarete.tool.exception.Exceptions;
import org.postgresql.util.PSQLException;

/**
 * @author Guillaume Mary
 */
class SQLOperationPostgreSQLTest extends SQLOperationITTest {
	
	private static final DataSource DATASOURCE = new PostgreSQLTestDataSourceSelector().giveDataSource();
	
	@Override
	public DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new PostgreSQLDatabaseHelper();
	}
	
	@Override
    String giveLockStatement() {
        return "lock table Toto NOWAIT";
    }

    @Override
    Predicate<Throwable> giveCancelOperationPredicate() {
        // PostgreSQL throws an exception on query cancelation
        return t -> Exceptions.findExceptionInCauses(t, PSQLException.class, "ERROR: canceling statement due to user request") != null;
    }
}
