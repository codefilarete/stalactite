package org.codefilarete.stalactite.sql.statement;

import javax.sql.DataSource;
import java.util.function.Predicate;

import com.mysql.cj.jdbc.exceptions.MySQLStatementCancelledException;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.MySQLDatabaseHelper;
import org.codefilarete.stalactite.sql.test.MySQLTestDataSourceSelector;
import org.codefilarete.tool.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
class SQLOperationMySQLTest extends SQLOperationITTest {
	
	private static final DataSource DATASOURCE = new MySQLTestDataSourceSelector().giveDataSource();
	
	@Override
	public DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new MySQLDatabaseHelper();
	}
	
	@Override
	String giveLockStatement() {
		return "lock table Toto WRITE";
	}
	
	@Override
	Predicate<Throwable> giveCancelOperationPredicate() {
		// MySQL throws an exception on query cancellation (https://mariadb.com/kb/en/library/multi-threading-and-statementcancel/), we check it.
		return t -> Exceptions.findExceptionInCauses(t, MySQLStatementCancelledException.class, "Statement cancelled due to client request") != null;
	}
}
