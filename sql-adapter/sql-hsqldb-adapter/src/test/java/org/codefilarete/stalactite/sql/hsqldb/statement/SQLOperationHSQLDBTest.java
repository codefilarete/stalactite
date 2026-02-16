package org.codefilarete.stalactite.sql.hsqldb.statement;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.function.Predicate;

import org.codefilarete.stalactite.sql.statement.SQLOperationITTest;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBDatabaseHelper;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
class SQLOperationHSQLDBTest extends SQLOperationITTest {
	
	@Override
	public DataSource giveDataSource() {
		return new HSQLDBInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new HSQLDBDatabaseHelper();
	}
	
	@Override
	protected String giveLockStatement() {
		return "lock table Toto WRITE";
	}
	
	@Override
	protected Predicate<Throwable> giveCancelOperationPredicate() {
		return Objects::isNull;
	}
}
