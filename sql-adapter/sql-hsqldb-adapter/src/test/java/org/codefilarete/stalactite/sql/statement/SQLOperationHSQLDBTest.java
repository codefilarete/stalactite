package org.codefilarete.stalactite.sql.statement;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.function.Predicate;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBDatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;

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
	String giveLockStatement() {
		return "lock table Toto WRITE";
	}
	
	@Override
	Predicate<Throwable> giveCancelOperationPredicate() {
		return Objects::isNull;
	}
}
