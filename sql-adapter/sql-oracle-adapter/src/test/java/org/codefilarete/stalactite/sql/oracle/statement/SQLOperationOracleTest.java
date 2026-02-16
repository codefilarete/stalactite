package org.codefilarete.stalactite.sql.oracle.statement;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.function.Predicate;

import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.SQLOperation;
import org.codefilarete.stalactite.sql.statement.SQLOperationITTest;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.oracle.test.OracleDatabaseHelper;
import org.codefilarete.stalactite.sql.oracle.test.OracleTestDataSourceSelector;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.function.Predicates;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class SQLOperationOracleTest extends SQLOperationITTest {
	
	private static final DataSource DATASOURCE = new OracleTestDataSourceSelector().giveDataSource();
	
	@Override
	public DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new OracleDatabaseHelper();
	}
	
	@Override
	protected String giveLockStatement() {
		return "lock table Toto in exclusive mode nowait";
	}
	
	@Override
	protected String giveCreateTableStatement() {
		// changed because Oracle doesn't support bigint type
		return "create table Toto(id number)";
	}
	
	@Test
	@Override
	protected void cancel() throws SQLException, InterruptedException {
		super.cancel();
	}
	
	@Override
	protected Duo<Runnable, SQLOperation> createLockingStatement() {
		// SQL operation must be changed to a write statement because Oracle lock doesn't block select statement
		WriteOperation<Integer> testInstance = new WriteOperation<>(new PreparedSQL("insert into Toto(id) values (42)", new HashMap<>()), connectionProvider, writeCount -> {});
		return new Duo<>(new Runnable() {
			@Override
			public void run() {
				try {
					testInstance.execute();
				} finally {
					// we must rollback blocked statement to avoid later error (following tests) that can't clean up schema
					try {
						connectionProvider.giveConnection().rollback();
					} catch (SQLException e) {
						// we don't want to handle any kind of extra problem here
					}
				}
			}
		}, testInstance);
	}
	
	@Override
	protected Predicate<Throwable> giveCancelOperationPredicate() {
		return Predicates.acceptAll();
	}
}
