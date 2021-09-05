package org.gama.stalactite.persistence.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.gama.lang.Retryer;
import org.gama.lang.Retryer.RetryException;
import org.gama.lang.function.ThrowingBiFunction;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.SQLExecutionException;
import org.gama.stalactite.sql.dml.SQLStatement;
import org.gama.stalactite.sql.dml.WriteOperation;

/**
 * MySQL's implementation of {@link WriteOperation} to retry statement execution because MySQL InnoDB has a poor behavior of insertion concurrency
 * 
 * @author Guillaume Mary
 */
public class MySQLWriteOperation<ParamType> extends WriteOperation<ParamType> {
	
	private final Retryer retryer;
	
	private final ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider;
	
	public MySQLWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, Retryer retryer,
							   ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider) {
		super(sqlGenerator, connectionProvider);
		this.retryer = retryer;
		this.statementProvider = statementProvider;
	}
	
	public MySQLWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, Retryer retryer) {
		this(sqlGenerator, connectionProvider, retryer, Connection::prepareStatement);
	}
	
	@Override
	protected void prepareStatement(Connection connection) throws SQLException {
		this.preparedStatement = statementProvider.apply(connection, getSQL());
	}
	
	@Override
	protected int[] doExecuteBatch() throws SQLException {
		try {
			return retryer.execute(preparedStatement::executeBatch, getSQL());
		} catch (RetryException e) {
			throw new SQLExecutionException(getSQL(), e);
		}
	}
	
	@Override
	protected int doExecuteUpdate() throws SQLException {
		try {
			return retryer.execute(preparedStatement::executeUpdate, getSQL());
		} catch (RetryException e) {
			throw new SQLExecutionException(getSQL(), e);
		}
	}
	
	/**
	 * Overriden to count {@link Statement#SUCCESS_NO_INFO} as 1 update to get around MySQL way of counting updated rows.
	 * This is a temporary fix : according to JDBC documentation it appears that returning a sum of updated element can hardly be achieved since
	 * JDBC specification states that {@link Statement#SUCCESS_NO_INFO} is an accepted value, even if many databases return real updated row count.
	 * Sadly MySQL seems to differ on that point.
	 * Hence, the gloabl approach of returning the sum of updated row count must be reviewed : it simply can't be done. Since the goal is to check
	 * that row count matches SQLStatement order, the solution would be that caller gives its way of checking it at construction time since it's
	 * there that SQL statement is also given and both can be decoralated.
	 * 
	 * TODO : fix global row count computation approach
	 * 
	 * @param rowCounts
	 * @return
	 */
	@Override
	protected int computeUpdatedRowCount(int[] rowCounts) {
		int updatedRowCountSum = 0;
		for (int rowCount : rowCounts) {
			switch (rowCount) {
				// first two cases are for drivers that conform to Statement.executeBatch specification
				case Statement.SUCCESS_NO_INFO:
					updatedRowCountSum++;
					break;
				case Statement.EXECUTE_FAILED:
					return Statement.EXECUTE_FAILED;
				default:	// 0 or really updated row count
					updatedRowCountSum += rowCount;
			}
		}
		return updatedRowCountSum;
	}
}
