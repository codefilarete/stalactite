package org.gama.stalactite.sql.dml;

import java.sql.SQLException;

import org.gama.stalactite.sql.ConnectionProvider;

/**
 * @author Guillaume Mary
 */
public class HSQLDBWriteOperation<ParamType> extends WriteOperation<ParamType> {
	
	public HSQLDBWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener) {
		super(sqlGenerator, connectionProvider, rowCountListener);
	}
	
	/**
	 * Overriden to call executeUpdate() instead of executeLargeUpdate() because HSQLDB 2.3.2 doesn't support it
	 */
	@Override
	protected long doExecuteUpdate() throws SQLException {
		return preparedStatement.executeUpdate();
	}
	
	/**
	 * Overriden to call executeBatch() instead of executeLargeBatch() because HSQLDB 2.3.2 doesn't support it
	 */
	@Override
	protected long[] doExecuteBatch() throws SQLException {
		int[] updatedRowCounts = preparedStatement.executeBatch();
		long[] updatedRowCountsAsLong = new long[updatedRowCounts.length];
		for (int i = 0; i < updatedRowCounts.length; i++) {
			updatedRowCountsAsLong[i] = updatedRowCounts[i];
		}
		return updatedRowCountsAsLong;
	}
}
