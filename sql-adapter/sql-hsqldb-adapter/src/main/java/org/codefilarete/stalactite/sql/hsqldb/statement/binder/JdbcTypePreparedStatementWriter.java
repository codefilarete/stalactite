package org.codefilarete.stalactite.sql.hsqldb.statement.binder;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;

/**
 * A class that uses {@link PreparedStatement#setObject(int, Object)} to write values to a statement.
 * Made for cases when database vendor supports particular types only through setObject method.
 * 
 * @param <T> the java class mapped to a database column
 * @author Guillaume Mary
 * @see JdbcTypeResultSetReader
 */
public class JdbcTypePreparedStatementWriter<T> implements PreparedStatementWriter<T> {
	
	private final Class<T> type;
	
	private final JDBCType jdbcType;
	
	public JdbcTypePreparedStatementWriter(Class<T> type, JDBCType jdbcType) {
		this.type = type;
		this.jdbcType = jdbcType;
	}
	
	@Override
	public void set(PreparedStatement preparedStatement, int valueIndex, T value) throws SQLException {
		preparedStatement.setObject(valueIndex, value, jdbcType);
	}
	
	@Override
	public Class<T> getType() {
		return type;
	}
}
