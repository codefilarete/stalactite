package org.gama.sql.dml;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.gama.sql.ConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tries to simplify usage of {@link PreparedStatement} in oriented scenarii like:
 * - set values on {@link PreparedStatement}
 * - executeBatch {@link PreparedStatement} (see {@link WriteOperation})
 * 
 * Logging of SQL execution can be activated with a logger with this class name.
 * If you want more fine grained logs, SQL statements can be logged with DEBUG level, whereas values can be logged with TRACE level.
 * <b>Despite that activation of fined grained logs defers by level, they are always logged at DEBUG level.</b> (which is not really consistent).
 * 
 * @see WriteOperation
 * @see ReadOperation
 * @param <ParamType> type of sqlStatement value entries, for example String (for {@link StringParamedSQL}), Integer (for {@link PreparedSQL}
 * 
 * @author Guillaume Mary
 */
public abstract class SQLOperation<ParamType> implements AutoCloseable {
	
	protected static final Logger LOGGER = LoggerFactory.getLogger(SQLOperation.class);
	
	protected final ConnectionProvider connectionProvider;
	
	protected PreparedStatement preparedStatement;
	
	protected final SQLStatement<ParamType> sqlStatement;
	
	private String sql;
	
	/** Parameters that mustn't be logged for security reason for instance */
	private Set<ParamType> notLoggedParams = Collections.emptySet();
	
	public SQLOperation(SQLStatement<ParamType> sqlStatement, ConnectionProvider connectionProvider) {
		this.sqlStatement = sqlStatement;
		this.connectionProvider = connectionProvider;
	}
	
	public SQLStatement<ParamType> getSqlStatement() {
		return sqlStatement;
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}
	
	/**
	 * Simple wrapping over {@link SQLStatement#setValues(Map)}
	 * @param values values for each parameter
	 */
	public void setValues(Map<ParamType, Object> values) {
		// we transfert data to our own structure
		this.sqlStatement.setValues(values);
	}
	
	/**
	 * Sets value for given parameter/index
	 * @param index parameter/index for which value mumst be set
	 * @param value parameter/index value
	 */
	public void setValue(ParamType index, Object value) {
		this.sqlStatement.setValue(index, value);
	}
	
	/**
	 * Common operation for subclasses. Rebuild PreparedStatement if connection has changed. Call {@link #getSQL()} when
	 * necessary.
	 * 
	 * @throws SQLException in case of error during execution
	 */
	protected void ensureStatement() throws SQLException {
		Connection connection = this.connectionProvider.getCurrentConnection();
		if (this.preparedStatement == null || this.preparedStatement.getConnection() != connection) {
			prepareStatement(connection);
		}
	}
	
	protected void prepareStatement(Connection connection) throws SQLException {
		this.preparedStatement = connection.prepareStatement(getSQL());
	}
	
	/**
	 * Gives the SQL that is used in the {@link PreparedStatement}.
	 * Called by {@link SQLStatement#getSQL()} then stores the result.
	 * 
	 * @return the SQL that is used in the {@link PreparedStatement}
	 */
	protected String getSQL() {
		if (this.sql == null) {
			this.sql = sqlStatement.getSQL();
		}
		return this.sql;
	}
	
	/**
	 * Cancels the underlying {@link PreparedStatement} (if exists and not closed, to avoid unecessary exceptions)
	 * 
	 * @throws SQLException this of the {@link PreparedStatement#cancel()} method
	 */
	public void cancel() throws SQLException {
		// avoid calling cancel() on a closed statement so only interesting exceptions will be thrown
		// but we let SQLFeatureNotSupportedException
		if (this.preparedStatement != null && !this.preparedStatement.isClosed()) {
			this.preparedStatement.cancel();
		}
	}
	
	/**
	 * Closes internal {@link PreparedStatement}
	 * 
	 * @throws SQLException this of the {@link PreparedStatement#close()} method
	 */
	@Override
	public void close() {
		try {
			this.preparedStatement.close();
		} catch (SQLException e) {
			LOGGER.warn("Can't close statement properly", e);
		}
	}
	
	/**
	 * Set params that mustn't be logged when debug is activated for values
	 * @param notLoggedParams set of not loggable values
	 */
	public void setNotLoggedParams(@Nonnull Set<ParamType> notLoggedParams) {
		this.notLoggedParams = notLoggedParams;
	}
	
	protected Map<ParamType, Object> filterLoggable(Map<ParamType, Object> values) {
		// we make a copy of values to prevent alteration
		Map<ParamType, Object> loggedValues = new HashMap<>(values);
		loggedValues.entrySet().forEach(e -> {
			if (notLoggedParams.contains(e.getKey())) {
				// we change logged value so param is still present in mapped params, showing that it hasn't desappeared
				e.setValue("X-masked value-X");
			}
		});
		return loggedValues;
	}
	
	protected void logExecution() {
		logExecution(() -> filterLoggable(sqlStatement.getValues()).toString());
	}
	
	protected void logExecution(Supplier<String> valuesSupplier) {
		// we log statement only in strict DEBUG mode because it's also logged in TRACE mode and DEBUG os also active at TRACE level
		if (LOGGER.isDebugEnabled() && !LOGGER.isTraceEnabled()) {
			LOGGER.debug(sqlStatement.getSQLSource());
		}
		if (LOGGER.isTraceEnabled()) {
			LOGGER.debug("{} | {}", sqlStatement.getSQLSource(), valuesSupplier.get());
		}
	}
}
