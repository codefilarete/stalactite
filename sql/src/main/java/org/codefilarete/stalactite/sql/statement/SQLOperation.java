package org.codefilarete.stalactite.sql.statement;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.tool.bean.Objects;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tries to simplify usage of {@link PreparedStatement} in oriented scenario like:
 * - set values on {@link PreparedStatement}
 * - executeBatch {@link PreparedStatement} (see {@link WriteOperation})
 * 
 * Logging of SQL execution can be activated with a logger with this class name.
 * If you want more fine-grained logs, SQL statements can be logged with DEBUG level, whereas values can be logged with TRACE level.
 * <b>Despite that activation of fined grained logs defers by level, they are always logged at DEBUG level.</b> (which is not really consistent).
 * 
 * @see WriteOperation
 * @see ReadOperation
 * @param <ParamType> type of sqlStatement value entries, for example String (for {@link StringParamedSQL}), Integer (for {@link PreparedSQL}
 * 
 * @author Guillaume Mary
 */
public abstract class SQLOperation<ParamType> implements AutoCloseable {
	
	/** Made public for internal project usage, not aimed at being used outside */
	public static final Logger LOGGER = LoggerFactory.getLogger(SQLOperation.class);
	
	/** Listener that does nothing, made to prevent from not-null-matching if */
	public static final SQLOperationListener NOOP_LISTENER = new SQLOperationListener() {
		/* Expected to do nothing, so we do nothing */
	};
	
	protected final ConnectionProvider connectionProvider;
	
	protected PreparedStatement preparedStatement;
	
	protected final SQLStatement<ParamType> sqlStatement;
	
	private SQLOperationListener<ParamType> listener = NOOP_LISTENER;
	
	private String sql;
	
	/** Parameters that mustn't be logged for security reason for instance */
	private Set<ParamType> notLoggedParams = Collections.emptySet();
	
	/** Timeout for SQl orders, default is null meaning that JDBC default timeout applies, which is generally 0, which means no timeout */
	private Integer timeout = null;
	
	public SQLOperation(SQLStatement<ParamType> sqlStatement, ConnectionProvider connectionProvider) {
		this.sqlStatement = sqlStatement;
		this.connectionProvider = connectionProvider;
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}
	
	public SQLStatement<ParamType> getSqlStatement() {
		return sqlStatement;
	}
	
	public SQLOperationListener<ParamType> getListener() {
		return listener;
	}
	
	public void setListener(@Nullable SQLOperationListener<ParamType> listener) {
		this.listener = Objects.preventNull(listener, NOOP_LISTENER);
	}
	
	/**
	 * Simple wrapping over {@link SQLStatement#setValues(Map)}
	 * @param values values for each parameter
	 */
	public void setValues(Map<ParamType, ?> values) {
		this.listener.onValuesSet(values);
		// we transfer data to our own structure
		this.sqlStatement.setValues(values);
	}
	
	/**
	 * Simple wrapping over {@link SQLStatement#setValue(Object, Object)}
	 * 
	 * @param index parameter/index for which value mumst be set
	 * @param value parameter/index value
	 */
	public void setValue(ParamType index, Object value) {
		this.listener.onValueSet(index, value);
		this.sqlStatement.setValue(index, value);
	}
	
	/**
	 * Common operation for subclasses. Rebuild PreparedStatement if connection has changed. Call {@link #getSQL()} when
	 * necessary.
	 * 
	 * @throws SQLException in case of error during execution
	 */
	protected void ensureStatement() throws SQLException {
		Connection connection = this.connectionProvider.giveConnection();
		if (this.preparedStatement == null) {
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
	
	protected void prepareExecute() {
		listener.onExecute(sqlStatement);
		applyValuesToEnsuredStatement();
		logExecution();
		try {
			applyTimeout();
		} catch (SQLException e) {
			throw new SQLExecutionException(getSQL(), e);
		}
	}
	
	protected void applyValuesToEnsuredStatement() {
		try {
			ensureStatement();
		} catch (RuntimeException | SQLException e) {
			throw new BindingException("Error while creating statement " + getSQL(), e);
		}
		try {
			this.sqlStatement.applyValues(preparedStatement);
		} catch (RuntimeException e) {
			throw new BindingException("Error while applying values " + this.sqlStatement.values + " on statement " + getSQL(), e);
		}
	}
	
	/**
	 * Cancels the underlying {@link PreparedStatement} (if exists and not closed, to avoid unnecessary exceptions)
	 * 
	 * @throws SQLException this of the {@link PreparedStatement#cancel()} method
	 */
	public void cancel() throws SQLException {
		if (this.preparedStatement != null) {
			this.preparedStatement.cancel();
		}
	}
	
	/**
	 * Gives current {@link PreparedStatement}
	 * 
	 * @return current {@link PreparedStatement}, maybe null, closed, cancel ...
	 */
	@Nullable
	public PreparedStatement getPreparedStatement() {
		return preparedStatement;
	}
	
	/**
	 *
	 * @return null means default timeout applies, else the timeout set, 0 means infinite (see JDBC specification)
	 */
	public Integer getTimeout() {
		return timeout;
	}
	
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	
	/**
	 * Closes the internal {@link PreparedStatement}
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
	
	protected Map<ParamType, Object> filterLoggable(Map<ParamType, ?> values) {
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
	
	protected void applyTimeout() throws SQLException {
		if (getTimeout() != null) {
			this.preparedStatement.setQueryTimeout(getTimeout());
		}
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
	
	/**
	 * Contract to implement for being notified of actions on an {@link SQLOperation}
	 * Created for use cases where listening to SQL orders is necessary but auditing logs is not enough because you only get a String version of
	 * what is executed.
	 * <strong>Be aware that sensible values are not filtered</strong> at the opposite to logged ones hence you get same values passed to SQL
	 * order, DON'T LOG THEM. Hence this listener is not made to replace logging system.
	 * 
	 * @param <ParamType> type of the {@link SQLOperation} to be registered on
	 */
	public interface SQLOperationListener<ParamType> {
		
		/**
		 * Called when the {@link SQLOperation#setValues(Map)} is called.
		 * Please note that the given {@link Map} is writable which allow values to be modified, but this is not the primary goal on this method
		 * and is not an active feature and may be changed in the future. This is done so because making it unmodifiable needs a superfluous
		 * instantiation.
		 * Please note also that this behavior defers from {@link #onValueSet(Object, Object)} where the value is "readonly" : since it is passed
		 * by reference (Java language), simple (non complex) types are considered readonly.
		 * 
		 * <strong>Be aware that sensible values are not filtered</strong> at the opposit to logged ones hence you get same values passed to SQL
		 * order, DON'T LOG THEM.
		 * 
		 * @param values
		 */
		default void onValuesSet(Map<ParamType, ?> values) {
			// does nothing by default
		}
		
		/**
		 * Called when the {@link SQLOperation#setValue(Object, Object)} is called.
		 * <strong>Be aware that sensible values are not filtered</strong> at the opposit to logged ones hence you get same values passed to SQL
		 * order, DON'T LOG THEM.
		 * 
		 * @param param
		 * @param value
		 */
		default void onValueSet(ParamType param, Object value) {
			// does nothing by default
		}
		
		default void onExecute(SQLStatement<ParamType> sqlStatement) {
			
		}
	}
}
