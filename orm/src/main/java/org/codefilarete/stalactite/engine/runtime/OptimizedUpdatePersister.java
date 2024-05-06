package org.codefilarete.stalactite.engine.runtime;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.NotYetSupportedOperationException;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.sql.CommitListener;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.RollbackListener;
import org.codefilarete.stalactite.sql.RollbackObserver;
import org.codefilarete.stalactite.sql.TransactionAwareConnectionProvider;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.result.NoopPreparedStatement;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.SQLOperation;
import org.codefilarete.tool.Experimental;
import org.codefilarete.tool.ThreadLocals;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.bean.ClassIterator;
import org.codefilarete.tool.bean.InterfaceIterator;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.PairIterator;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.ThrowingTriConsumer;
import org.codefilarete.tool.sql.ConnectionWrapper;
import org.codefilarete.tool.sql.ResultSetWrapper;

/**
 * Persister with optimized {@link #update(Object, Consumer)} method by leveraging an internal cache so only one select is really executed. 
 * Acts as a proxy over a delegate {@link ConfiguredRelationalPersister persister} to enhance its {@link #update(Object, Consumer)}
 * method.
 * <strong>
 * It requires that given {@link ConfiguredRelationalPersister} uses a {@link CachingQueryConnectionProvider}, this is done at build time
 * ({@link PersisterBuilderImpl}) by calling {@link #wrapWithQueryCache(ConnectionConfiguration)}.
 * </strong>
 * 
 * @author Guillaume Mary
 */
public class OptimizedUpdatePersister<C, I> extends PersisterWrapper<C, I> {
	
	@VisibleForTesting
	static final ThreadLocal<Map<ResultSetCacheKey, ResultSet>> CURRENT_QUERY = new ThreadLocal<>();
	
	/**
	 * Creates a new {@link ConnectionConfiguration} from given one and wraps its {@link ConnectionProvider} with one that caches select queries
	 * on demand of {@link OptimizedUpdatePersister} instances.
	 * Hence given {@link ConnectionConfiguration} should not be used anymore by caller.
	 * 
	 * @param connectionConfiguration the configuration to be wrapped with cache over its {@link ConnectionProvider}
	 * @return a new {@link ConnectionConfiguration} with enhanced {@link ConnectionProvider}
	 */
	public static ConnectionConfiguration wrapWithQueryCache(ConnectionConfiguration connectionConfiguration) {
		ConnectionProvider delegate = connectionConfiguration.getConnectionProvider();
		CachingQueryConnectionProvider cachingQueryConnectionProvider = new CachingQueryConnectionProvider(delegate);
		// We create a proxy that will redirect ConnectionProvider#giveConnection to the caching one (then queries will be cached) and
		// leave other methods invoked on original provider 
		// NB : we use a Set to avoid error thrown by Proxy.newProxyInstance when an interface is present several time
		Set<Class> interfaces = new HashSet<>(Iterables.copy(new InterfaceIterator(new ClassIterator(delegate.getClass(), null))));
		ConnectionProvider connectionProvider = (ConnectionProvider) Proxy.newProxyInstance(delegate.getClass().getClassLoader(), interfaces.toArray(new Class[0]),
				(proxy, method, args) -> {
					if (!method.getName().equals("giveConnection")) {
						return method.invoke(delegate, args);
					}
					return cachingQueryConnectionProvider.giveConnection();
				});
		return new ConnectionConfigurationSupport(connectionProvider, connectionConfiguration.getBatchSize());
	}
	
	public OptimizedUpdatePersister(ConfiguredRelationalPersister<C, I> surrogate) {
		super(surrogate);
	}
	
	/**
	 * Implementation that optimizes second entity loading by caching {@link ResultSet} and reuses it to create a clone of first entity.
	 * One may ask why not simply use any cloning algorithm / framework for such a use case. The answer is compounded in two reasons:
	 * - because Stalactite allows to build an entity from constructor with arguments, which hardly allows a cloning framework (actually not found any)
	 * - because Stalactite configuration is mainly made by method reference which even more reduces cloning-framework choice, meaning being closely
	 * tied to one for bugs and features  
	 * 
	 * @param id key of entity to be modified 
	 * @param entityConsumer business code expected to modify its given entity
	 */
	@Experimental
	@Override
	public void update(I id, Consumer<C> entityConsumer) {
		update(Collections.singleton(id), entityConsumer);
	}
	
	/**
	 * Implementation that optimizes entity loading by caching {@link ResultSet} and reuses it to create clones.
	 * One may ask why not simply use any cloning algorithm / framework for such a use case. The answer is compounded in two reasons:
	 * - because Stalactite allows to build an entity from constructor with arguments, which hardly allows a cloning framework (actually not found any)
	 * - because Stalactite configuration is mainly made by method reference which even more reduces cloning-framework choice, meaning being closely
	 * tied to one for bugs and features  
	 *
	 * @param ids keys of entities to be modified 
	 * @param entityConsumer business code expected to modify its given entity
	 */
	@Experimental
	@Override
	public void update(Iterable<I> ids, Consumer<C> entityConsumer) {
		Holder<Set<C>> referenceEntity = new Holder<>();
		Holder<Set<C>> entityToModify = new Holder<>();
		ThreadLocals.doWithThreadLocal(CURRENT_QUERY, HashMap::new, (Runnable) () -> {
			// Thanks to query cache this first select will be executed and its result put into QUERY_CACHE
			referenceEntity.set(select(ids));
			// Thanks to query cache, this second (and same) select won't be executed and it allows to get a copy of first entity 
			entityToModify.set(select(ids));
		});
		entityToModify.get().forEach(entityConsumer);
		update(() -> new PairIterator<>(entityToModify.get(), referenceEntity.get()), true);
	}
	
	/**
	 * Key for SQL Select statement (in the context of {@link OptimizedUpdatePersister})
	 */
	@VisibleForTesting
	static class ResultSetCacheKey {
		private final String sql;
		private final Map<Integer, Object> values = new HashMap<>();
		private final Map<Integer, ThrowingTriConsumer<PreparedStatement, Integer, Object, SQLException>> writers = new HashMap<>();
		
		@VisibleForTesting
		ResultSetCacheKey(String sql) {
			this.sql = sql;
		}
		
		private Map<Integer, Object> getValues() {
			return values;
		}
		
		private <T> void setValue(Integer index, Object value, ThrowingTriConsumer<PreparedStatement, Integer, T, SQLException> writer) {
			this.values.put(index, value);
			this.writers.put(index, (ThrowingTriConsumer<PreparedStatement, Integer, Object, SQLException>) writer);
		}
		
		public Map<Integer, ThrowingTriConsumer<PreparedStatement, Integer, Object, SQLException>> getWriters() {
			return writers;
		}
		
		/**
		 * Implementation based on sql and its arguments because we're an SQL statement cache key
		 */
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof ResultSetCacheKey)) return false;
			
			ResultSetCacheKey that = (ResultSetCacheKey) o;
			
			if (!sql.equals(that.sql)) return false;
			return values.equals(that.values);
		}
		
		/**
		 * Implementation based on sql and its arguments because we're an SQL statement cache key
		 */
		@Override
		public int hashCode() {
			int result = sql.hashCode();
			result = 31 * result + values.hashCode();
			return result;
		}
	}
	
	/**
	 * {@link ConnectionProvider} that proxies {@link Connection} of a delegate {@link ConnectionProvider} to add caching algorithm on select queries.
	 * Spied method is {@link Connection#prepareStatement(String)} and {@link PreparedStatement#executeQuery()} to match {@link org.codefilarete.stalactite.sql.statement.ReadOperation}
	 * algorithm.
	 * 
	 * It implements {@link RollbackObserver} to match contract expected by {@link org.codefilarete.stalactite.engine.runtime.AbstractRevertOnRollbackMVCC}
	 */
	private static class CachingQueryConnectionProvider implements ConnectionProvider {
		
		private final ConnectionProvider delegate;
		private CachingQueryConnectionWrapper currentConnection;
		
		private CachingQueryConnectionProvider(ConnectionProvider delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public Connection giveConnection() {
			try {
				if (currentConnection == null || currentConnection.isClosed()) {
					currentConnection = new CachingQueryConnectionWrapper(delegate.giveConnection());
					if (delegate instanceof TransactionAwareConnectionProvider) {
						((TransactionAwareConnectionProvider) delegate).addCommitListener(new CommitListener() {
							@Override
							public void beforeCommit() {
								
							}
							
							@Override
							public void afterCommit() {
								releaseCurrentConnection();
							}
						});
						((TransactionAwareConnectionProvider) delegate).addRollbackListener(new RollbackListener() {
							@Override
							public void beforeRollback() {
								
							}
							
							@Override
							public void afterRollback() {
								releaseCurrentConnection();
							}
							
							@Override
							public void beforeRollback(Savepoint savepoint) {
								
							}
							
							@Override
							public void afterRollback(Savepoint savepoint) {
								releaseCurrentConnection();
							}
						});
					}
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			return currentConnection;
		}
		
		private void releaseCurrentConnection() {
			this.currentConnection = null;
		}
	}
	
	/**
	 * {@link ConnectionWrapper} that caches queries executed by given {@link Connection}, expecting it creates them from
	 * {@link Connection#prepareStatement(String)} and then {@link PreparedStatement#executeQuery()}.
	 * Therefore it is tightly tied to {@link SQLOperation#prepareStatement(Connection)} because it spies same {@link Connection} method
	 * that creates {@link PreparedStatement} : {@link Connection#prepareStatement(String)}, hence it can keep track of all executed SQL. Nevertheless
	 * it only looks at {@link PreparedStatement#executeQuery()} to watch select statement, because it's the only purpose of this class.
	 * 
	 */
	private static class CachingQueryConnectionWrapper extends ConnectionWrapper {
		
		private CachingQueryConnectionWrapper(Connection surrogate) {
			super(surrogate);
		}
		
		@Override
		public PreparedStatement prepareStatement(String sql) throws SQLException {
			if (CURRENT_QUERY.get() == null) {
				// No cache active so we let default behavior
				return super.prepareStatement(sql);
			} else {
				return new SpyingQueryPreparedStatement(sql);
			}
		}
		
		/**
		 * {@link ResultSet} that stores in memory data of a delegate {@link ResultSet} : acts as a proxy over it.
		 * Only read data are cached (to avoid reading {@link java.sql.ResultSetMetaData} which causes performance issue) and only read rows, hence
		 * calling {@link #next()} and getXXX() methods is necessary to fill cache. 
		 */
		private static class CachingResultSet extends ResultSetWrapper {
			
			private final List<Map<String, Object>> inMemoryValues = new ArrayList<>();
			
			private final ResultSetCacheKey resultSetCacheKey;
			private Map<String, Object> rowContent;
			private final Map<ResultSetCacheKey, ResultSet> cache;
			
			private CachingResultSet(ResultSet resultSet, ResultSetCacheKey resultSetCacheKey, Map<ResultSetCacheKey, ResultSet> cache) {
				super(resultSet);
				this.resultSetCacheKey = resultSetCacheKey;
				this.cache = cache;
			}
			
			/**
			 * Overridden to put data in cache at the very end of iteration (when <pre>super.next()</pre> returns false)
			 * @return false when there's no more row to read
			 * @throws SQLException if a database access error occurs or this method is called on a closed result set
			 */
			@Override
			public boolean next() throws SQLException {
				boolean next = super.next();
				if (next) {
					// end of ResultSet still not reached, we "open" a new row so getXXX methods can put values in it  
					rowContent = new TreeMap<>();
					inMemoryValues.add(rowContent);
				} else {
					// we've reached last row so we can put result into cache
					cache.put(resultSetCacheKey, new InMemoryResultSet(inMemoryValues));
				}
				return next;
			}
			
			@Override
			public String getString(String columnLabel) throws SQLException {
				String value = delegate.getString(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public boolean getBoolean(String columnLabel) throws SQLException {
				boolean value = delegate.getBoolean(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public byte getByte(String columnLabel) throws SQLException {
				byte value = delegate.getByte(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public short getShort(String columnLabel) throws SQLException {
				short value = delegate.getShort(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public int getInt(String columnLabel) throws SQLException {
				int value = delegate.getInt(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public long getLong(String columnLabel) throws SQLException {
				long value = delegate.getLong(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public float getFloat(String columnLabel) throws SQLException {
				float value = delegate.getFloat(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public double getDouble(String columnLabel) throws SQLException {
				double value = delegate.getDouble(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public byte[] getBytes(String columnLabel) throws SQLException {
				byte[] value = delegate.getBytes(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public Date getDate(String columnLabel) throws SQLException {
				Date value = delegate.getDate(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public Time getTime(String columnLabel) throws SQLException {
				Time value = delegate.getTime(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public Timestamp getTimestamp(String columnLabel) throws SQLException {
				Timestamp value = delegate.getTimestamp(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public Object getObject(String columnLabel) throws SQLException {
				Object value = delegate.getObject(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
				BigDecimal value = delegate.getBigDecimal(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public URL getURL(String columnLabel) throws SQLException {
				URL value = delegate.getURL(columnLabel);
				rowContent.put(columnLabel, value);
				return value;
			}
			
			@Override
			public Blob getBlob(String columnLabel) throws SQLException {
				//return surrogate.getBlob(columnLabel);
				throw new NotYetSupportedOperationException("Result will not be readable twice");
			}
			
			@Override
			public Clob getClob(String columnLabel) throws SQLException {
				//return surrogate.getClob(columnLabel);
				throw new NotYetSupportedOperationException("Result will not be readable twice");
			}
			
			@Override
			public InputStream getBinaryStream(String columnLabel) throws SQLException {
				//return surrogate.getBinaryStream(columnLabel);
				throw new NotYetSupportedOperationException("Result will not be readable twice");
			}
			
			@Override
			public Reader getCharacterStream(String columnLabel) throws SQLException {
				//return surrogate.getCharacterStream(columnLabel);
				throw new NotYetSupportedOperationException("Result will not be readable twice");
			}
			
			@Override
			public InputStream getAsciiStream(String columnLabel) throws SQLException {
				//return surrogate.getAsciiStream(columnLabel);
				throw new NotYetSupportedOperationException("Result will not be readable twice");
			}
		}
		
		/**
		 * Fake {@link PreparedStatement} that will put result of {@link #executeQuery()} into cache with its parameters.
		 * @see #executeQuery()
		 * @see #executeQueryAndCacheResult(String, ResultSetCacheKey) 
		 */
		private class SpyingQueryPreparedStatement extends NoopPreparedStatement {
			private final String sql;
			private final ResultSetCacheKey resultSetCacheKey;
			
			private SpyingQueryPreparedStatement(String sql) {
				this.sql = sql;
				this.resultSetCacheKey = new ResultSetCacheKey(sql);
			}
			
			@Override
			public ResultSet executeQuery() throws SQLException {
				return executeQueryAndCacheResult(sql, resultSetCacheKey);
			}
			
			private ResultSet executeQueryAndCacheResult(String sql, ResultSetCacheKey resultSetCacheKey) throws SQLException {
				Map<ResultSetCacheKey, ResultSet> resultSetCache = CURRENT_QUERY.get();
				ResultSet previousResult = resultSetCache.get(resultSetCacheKey);
				if (previousResult != null) {
					// we trace cache usage in log to prevent user from becoming crazy by not seeing any real call to RDBMS
					SQLOperation.LOGGER.debug("Result found in cache, statement will not be executed");
					return previousResult;
				} else {
					PreparedStatement realStatement = CachingQueryConnectionWrapper.super.prepareStatement(sql);
					resultSetCacheKey.getValues().forEach((index, value) -> {
						try {
							resultSetCacheKey.getWriters().get(index).accept(realStatement, index, value);
						} catch (SQLException throwable) {
							throw new SQLExecutionException(throwable);
						}
					});
					return new CachingResultSet(realStatement.executeQuery(), resultSetCacheKey, resultSetCache);
				}
			}
			
			@Override
			public void setArray(int parameterIndex, Array value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setArray);
			}
			
			@Override
			public void setBigDecimal(int parameterIndex, BigDecimal value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setBigDecimal);
			}
			
			@Override
			public void setBoolean(int parameterIndex, boolean value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setBoolean);
			}
			
			@Override
			public void setByte(int parameterIndex, byte value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setByte);
			}
			
			@Override
			public void setBytes(int parameterIndex, byte[] value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setBytes);
			}
			
			@Override
			public void setDate(int parameterIndex, Date value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, (ps, index, x) -> ps.setDate(index, (Date) x));
			}
			
			@Override
			public void setDate(int parameterIndex, Date value, Calendar cal) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, (ps, index, x) -> ps.setDate(index, (Date) x, cal));
			}
			
			@Override
			public void setDouble(int parameterIndex, double value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setDouble);
			}
			
			@Override
			public void setFloat(int parameterIndex, float value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setFloat);
			}
			
			@Override
			public void setInt(int parameterIndex, int value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setInt);
			}
			
			@Override
			public void setLong(int parameterIndex, long value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setLong);
			}
			
			@Override
			public void setNull(int parameterIndex, int sqlType) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, null, (ps, index, x) -> ps.setNull(index, sqlType));
			}
			
			@Override
			public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, null, (ps, index, x) -> ps.setNull(index, sqlType, typeName));
			}
			
			@Override
			public void setObject(int parameterIndex, Object value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setObject);
			}
			
			@Override
			public void setShort(int parameterIndex, short value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setShort);
			}
			
			@Override
			public void setString(int parameterIndex, String value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setString);
			}
			
			@Override
			public void setTime(int parameterIndex, Time value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, (ps, index, x) -> ps.setTime(index, (Time) x));
			}
			
			@Override
			public void setTime(int parameterIndex, Time value, Calendar cal) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, (ps, index, x) -> ps.setTime(index, (Time) x, cal));
			}
			
			@Override
			public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, (ps, index, x) -> ps.setTimestamp(index, (Timestamp) x));
			}
			
			@Override
			public void setTimestamp(int parameterIndex, Timestamp value, Calendar cal) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, (ps, index, x) -> ps.setTimestamp(index, (Timestamp) x, cal));
			}
			
			@Override
			public void setURL(int parameterIndex, URL value) throws SQLException {
				resultSetCacheKey.setValue(parameterIndex, value, PreparedStatement::setURL);
			}
				
			/*
				All following parameters are not considered to make sense in a SQL Select where clause so are not spied
				setAsciiStream
				setBinaryStream
				setBlob
				setCharacterStream
				setClob
				setNCharacterStream
				setNClob
				setNString
				setRef
				setRowId
			 */
		}
	}
}
