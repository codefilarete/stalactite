package org.gama.sql;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * {@link Connection} that wraps another one and delegates all its methods to it without any additionnal feature.
 * Made for overriding only some targeted methods.
 * 
 * @author Guillaume Mary
 */
public class ConnectionWrapper implements Connection {

	private Connection surrogate;
	
	public ConnectionWrapper() {
	}
	
	public ConnectionWrapper(Connection surrogate) {
		this.surrogate = surrogate;
	}
	
	public void setSurrogate(Connection surrogate) {
		this.surrogate = surrogate;
	}
	
	@Override
	public Statement createStatement() throws SQLException {
		return surrogate.createStatement();
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return surrogate.prepareStatement(sql);
	}
	
	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return surrogate.prepareCall(sql);
	}
	
	@Override
	public String nativeSQL(String sql) throws SQLException {
		return surrogate.nativeSQL(sql);
	}
	
	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		surrogate.setAutoCommit(autoCommit);
	}
	
	@Override
	public boolean getAutoCommit() throws SQLException {
		return surrogate.getAutoCommit();
	}
	
	@Override
	public void commit() throws SQLException {
		surrogate.commit();
	}
	
	@Override
	public void rollback() throws SQLException {
		surrogate.rollback();
	}
	
	@Override
	public void close() throws SQLException {
		surrogate.close();
	}
	
	@Override
	public boolean isClosed() throws SQLException {
		return surrogate.isClosed();
	}
	
	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return surrogate.getMetaData();
	}
	
	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		surrogate.setReadOnly(readOnly);
	}
	
	@Override
	public boolean isReadOnly() throws SQLException {
		return surrogate.isReadOnly();
	}
	
	@Override
	public void setCatalog(String catalog) throws SQLException {
		surrogate.setCatalog(catalog);
	}
	
	@Override
	public String getCatalog() throws SQLException {
		return surrogate.getCatalog();
	}
	
	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		surrogate.setTransactionIsolation(level);
	}
	
	@Override
	public int getTransactionIsolation() throws SQLException {
		return surrogate.getTransactionIsolation();
	}
	
	@Override
	public SQLWarning getWarnings() throws SQLException {
		return surrogate.getWarnings();
	}
	
	@Override
	public void clearWarnings() throws SQLException {
		surrogate.clearWarnings();
	}
	
	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return surrogate.createStatement(resultSetType, resultSetConcurrency);
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return surrogate.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}
	
	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return surrogate.prepareCall(sql, resultSetType, resultSetConcurrency);
	}
	
	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return surrogate.getTypeMap();
	}
	
	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		surrogate.setTypeMap(map);
	}
	
	@Override
	public void setHoldability(int holdability) throws SQLException {
		surrogate.setHoldability(holdability);
	}
	
	@Override
	public int getHoldability() throws SQLException {
		return surrogate.getHoldability();
	}
	
	@Override
	public Savepoint setSavepoint() throws SQLException {
		return surrogate.setSavepoint();
	}
	
	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return surrogate.setSavepoint(name);
	}
	
	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		surrogate.rollback(savepoint);
	}
	
	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		surrogate.releaseSavepoint(savepoint);
	}
	
	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return surrogate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws 
			SQLException {
		return surrogate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}
	
	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return surrogate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		return surrogate.prepareStatement(sql, autoGeneratedKeys);
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		return surrogate.prepareStatement(sql, columnIndexes);
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		return surrogate.prepareStatement(sql, columnNames);
	}
	
	@Override
	public Clob createClob() throws SQLException {
		return surrogate.createClob();
	}
	
	@Override
	public Blob createBlob() throws SQLException {
		return surrogate.createBlob();
	}
	
	@Override
	public NClob createNClob() throws SQLException {
		return surrogate.createNClob();
	}
	
	@Override
	public SQLXML createSQLXML() throws SQLException {
		return surrogate.createSQLXML();
	}
	
	@Override
	public boolean isValid(int timeout) throws SQLException {
		return surrogate.isValid(timeout);
	}
	
	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		surrogate.setClientInfo(name, value);
	}
	
	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		surrogate.setClientInfo(properties);
	}
	
	@Override
	public String getClientInfo(String name) throws SQLException {
		return surrogate.getClientInfo(name);
	}
	
	@Override
	public Properties getClientInfo() throws SQLException {
		return surrogate.getClientInfo();
	}
	
	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return surrogate.createArrayOf(typeName, elements);
	}
	
	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return surrogate.createStruct(typeName, attributes);
	}
	
	@Override
	public void setSchema(String schema) throws SQLException {
		surrogate.setSchema(schema);
	}
	
	@Override
	public String getSchema() throws SQLException {
		return surrogate.getSchema();
	}
	
	@Override
	public void abort(Executor executor) throws SQLException {
		surrogate.abort(executor);
	}
	
	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		surrogate.setNetworkTimeout(executor, milliseconds);
	}
	
	@Override
	public int getNetworkTimeout() throws SQLException {
		return surrogate.getNetworkTimeout();
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return surrogate.unwrap(iface);
	}
	
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return surrogate.isWrapperFor(iface);
	}
}
