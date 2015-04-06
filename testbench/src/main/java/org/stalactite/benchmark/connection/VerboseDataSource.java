package org.stalactite.benchmark.connection;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * @author Guillaume Mary
 */
public class VerboseDataSource implements DataSource {
	
	private final String url;
	private final Map<String, String> properties;
	private final DataSource delegate;
	
	VerboseDataSource(String url, Map<String, String> properties, DataSource delegate) {
		this.url = url;
		this.properties = properties;
		this.delegate = delegate;
	}
	
	public String getUrl() {
		return url;
	}
	
	public Map<String, String> getProperties() {
		return properties;
	}
	
	@Override
	public Connection getConnection() throws SQLException {
		return delegate.getConnection();
	}
	
	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		return delegate.getConnection(username, password);
	}
	
	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return delegate.getLogWriter();
	}
	
	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {
		delegate.setLogWriter(out);
	}
	
	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
		delegate.setLoginTimeout(seconds);
	}
	
	@Override
	public int getLoginTimeout() throws SQLException {
		return delegate.getLoginTimeout();
	}
	
	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return delegate.getParentLogger();
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return delegate.unwrap(iface);
	}
	
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return delegate.isWrapperFor(iface);
	}
}
