package org.gama.sql;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * {@link DataSource} that wraps another one and delegates all its methods to it without any additionnal feature.
 * Made for overriding only some targeted methods.
 *
 * @author Guillaume Mary
 */
public class DataSourceWrapper implements DataSource {
	
	private DataSource delegate;
	
	public DataSourceWrapper() {
	}
	
	public void setDelegate(DataSource delegate) {
		this.delegate = delegate;
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
