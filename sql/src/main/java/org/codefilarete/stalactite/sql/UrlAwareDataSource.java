package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;

import org.codefilarete.tool.sql.DataSourceWrapper;

/**
 * @author Guillaume Mary
 */
public class UrlAwareDataSource extends DataSourceWrapper {
	
	private final String url;
	
	public UrlAwareDataSource(String url, DataSource delegate) {
		this.url = url;
		setDelegate(delegate);
	}
	
	protected UrlAwareDataSource(String url) {
		this(url, null);
	}
	
	public String getUrl() {
		return url;
	}
	
	@Override
	public String toString() {
		return getUrl();
	}
}
