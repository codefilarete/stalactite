package org.codefilarete.stalactite.sql.postgresql.test;

import javax.sql.DataSource;
import java.util.Map;

import org.codefilarete.stalactite.sql.test.TestDataSourceSelector;
import org.codefilarete.tool.Nullable;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * @author Guillaume Mary
 */
public class PostgreSQLTestDataSourceSelector extends TestDataSourceSelector {
	
	@Override
	protected boolean isExternalServiceConfigured(Map<String, String> properties) {
		return properties.get("postgresql.url") != null;
	}
	
	@Override
	protected DataSource buildDataSource(Map<String, String> properties) {
		String url = properties.get("postgresql.url");
		PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();
		pgSimpleDataSource.setUrl(url);
		Nullable.nullable(properties.get("postgresql.user")).invoke(pgSimpleDataSource::setUser);
		Nullable.nullable(properties.get("postgresql.password")).invoke(pgSimpleDataSource::setPassword);
		return pgSimpleDataSource;
	}
	
	@Override
	protected DataSource buildEmbeddedDataSource() {
		return new PostgreSQLEmbeddedDataSource(5431);
	}
}
