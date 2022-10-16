package org.codefilarete.stalactite.sql.test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

import org.codefilarete.tool.Nullable;
import org.mariadb.jdbc.MariaDbDataSource;

/**
 * @author Guillaume Mary
 */
public class MariaDBTestDataSourceSelector extends TestDataSourceSelector {
	
	@Override
	protected boolean isExternalServiceConfigured(Map<String, String> properties) {
		return properties.get("mariadb.url") != null;
	}
	
	@Override
	protected DataSource buildDataSource(Map<String, String> properties) {
		String url = properties.get("mariadb.url");
		MariaDbDataSource mariaDbDataSource;
		try {
			mariaDbDataSource = new MariaDbDataSource(url);
			Nullable.nullable(properties.get("mariadb.user")).invokeThrower(mariaDbDataSource::setUser);
			Nullable.nullable(properties.get("mariadb.password")).invokeThrower(mariaDbDataSource::setPassword);
		} catch (SQLException e) {
			throw new RuntimeException("Can't build MariaDB DataSource from url " + url, e);
		}
		return mariaDbDataSource;
	}
	
	@Override
	protected DataSource buildEmbeddedDataSource() {
		return new MariaDBEmbeddableDataSource(3406);
	}
}
