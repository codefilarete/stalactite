package org.codefilarete.stalactite.sql.oracle.test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

import oracle.jdbc.datasource.impl.OracleDataSource;
import org.codefilarete.stalactite.sql.test.TestDataSourceSelector;
import org.codefilarete.tool.Nullable;

/**
 * @author Guillaume Mary
 */
public class OracleTestDataSourceSelector extends TestDataSourceSelector {
	
	@Override
	protected boolean isExternalServiceConfigured(Map<String, String> properties) {
		return properties.get("oracle.url") != null;
	}
	
	@Override
	protected DataSource buildDataSource(Map<String, String> properties) {
		String url = properties.get("oracle.url");
		try {
			OracleDataSource oracleDataSource = new OracleDataSource();
			oracleDataSource.setURL(url);
			Nullable.nullable(properties.get("oracle.user")).invoke(oracleDataSource::setUser);
			Nullable.nullable(properties.get("oracle.password")).invoke(oracleDataSource::setPassword);
			return oracleDataSource;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	protected DataSource buildEmbeddedDataSource() {
		return new OracleEmbeddableDataSource();
	}
}
