package org.codefilarete.stalactite.sql.test;

import javax.sql.DataSource;
import java.util.Map;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.codefilarete.tool.Nullable;

/**
 * @author Guillaume Mary
 */
public class MySQLTestDataSourceSelector extends TestDataSourceSelector {
	
	@Override
	protected boolean isExternalServiceConfigured(Map<String, String> properties) {
		return properties.get("mysql.url") != null;
	}
	
	@Override
	protected DataSource buildDataSource(Map<String, String> properties) {
		String url = properties.get("mysql.url");
		MysqlDataSource mysqlDataSource = new MysqlDataSource();
		mysqlDataSource.setUrl(url);
		Nullable.nullable(properties.get("mysql.user")).invoke(mysqlDataSource::setUser);
		Nullable.nullable(properties.get("mysql.password")).invoke(mysqlDataSource::setPassword);
		return mysqlDataSource;
	}
	
	@Override
	protected DataSource buildEmbeddedDataSource() {
		return new MySQLEmbeddableDataSource(3406);
	}
}