package org.codefilarete.stalactite.sql.test;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.tool.exception.Exceptions;

/**
 * A {@link DataSource} that wraps Oracle one to ease configuration.
 * Activates some parameters to get better performances. 
 * 
 * @author Guillaume Mary
 */
public class OracleDataSource extends UrlAwareDataSource {
	
	public OracleDataSource(String host, int port, String schema, String user, String password) {
		// Note that we use the service name form of the URL because for now this class is used in test with containers
		// which works with service name URL, not the SID form : "jdbc:oracle:thin:@" + host + ":" + port + ":" + schema
		super("jdbc:oracle:thin:@//" + host + ":" + port + "/" + schema);
		try {
			oracle.jdbc.datasource.impl.OracleDataSource oracleDataSource = new oracle.jdbc.datasource.impl.OracleDataSource();
			oracleDataSource.setURL(getUrl());
			oracleDataSource.setUser(user);
			oracleDataSource.setPassword(password);
			// apply properties to DataSource
			setDelegate(oracleDataSource);
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
}
