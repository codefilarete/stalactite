package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.DialectOptions;

/**
 * 
 * @author Guillaume Mary
 */
public class PersistenceContextConfigurationBuilder {
	
	protected final DatabaseVendorSettings vendorSettings;
	protected final ConnectionSettings connectionSettings;
	protected final DataSource dataSource;
	
	public PersistenceContextConfigurationBuilder(DatabaseVendorSettings vendorSettings, ConnectionSettings connectionSettings, DataSource dataSource) {
		this.vendorSettings = vendorSettings;
		this.connectionSettings = connectionSettings;
		this.dataSource = dataSource;
	}
	
	public PersistenceContextConfiguration build() {
		return build(DialectOptions.noOptions());
	}
	
	public PersistenceContextConfiguration build(DialectOptions dialectOptions) {
		Dialect dialect = new DialectBuilder(vendorSettings, dialectOptions).build();
		ConnectionConfiguration connectionConfiguration = buildConnectionConfiguration();
		return new PersistenceContextConfiguration(connectionConfiguration, dialect);
	}
	
	protected ConnectionConfiguration buildConnectionConfiguration() {
		return new ConnectionConfigurationSupport(
				new CurrentThreadTransactionalConnectionProvider(dataSource),
				connectionSettings.getBatchSize(),
				connectionSettings.getFetchSize());
	}
	
	/**
	 * Small class to store result of {@link PersistenceContextConfigurationBuilder#build()}.
	 * @author Guillaume Mary
	 */
	public static class PersistenceContextConfiguration {
		
		private final ConnectionConfiguration connectionConfiguration;
		private final Dialect dialect;
		
		public PersistenceContextConfiguration(ConnectionConfiguration connectionConfiguration, Dialect dialect) {
			this.connectionConfiguration = connectionConfiguration;
			this.dialect = dialect;
		}
		
		public Dialect getDialect() {
			return dialect;
		}
		
		public ConnectionConfiguration getConnectionConfiguration() {
			return connectionConfiguration;
		}
	}
}
