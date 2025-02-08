package org.codefilarete.stalactite.spring.repository.config;

import org.codefilarete.stalactite.engine.ConnectionSettings;
import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.PersistenceContextConfigurationBuilder;
import org.codefilarete.stalactite.spring.transaction.StalactitePlatformTransactionManager;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Dedicated {@link PersistenceContextConfigurationBuilder} for Spring Data: creates a {@link StalactitePlatformTransactionManager} as
 * {@link org.codefilarete.stalactite.sql.ConnectionProvider} of the built {@link ConnectionConfiguration}
 * 
 * @author Guillaume Mary
 */
public class SpringDataPersistenceContextConfigurationBuilder extends PersistenceContextConfigurationBuilder {
	
	public SpringDataPersistenceContextConfigurationBuilder(DatabaseVendorSettings vendorSettings, ConnectionSettings connectionSettings) {
		super(vendorSettings, connectionSettings);
	}
	
	/**
	 * Overridden to create a {@link StalactitePlatformTransactionManager} as {@link org.codefilarete.stalactite.sql.ConnectionProvider} of the
	 * result.
	 * @return a {@link ConnectionConfiguration} made of a {@link StalactitePlatformTransactionManager} as its {@link org.codefilarete.stalactite.sql.ConnectionProvider}
	 */
	@Override
	protected ConnectionConfiguration buildConnectionConfiguration() {
		return new PlatformTransactionManagerConnectionConfigurationSupport(new StalactitePlatformTransactionManager(super.connectionSettings.getDataSource()), super.connectionSettings.getBatchSize());
	}
	
	static class PlatformTransactionManagerConnectionConfigurationSupport extends ConnectionConfigurationSupport {
		
		
		private final PlatformTransactionManager platformTransactionManager;
		
		public PlatformTransactionManagerConnectionConfigurationSupport(StalactitePlatformTransactionManager connectionProvider, int batchSize) {
			super(connectionProvider, batchSize);
			this.platformTransactionManager = connectionProvider;
		}
		
		public PlatformTransactionManager getPlatformTransactionManager() {
			return platformTransactionManager;
		}
	}
}
