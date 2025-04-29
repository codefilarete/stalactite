package org.codefilarete.stalactite.spring.autoconfigure;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.codefilarete.stalactite.engine.ConnectionSettings;
import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.spring.autoconfigure.StalactiteRepositoriesAutoConfiguration.StalactiteRepositoriesImportSelector;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.codefilarete.stalactite.spring.repository.StalactiteRepositoryFactoryBean;
import org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.spring.repository.config.SpringDataPersistenceContextConfigurationBuilder;
import org.codefilarete.stalactite.spring.transaction.StalactitePlatformTransactionManager;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.DialectResolver;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.task.TaskExecutionAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

import static org.codefilarete.stalactite.engine.PersistenceContextConfigurationBuilder.PersistenceContextConfiguration;
import static org.codefilarete.tool.Nullable.nullable;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Spring Data's Stalactite Repositories.
 * <p>
 * Activates when there is a bean of type {@link javax.sql.DataSource} configured in the
 * context, the Spring Data Stalactite {@link StalactiteRepository} type is on the classpath, and there
 * is no other, existing {@link StalactiteRepository} configured.
 * <p>
 * Once in effect, the auto-configuration is the equivalent of enabling Stalactite repositories
 * using the {@link EnableStalactiteRepositories @EnableStalactiteRepositories} annotation.
 * <p>
 *
 * @author Guillaume Mary
 * @see org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories
 */
@AutoConfiguration(after = { StalactiteAutoConfiguration.class, TaskExecutionAutoConfiguration.class })
@ConditionalOnBean(value = { DataSource.class })
@ConditionalOnClass(StalactiteRepository.class)
@ConditionalOnMissingBean({ StalactiteRepositoryFactoryBean.class })
@ConditionalOnProperty(prefix = "spring.data.stalactite.repositories", name = "enabled", havingValue = "true", matchIfMissing = true)
@Import(StalactiteRepositoriesImportSelector.class)
public class StalactiteRepositoriesAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public PersistenceContext persistenceContext(PersistenceContextConfiguration persistenceContextConfiguration, Dialect dialect) {
		return new PersistenceContext(persistenceContextConfiguration.getConnectionConfiguration(), dialect);
	}
	
	/**
	 * Creates a {@link SpringDataPersistenceContextConfigurationBuilder} and returns its {@link PersistenceContextConfiguration}, hence making
	 * available a {@link StalactitePlatformTransactionManager} through <code>persistenceContextConfiguration.getConnectionConfiguration().getConnectionProvider()</code>
	 * 
	 * @param databaseVendorSettings
	 * @param connectionSettings
	 * @return
	 */
	@Bean
	@ConditionalOnMissingBean
	public PersistenceContextConfiguration persistenceContextConfigurationBuilder(DatabaseVendorSettings databaseVendorSettings, ConnectionSettings connectionSettings, DataSource dataSource) {
		SpringDataPersistenceContextConfigurationBuilder persistenceContextConfigurationBuilder = new SpringDataPersistenceContextConfigurationBuilder(databaseVendorSettings, connectionSettings, dataSource);
		return persistenceContextConfigurationBuilder.build();
	}
	
	@Bean
	@ConditionalOnMissingBean
	public StalactitePlatformTransactionManager platformTransactionManager(PersistenceContextConfiguration persistenceContextConfiguration) {
		return (StalactitePlatformTransactionManager) persistenceContextConfiguration.getConnectionConfiguration().getConnectionProvider();
	}

	@Bean
	@ConditionalOnMissingBean
	public Dialect dialect(PersistenceContextConfiguration persistenceContextConfiguration, ObjectProvider<DialectCustomizer> dialectCustomizer) {
		Dialect dialect = persistenceContextConfiguration.getDialect();
		nullable(dialectCustomizer.getIfAvailable()).invoke(customizer -> customizer.customize(dialect));
		return dialect;
	}
	
	@Bean
	@ConditionalOnMissingBean
	public ConnectionSettings connectionSettings() {
		return new ConnectionSettings();
	}
	
	@Bean
	@ConditionalOnMissingBean
	public DatabaseVendorSettings databaseVendorSettings(DialectResolver dialectResolver, DataSource dataSource) throws SQLException {
		DatabaseVendorSettings databaseVendorSettings;
		try (Connection connection = dataSource.getConnection()) {
			databaseVendorSettings = dialectResolver.determineVendorSettings(connection);
		}
		return databaseVendorSettings;
	}
	
	@Bean
	@ConditionalOnMissingBean
	public DialectResolver dialectResolver() {
		return new ServiceLoaderDialectResolver();
	}
	
	@Bean
	@ConditionalOnMissingBean
	public StalactitePlatformTransactionManager stalactitePlatformTransactionManager(DataSource dataSource) {
		return new StalactitePlatformTransactionManager(dataSource);
	}
	
	static class StalactiteRepositoriesImportSelector implements ImportSelector {

		@Override
		public String[] selectImports(AnnotationMetadata importingClassMetadata) {
			return new String[] { StalactiteRepositoriesRegistrar.class.getName() };
		}
	}
}
