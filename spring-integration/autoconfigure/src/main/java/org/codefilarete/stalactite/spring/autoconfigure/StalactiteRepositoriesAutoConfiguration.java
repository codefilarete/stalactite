package org.codefilarete.stalactite.spring.autoconfigure;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.spring.autoconfigure.StalactiteRepositoriesAutoConfiguration.StalactiteRepositoriesImportSelector;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.codefilarete.stalactite.spring.repository.StalactiteRepositoryFactoryBean;
import org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories;
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
@ConditionalOnProperty(prefix = "spring.data.stalactite.repositories", name = "enabled", havingValue = "true",
		matchIfMissing = true)
@Import(StalactiteRepositoriesImportSelector.class)
public class StalactiteRepositoriesAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public PersistenceContext persistenceContext(DataSource dataSource, Dialect dialect) {
		return new PersistenceContext(dataSource, dialect);
	}
	
	@Bean
	@ConditionalOnMissingBean
	public Dialect dialect(DialectResolver dialectResolver, DataSource dataSource, ObjectProvider<DialectCustomizer> dialectCustomizer) throws SQLException {
		Dialect dialect;
		try (Connection connection = dataSource.getConnection()) {
			dialect = dialectResolver.determineDialect(connection);
		}
		dialectCustomizer.getIfAvailable().customize(dialect);
		return dialect;
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
