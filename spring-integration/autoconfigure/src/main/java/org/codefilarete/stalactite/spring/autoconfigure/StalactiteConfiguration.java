package org.codefilarete.stalactite.spring.autoconfigure;

import javax.sql.DataSource;

import org.codefilarete.stalactite.spring.transaction.StalactitePlatformTransactionManager;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.autoconfigure.transaction.TransactionManagerCustomizers;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionManager;

/**
 * {@link Configuration} implementation for Stalactite.
 *
 * @author Guillaume Mary
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(StalactiteProperties.class)
@ConditionalOnSingleCandidate(DataSource.class)
class StalactiteConfiguration {
	
	private final DataSource dataSource;
	
	StalactiteConfiguration(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	public DataSource getDataSource() {
		return dataSource;
	}
	
	@Bean
	@ConditionalOnMissingBean(TransactionManager.class)
	public PlatformTransactionManager transactionManager(
			ObjectProvider<TransactionManagerCustomizers> transactionManagerCustomizers) {
		StalactitePlatformTransactionManager transactionManager = new StalactitePlatformTransactionManager(getDataSource());
		transactionManagerCustomizers.ifAvailable(customizers -> customizers.customize(transactionManager));
		return transactionManager;
	}
}
