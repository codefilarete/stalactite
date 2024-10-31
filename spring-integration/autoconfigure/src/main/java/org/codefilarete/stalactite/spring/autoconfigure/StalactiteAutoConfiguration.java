package org.codefilarete.stalactite.spring.autoconfigure;

import org.codefilarete.stalactite.engine.PersistenceContext;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Stalactite.
 *
 * @author Guillaume Mary
 */
@AutoConfiguration(after = DataSourceAutoConfiguration.class, before = TransactionAutoConfiguration.class)
@ConditionalOnClass({ PersistenceContext.class })
@EnableConfigurationProperties(StalactiteProperties.class)
@Import(StalactiteConfiguration.class)
public class StalactiteAutoConfiguration {

}