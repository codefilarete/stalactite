package org.codefilarete.stalactite.spring.repository.config;

import java.lang.annotation.Annotation;

import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.data.repository.config.RepositoryBeanDefinitionRegistrarSupport;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;

/**
 * {@link ImportBeanDefinitionRegistrar} to enable {@link EnableStalactiteRepositories} annotation.
 *
 * @author Oliver Gierke
 */
class StalactiteRepositoriesRegistrar extends RepositoryBeanDefinitionRegistrarSupport {
	
	@Override
	protected Class<? extends Annotation> getAnnotation() {
		return EnableStalactiteRepositories.class;
	}

	@Override
	protected RepositoryConfigurationExtension getExtension() {
		return new StalactiteRepositoryConfigExtension();
	}
}
