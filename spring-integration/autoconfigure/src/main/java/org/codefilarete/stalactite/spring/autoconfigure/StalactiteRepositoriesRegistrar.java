package org.codefilarete.stalactite.spring.autoconfigure;

import java.lang.annotation.Annotation;
import java.util.Locale;

import org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.spring.repository.config.StalactiteRepositoryConfigExtension;
import org.springframework.boot.autoconfigure.data.AbstractRepositoryConfigurationSourceSupport;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.data.repository.config.BootstrapMode;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.util.StringUtils;

/**
 * {@link ImportBeanDefinitionRegistrar} used to auto-configure Spring Data Stalactite
 * Repositories.
 *
 * @author Guillaume Mary
 */
class StalactiteRepositoriesRegistrar extends AbstractRepositoryConfigurationSourceSupport {

	private BootstrapMode bootstrapMode = null;

	@Override
	protected Class<? extends Annotation> getAnnotation() {
		return EnableStalactiteRepositories.class;
	}

	@Override
	protected Class<?> getConfiguration() {
		return EnableStalactiteRepositoriesConfiguration.class;
	}

	@Override
	protected RepositoryConfigurationExtension getRepositoryConfigurationExtension() {
		return new StalactiteRepositoryConfigExtension();
	}

	@Override
	protected BootstrapMode getBootstrapMode() {
		return this.bootstrapMode == null ? BootstrapMode.DEFAULT : this.bootstrapMode;
	}

	@Override
	public void setEnvironment(Environment environment) {
		super.setEnvironment(environment);
		configureBootstrapMode(environment);
	}

	private void configureBootstrapMode(Environment environment) {
		String property = environment.getProperty("spring.data.stalactite.repositories.bootstrap-mode");
		if (StringUtils.hasText(property)) {
			this.bootstrapMode = BootstrapMode.valueOf(property.toUpperCase(Locale.ENGLISH));
		}
	}

	@EnableStalactiteRepositories
	private static class EnableStalactiteRepositoriesConfiguration {

	}

}