package org.codefilarete.stalactite.sql.spring.repository.config;

import java.util.Optional;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.data.repository.config.BootstrapMode;
import org.springframework.data.repository.config.ImplementationDetectionConfiguration;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.util.Streamable;

/**
 * A simple wrapper of {@link RepositoryConfigurationSource} that delegates its invocations to a delegate.
 * Made to be overridden in order that we understand better which behavior is overridden by subclass. Kind of design stuff.
 * 
 * @param <T>
 * @author Guillaume Mary
 */
class RepositoryConfigurationSourceWrapper<T extends RepositoryConfigurationSource> implements RepositoryConfigurationSource {
	
	private final T delegate;
	
	public RepositoryConfigurationSourceWrapper(T delegate) {
		this.delegate = delegate;
	}
	
	@Override
	public Object getSource() {
		return delegate.getSource();
	}
	
	@Override
	public Streamable<String> getBasePackages() {
		return delegate.getBasePackages();
	}
	
	@Override
	public Optional<Object> getQueryLookupStrategyKey() {
		return delegate.getQueryLookupStrategyKey();
	}
	
	@Override
	public Optional<String> getRepositoryImplementationPostfix() {
		return delegate.getRepositoryImplementationPostfix();
	}
	
	@Override
	public Optional<String> getNamedQueryLocation() {
		return delegate.getNamedQueryLocation();
	}
	
	@Override
	public Optional<String> getRepositoryBaseClassName() {
		return delegate.getRepositoryBaseClassName();
	}
	
	@Override
	public Optional<String> getRepositoryFactoryBeanClassName() {
		return delegate.getRepositoryFactoryBeanClassName();
	}
	
	@Override
	public Streamable<BeanDefinition> getCandidates(ResourceLoader loader) {
		return delegate.getCandidates(loader);
	}
	
	@Override
	public Optional<String> getAttribute(String name) {
		return delegate.getAttribute(name);
	}
	
	@Override
	public <T> Optional<T> getAttribute(String name, Class<T> type) {
		return delegate.getAttribute(name, type);
	}
	
	@Override
	public boolean usesExplicitFilters() {
		return delegate.usesExplicitFilters();
	}
	
	@Override
	public Streamable<TypeFilter> getExcludeFilters() {
		return delegate.getExcludeFilters();
	}
	
	@Override
	public String generateBeanName(BeanDefinition beanDefinition) {
		return delegate.generateBeanName(beanDefinition);
	}
	
	@Override
	public ImplementationDetectionConfiguration toImplementationDetectionConfiguration(MetadataReaderFactory factory) {
		return delegate.toImplementationDetectionConfiguration(factory);
	}
	
	@Override
	public BootstrapMode getBootstrapMode() {
		return delegate.getBootstrapMode();
	}
	
	@Override
	public String getResourceDescription() {
		return delegate.getResourceDescription();
	}
}
