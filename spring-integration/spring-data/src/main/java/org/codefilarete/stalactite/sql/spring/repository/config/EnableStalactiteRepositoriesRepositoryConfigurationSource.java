package org.codefilarete.stalactite.sql.spring.repository.config;

import java.util.Optional;

import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;

/**
 * {@link org.springframework.data.repository.config.RepositoryConfigurationSource} for @{@link EnableStalactiteRepositories}.
 * 
 * @author Guillaume Mary
 */
public class EnableStalactiteRepositoriesRepositoryConfigurationSource extends RepositoryConfigurationSourceWrapper<AnnotationRepositoryConfigurationSource> {
	
	/**
	 * Constructor with mandatory values.
	 * 
	 * @param delegate a source for @{@link EnableStalactiteRepositories}
	 */
	public EnableStalactiteRepositoriesRepositoryConfigurationSource(AnnotationRepositoryConfigurationSource delegate) {
		super(delegate);
	}
	
	/**
	 * Implemented to return empty result since this feature is not implemented.
	 * @return an empty result
	 */
	@Override
	public Optional<Object> getQueryLookupStrategyKey() {
		return Optional.empty();
	}
	
	/**
	 * Implemented to return empty result since this feature is not implemented.
	 * @return an empty result
	 */
	@Override
	public Optional<String> getNamedQueryLocation() {
		return Optional.empty();
	}
}
