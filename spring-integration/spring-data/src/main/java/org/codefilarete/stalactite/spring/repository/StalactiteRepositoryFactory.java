package org.codefilarete.stalactite.spring.repository;

import java.util.Optional;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.spring.repository.query.CreateQueryLookupStrategy;
import org.codefilarete.tool.exception.NotImplementedException;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.lang.Nullable;

/**
 * Mimics JpaRepositoryFactory for Stalactite
 * 
 * @author Guillaume Mary
 */
public class StalactiteRepositoryFactory extends RepositoryFactorySupport {
	
	private final AdvancedEntityPersister<?, ?> entityPersister;
	
	public StalactiteRepositoryFactory(AdvancedEntityPersister<?, ?> entityPersister) {
		this.entityPersister = entityPersister;
	}
	
	@Override
	public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
		// because I don't know in which case this method is invoked I throw an exception
		throw new NotImplementedException("waiting for use case");
	}
	
	@Override
	protected Object getTargetRepository(RepositoryInformation metadata) {
		return new SimpleStalactiteRepository<>(entityPersister);
	}
	
	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleStalactiteRepository.class;
	}
	
	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable Key key,
																   QueryMethodEvaluationContextProvider evaluationContextProvider) {
		return Optional.of(new CreateQueryLookupStrategy<>(entityPersister));
	}
	
}
