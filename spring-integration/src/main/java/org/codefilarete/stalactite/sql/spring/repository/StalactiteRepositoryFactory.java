package org.codefilarete.stalactite.sql.spring.repository;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.tool.exception.NotImplementedException;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

/**
 * Mimics Spring JpaRepositoryFactory for Stalactite.
 * 
 * @author Guillaume Mary
 */
public class StalactiteRepositoryFactory extends RepositoryFactorySupport {
	
	private final EntityPersister<?, ?> entityPersister;
	
	public StalactiteRepositoryFactory(EntityPersister<?, ?> entityPersister) {
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
}
