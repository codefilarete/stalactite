package org.codefilarete.stalactite.sql.spring.repository;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.core.support.TransactionalRepositoryFactoryBeanSupport;

/**
 * Mimics Spring JpaRepositoryFactoryBean for Stalactite.
 * 
 * @param <R> repository type
 * @param <C> entity type the repository persists
 * @param <I> entity identifier type
 */
public class StalactiteRepositoryFactoryBean<R extends Repository<C, I>, C, I>
		extends TransactionalRepositoryFactoryBeanSupport<R, C, I> {
	
	private EntityPersister<?, ?> entityPersister;
	
	/**
	 * Creates a new {@link StalactiteRepositoryFactoryBean} for the given repository interface.
	 *
	 * @param repositoryInterface must not be {@literal null}.
	 */
	protected StalactiteRepositoryFactoryBean(Class<? extends R> repositoryInterface) {
		super(repositoryInterface);
	}
	
	public void setEntityPersister(EntityPersister<?, ?> entityPersister) {
		this.entityPersister = entityPersister;
	}
	
	@Override
	protected RepositoryFactorySupport doCreateRepositoryFactory() {
		return new StalactiteRepositoryFactory(entityPersister);
	}
}
