package org.codefilarete.stalactite.sql.spring.repository;

import java.lang.reflect.Type;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.core.support.TransactionalRepositoryFactoryBeanSupport;
import org.springframework.util.Assert;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

/**
 * Mimics {@link org.springframework.data.jpa.repository.support.JpaRepositoryFactoryBean}
 * 
 * @param <R> repository type
 * @param <C> entity type the repository persists
 * @param <I> entity identifier type
 */
public class StalactiteRepositoryFactoryBean<R extends Repository<C, I>, C, I>
		extends TransactionalRepositoryFactoryBeanSupport<R, C, I> {
	
	private EntityPersister<?, ?> entityPersister;
	private Class<?> entityType;
	
	/**
	 * Creates a new {@link StalactiteRepositoryFactoryBean} for the given repository interface.
	 *
	 * @param repositoryInterface must not be {@literal null}.
	 */
	protected StalactiteRepositoryFactoryBean(Class<? extends R> repositoryInterface) {
		super(repositoryInterface);
		for (Type genericInterface : repositoryInterface.getGenericInterfaces()) {
			if (genericInterface instanceof ParameterizedTypeImpl
					&& StalactiteRepository.class.isAssignableFrom((((ParameterizedTypeImpl) genericInterface).getRawType()))) {
				Type persistedType = ((ParameterizedTypeImpl) genericInterface).getActualTypeArguments()[0];
				if (persistedType instanceof Class) {
					this.entityType = (Class<?>) persistedType;
				}
			}
		}
	}
	
	@Autowired
	public void setPersistenceContext(PersistenceContext persistenceContext) {
		this.entityPersister = persistenceContext.getPersister(this.entityType);
	}
	
	@Override
	protected RepositoryFactorySupport doCreateRepositoryFactory() {
		return new StalactiteRepositoryFactory(this.entityPersister);
	}
	
	@Override
	public void afterPropertiesSet() {
		
		Assert.state(entityPersister != null, "EntityPersister must not be null!");
		
		super.afterPropertiesSet();
	}
}
