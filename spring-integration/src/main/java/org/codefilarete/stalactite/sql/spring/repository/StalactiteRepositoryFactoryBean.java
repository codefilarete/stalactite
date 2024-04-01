package org.codefilarete.stalactite.sql.spring.repository;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.tool.Reflections;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.core.support.TransactionalRepositoryFactoryBeanSupport;
import org.springframework.util.Assert;

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
			if (genericInterface instanceof ParameterizedType && ((ParameterizedType) genericInterface).getRawType() instanceof Class) {
				if (StalactiteRepository.class.isAssignableFrom((Class<?>) ((ParameterizedType) genericInterface).getRawType())) {
					Type persistedType = ((ParameterizedType) genericInterface).getActualTypeArguments()[0];
					if (persistedType instanceof Class) {
						this.entityType = (Class<?>) persistedType;
					}
				}
			}
		}
		if (this.entityType == null) {
			throw new UnsupportedOperationException("Entity type can't be deduced : repository class has unsupported generic type, must be a class");
		}
	}
	
	@Autowired
	public void setPersistenceContext(PersistenceContext persistenceContext) {
		EntityPersister<?, Object> foundPersister = persistenceContext.getPersister(this.entityType);
		if (foundPersister == null) {
			throw new IllegalArgumentException("No persister found for entityType " + Reflections.toString(entityType) + " in persistence context.");
		}
		this.entityPersister = foundPersister;
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
