package org.codefilarete.stalactite.spring.repository;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.OptimizedUpdatePersister;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.reflect.MethodDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.core.support.TransactionalRepositoryFactoryBeanSupport;

/**
 * Mimics {@link org.springframework.data.jpa.repository.support.JpaRepositoryFactoryBean}
 * 
 * @param <R> repository type
 * @param <C> entity type the repository persists
 * @param <I> entity identifier type
 */
public class StalactiteRepositoryFactoryBean<R extends Repository<C, I>, C, I>
		extends TransactionalRepositoryFactoryBeanSupport<R, C, I> {
	
	public static <C, I> AdvancedEntityPersister<C, I> asInternalPersister(EntityPersister<C, I> foundPersister) {
		// Converting found EntityPersister to an AdvancedEntityPersister
		// This is hideous : due to the will to not expose AdvancedEntityPersister to the outside world, but combined to the need to use it and
		// the fact that its implementing classes are hidden by several layers of interfaces, with "dig" into given result to find them
		// and wrap the result into a proxy that dispatch called methods accordingly.
		ConfiguredRelationalPersister<C, I> deepestDelegate = ((OptimizedUpdatePersister<C, I>) foundPersister).getDeepestDelegate();
		MethodDispatcher methodDispatcher = new MethodDispatcher();
		// Please note that order of precedence has an impact on getting a working result or not because AdvancedEntityPersister already extends
		// ConfiguredPersister (yes, that's awful, but I couldn't find a better way without the constraint of not exposing AdvancedEntityPersister)
		methodDispatcher
				.redirect(AdvancedEntityPersister.class, (AdvancedEntityPersister<C, I>) deepestDelegate)
				.redirect(ConfiguredPersister.class, (ConfiguredPersister<C, I>) foundPersister);
		return methodDispatcher.build(AdvancedEntityPersister.class);
	}
	
	private Class<?> entityType;
	private PersistenceContext persistenceContext;
	
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
		this.persistenceContext = persistenceContext;
	}
	
	@Autowired
	public void setApplicationContext(ApplicationContext applicationContext) {
		applicationContext.getBeansOfType(EntityPersister.class);
	}
	
	@Override
	protected RepositoryFactorySupport doCreateRepositoryFactory() {
		EntityPersister<?, Object> foundPersister = persistenceContext.findPersister(this.entityType);
		if (foundPersister == null) {
			throw new IllegalArgumentException("No persister found for entityType " + Reflections.toString(entityType) + " in persistence context.");
		}
		return new StalactiteRepositoryFactory(asInternalPersister(foundPersister), persistenceContext.getDialect(), persistenceContext.getConnectionProvider());
	}
}
