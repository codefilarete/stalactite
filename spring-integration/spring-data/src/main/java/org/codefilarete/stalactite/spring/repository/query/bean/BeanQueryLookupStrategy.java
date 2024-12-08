package org.codefilarete.stalactite.spring.repository.query.bean;

import java.lang.reflect.Method;

import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.spring.repository.query.Query;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.Nullable;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanNotOfRequiredTypeException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * {@link QueryLookupStrategy} that tries to detect a declared query declared via {@link Query} annotation followed by
 * a JPA named query lookup.
 *
 * @author Guillaume Mary
 */
public class BeanQueryLookupStrategy<C> implements QueryLookupStrategy {
	
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final BeanFactory beanFactory;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	/**
	 * Creates a new {@link BeanQueryLookupStrategy}.
	 *
	 */
	public BeanQueryLookupStrategy(AdvancedEntityPersister<C, ?> entityPersister,
								   Dialect dialect,
								   ConnectionProvider connectionProvider,
								   BeanFactory beanFactory) {
		this.entityPersister = entityPersister;
		this.beanFactory = beanFactory;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
	}
	
	@Override
	public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory, NamedQueries namedQueries) {
		Nullable<ExecutableEntityQuery<C, ?>> bean;
		try {
			bean = nullable(beanFactory.getBean(method.getName(), ExecutableEntityQuery.class));
		} catch (NoSuchBeanDefinitionException | BeanNotOfRequiredTypeException e) {
			bean = nullable((ExecutableEntityQuery<C, ?>) null);
		}
		if (bean.isPresent()) {
			BeanQueryMethod queryMethod = new BeanQueryMethod(method, metadata, factory);
			Accumulator<C, ?, ?> accumulator = queryMethod.isCollectionQuery()
					? (Accumulator) Accumulators.toKeepingOrderSet()
					: (Accumulator) Accumulators.getFirstUnique();
			return new BeanRepositoryQuery<>(queryMethod, bean.get(), entityPersister, accumulator, dialect, connectionProvider);
		} else {
			return null;
		}
	}
}