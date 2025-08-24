package org.codefilarete.stalactite.spring.repository;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Optional;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.spring.repository.query.CreateQueryLookupStrategy;
import org.codefilarete.stalactite.spring.repository.query.bean.BeanQueryLookupStrategy;
import org.codefilarete.stalactite.spring.repository.query.nativ.NativeQueryLookupStrategy;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.NotImplementedException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;

/**
 * Mimics JpaRepositoryFactory for Stalactite
 *
 * @author Guillaume Mary
 */
public class StalactiteRepositoryFactory extends RepositoryFactorySupport {
	
	private final AdvancedEntityPersister<?, ?> entityPersister;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	private ListableBeanFactory beanFactory;
	
	public StalactiteRepositoryFactory(AdvancedEntityPersister<?, ?> entityPersister,
									   Dialect dialect,
									   ConnectionProvider connectionProvider) {
		this.entityPersister = entityPersister;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
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
	
	/**
	 * Overridden to be capable of accessing the instance, because parent field is private :'( 
	 * @param beanFactory owning BeanFactory (never {@code null}).
	 */
	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = (ListableBeanFactory) beanFactory;
	}
	
	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable QueryLookupStrategy.Key key,
																   QueryMethodEvaluationContextProvider evaluationContextProvider) {

		switch (Objects.preventNull(key, Key.CREATE_IF_NOT_FOUND)) {
			case CREATE:
				return Optional.of(new CreateQueryLookupStrategy<>(entityPersister));
			case USE_DECLARED_QUERY:
				return Optional.of(new FirstMatchingQueryLookupStrategy(
						new BeanQueryLookupStrategy<>(beanFactory, dialect),
						new NativeQueryLookupStrategy<>(entityPersister, dialect, connectionProvider)));
			case CREATE_IF_NOT_FOUND:
				return Optional.of(new FirstMatchingQueryLookupStrategy(
						new BeanQueryLookupStrategy<>(beanFactory, dialect),
						new NativeQueryLookupStrategy<>(entityPersister, dialect, connectionProvider),
						new CreateQueryLookupStrategy<>(entityPersister)));
			default:
				throw new IllegalArgumentException(String.format("Unsupported query lookup strategy %s!", key));
		}
	}
	
	/**
	 * {@link QueryLookupStrategy} to try to detect a declared query first (
	 * {@link org.springframework.data.jpa.repository.Query}, JPA named query). In case none is found we fall back on
	 * query creation.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	private static class FirstMatchingQueryLookupStrategy implements QueryLookupStrategy {
		
		private final Iterable<QueryLookupStrategy> lookupStrategies;
		
		/**
		 * Creates a new {@link FirstMatchingQueryLookupStrategy}.
		 *
		 * @param lookupStrategies must not be {@literal null}.
		 */
		public FirstMatchingQueryLookupStrategy(QueryLookupStrategy... lookupStrategies) {
			this.lookupStrategies = Arrays.asSet(lookupStrategies);
		}
		
		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory, NamedQueries namedQueries) {
			return Iterables.find(lookupStrategies, lookupStrategy -> lookupStrategy.resolveQuery(method, metadata, factory, namedQueries), java.util.Objects::nonNull).getRight();
		}
	}
}
