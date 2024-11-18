package org.codefilarete.stalactite.sql.spring.repository;

import java.lang.reflect.Method;
import java.util.Optional;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.spring.repository.SimpleStalactiteRepository;
import org.codefilarete.stalactite.spring.repository.query.CreateQueryLookupStrategy;
import org.codefilarete.stalactite.spring.repository.query.DeclaredQueryLookupStrategy;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.exception.NotImplementedException;
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
import org.springframework.lang.Nullable;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Mimics JpaRepositoryFactory for Stalactite
 *
 * @author Guillaume Mary
 */
public class StalactiteRepositoryFactory extends RepositoryFactorySupport {
	
	private final AdvancedEntityPersister<?, ?> entityPersister;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
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
	
	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable Key key,
																   QueryMethodEvaluationContextProvider evaluationContextProvider) {

//		return Optional.of(new CreateQueryLookupStrategy<>(entityPersister));
		switch (Objects.preventNull(key, Key.CREATE_IF_NOT_FOUND)) {
			case CREATE:
				return Optional.of(new CreateQueryLookupStrategy<>(entityPersister));
			case USE_DECLARED_QUERY:
				return Optional.of(new DeclaredQueryLookupStrategy<>(entityPersister, dialect, connectionProvider));
			case CREATE_IF_NOT_FOUND:
				return Optional.of(new CreateIfNotFoundQueryLookupStrategy(
						new DeclaredQueryLookupStrategy<>(entityPersister, dialect, connectionProvider),
						new CreateQueryLookupStrategy(entityPersister)));
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
	private static class CreateIfNotFoundQueryLookupStrategy implements QueryLookupStrategy {
		
		private final DeclaredQueryLookupStrategy lookupStrategy;
		private final CreateQueryLookupStrategy createStrategy;
		
		/**
		 * Creates a new {@link CreateIfNotFoundQueryLookupStrategy}.
		 *
		 * @param createStrategy must not be {@literal null}.
		 * @param lookupStrategy must not be {@literal null}.
		 */
		public CreateIfNotFoundQueryLookupStrategy(DeclaredQueryLookupStrategy lookupStrategy, CreateQueryLookupStrategy createStrategy) {
			this.lookupStrategy = lookupStrategy;
			this.createStrategy = createStrategy;
		}
		
		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory, NamedQueries namedQueries) {
			return nullable(lookupStrategy.resolveQuery(method, metadata, factory, namedQueries))
					.getOr(() -> createStrategy.resolveQuery(method, metadata, factory, namedQueries));
		}
	}
}
