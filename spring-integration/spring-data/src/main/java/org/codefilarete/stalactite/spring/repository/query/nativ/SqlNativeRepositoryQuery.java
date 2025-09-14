package org.codefilarete.stalactite.spring.repository.query.nativ;

import javax.annotation.Nullable;
import java.lang.reflect.Executable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityFinder;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.spring.repository.query.AbstractQueryExecutor;
import org.codefilarete.stalactite.spring.repository.query.AbstractRepositoryQuery;
import org.codefilarete.stalactite.spring.repository.query.NativeQuery;
import org.codefilarete.stalactite.spring.repository.query.ProjectionMappingFinder;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethodInvocationParameters;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.StringParamedSQL;
import org.codefilarete.tool.Reflections;
import org.springframework.data.projection.ProjectionFactory;

public class SqlNativeRepositoryQuery<C, R> extends AbstractRepositoryQuery<C, R> {
	
	private final String sql;
	/**
	 * Only available when a {@link org.springframework.data.domain.Page} type is expected as a result. Null otherwise.
	 * Because the {@link org.springframework.data.domain.Page} instantiation algorithm requires the total number of elements.
	 */
	@Nullable
	private final String sqlCount;
	private final ProjectionFactory factory;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	private final RelationalEntityFinder<C, ?, ?> relationalEntityFinder;
	private final ProjectionMappingFinder<C> projectionMappingFinder;
	
	public SqlNativeRepositoryQuery(StalactiteQueryMethod queryMethod,
									String sql,
									@Nullable String sqlCount,
									AdvancedEntityPersister<C, ?> entityPersister,
									ProjectionFactory factory,
									Dialect dialect,
									ConnectionProvider connectionProvider) {
		super(queryMethod);
		this.sql = sql;
		this.sqlCount = sqlCount;
		this.factory = factory;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		
		this.projectionMappingFinder = new ProjectionMappingFinder<>(factory, entityPersister);
		
		// Note that at this stage we can afford to ask for immediate Query creation because we are at a high layer (Spring Data Query discovery) and
		// persister is supposed to be finalized and up-to-date (containing the whole entity aggregate graph), that why we pass "true" as argument
		this.relationalEntityFinder = new RelationalEntityFinder<>(
				entityPersister,
				connectionProvider,
				dialect,
				true);
		
		// TODO: when upgrading to Spring Data 3.x.y, add an assertion on Limit parameter presence as it's done in StringBasedJdbcQuery
		// https://github.com/spring-projects/spring-data-relational/blob/main/spring-data-jdbc/src/main/java/org/springframework/data/jdbc/repository/query/StringBasedJdbcQuery.java#L176
	}
	
	@Override
	protected AbstractQueryExecutor<List<Object>, Object> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters) {
		// Note that if the query is a projection then the result List must contain Map<String, Object> else it must contain entities
		AbstractQueryExecutor<List<Object>, Object> queryExecutor;
		
		if (method.getParameters().hasDynamicProjection() && factory.getProjectionInformation(invocationParameters.getDynamicProjectionType()).isClosed()
			|| getQueryMethod().getResultProcessor().getReturnedType().isProjecting()) {
			
			// Extracting the Selectable and PropertyPath from the projection type
			Class<?> projectionType = method.getParameters().hasDynamicProjection()
					? invocationParameters.getDynamicProjectionType()
					: method.getReturnedObjectType();
			IdentityHashMap<JoinLink<?, ?>, AccessorChain<C, ?>> columnToProperties = this.projectionMappingFinder.lookup(projectionType);
			// Building aliases. The way we build them has no impact on other algorithms, they could be random, but for debugging and clarity purpose
			// we generate them as closest as possible to the property name
			IdentityHashMap<JoinLink<?, ?>, String> aliases = buildAliases(columnToProperties);
			
			queryExecutor = (AbstractQueryExecutor) new TupleNativeQueryExecutor(getQueryMethod(), sql, dialect, connectionProvider, aliases, columnToProperties, invocationParameters::getLimit);
		} else {
			queryExecutor = (AbstractQueryExecutor) new DomainEntityNativeQueryExecutor<>(getQueryMethod(), sql, relationalEntityFinder, dialect);
		}
		return queryExecutor;
	}
	
	@Override
	protected LongSupplier buildCountSupplier(StalactiteQueryMethodInvocationParameters invocationParameters) {
		return () -> {
			if (sqlCount == null || sqlCount.isEmpty()) {
				MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
				Executable countQueryAccessor = methodReferenceCapturer.findExecutable(NativeQuery::countQuery);
				throw new IllegalStateException("Count query is mandatory for paged queries, please provide one through " + Reflections.toString(countQueryAccessor));
			}
			StringParamedSQL query = new StringParamedSQL(sqlCount, invocationParameters.bindParameters(dialect));
			query.setValues(invocationParameters.getNamedValues());
			
			try (ReadOperation<?> readOperation = dialect.getReadOperationFactory().createInstance(query, connectionProvider)) {
//					readOperation.setListener((SQLOperation.SQLOperationListener<ParamType>) operationListener);
				// Note that setValues must be done after operationListener set
				readOperation.setValues((Map) query.getValues());
				ResultSet resultSet = readOperation.execute();
				ResultSetIterator<Long> resultSetIterator = new ResultSetIterator<Long>(resultSet) {
					@Override
					public Long convert(ResultSet resultSet) throws SQLException {
						return resultSet.getLong(1);
					}
				};
				return resultSetIterator.hasNext() ? resultSetIterator.next() : 0;
			} catch (RuntimeException e) {
				throw new SQLExecutionException(query.getSQL(), e);
			}
		};
	}
}
