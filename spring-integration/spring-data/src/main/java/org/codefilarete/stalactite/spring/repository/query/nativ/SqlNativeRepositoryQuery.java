package org.codefilarete.stalactite.spring.repository.query.nativ;

import javax.annotation.Nullable;
import java.lang.reflect.Executable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityFinder;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.spring.repository.query.AbstractRepositoryQuery;
import org.codefilarete.stalactite.spring.repository.query.NativeQuery;
import org.codefilarete.stalactite.spring.repository.query.ProjectionTypeInformationExtractor;
import org.codefilarete.stalactite.spring.repository.query.StalactiteLimitRepositoryQuery;
import org.codefilarete.stalactite.spring.repository.query.projection.CollectionProjectionEngine;
import org.codefilarete.stalactite.spring.repository.query.projection.PagedProjectionEngine;
import org.codefilarete.stalactite.spring.repository.query.projection.ProjectionEngine;
import org.codefilarete.stalactite.spring.repository.query.projection.SingleProjectionEngine;
import org.codefilarete.stalactite.spring.repository.query.projection.SlicedProjectionEngine;
import org.codefilarete.stalactite.spring.repository.query.projection.StalactiteParametersParameterAccessor;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.StringParamedSQL;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.tool.Reflections;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.query.ParametersParameterAccessor;

public class SqlNativeRepositoryQuery<C, R> extends AbstractRepositoryQuery implements StalactiteLimitRepositoryQuery<C, R> {
	
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
	private final ProjectionTypeInformationExtractor<C> projectionTypeInformationExtractor;
	private Limit limit;
	
	public SqlNativeRepositoryQuery(NativeQueryMethod queryMethod,
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
		
		this.projectionTypeInformationExtractor = new ProjectionTypeInformationExtractor<>(factory, entityPersister);
		// Extracting the Selectable and PropertyPath from the aggregate
		this.projectionTypeInformationExtractor.extract(method.getReturnedObjectType());
		
		// Note that at this stage we can afford to ask for immediate Query creation because we are at a high layer (Spring Data Query discovery) and
		// persister is supposed to be finalized and up-to-date (containing the whole entity aggregate graph), that why we pass "true" as argument
		this.relationalEntityFinder = new RelationalEntityFinder<>(
				entityPersister.getEntityJoinTree(),
				connectionProvider,
				dialect,
				true);
		
		// TODO: when upgrading to Spring Data 3.x.y, add an assertion on Limit parameter presence as it's done in StringBasedJdbcQuery
		// https://github.com/spring-projects/spring-data-relational/blob/main/spring-data-jdbc/src/main/java/org/springframework/data/jdbc/repository/query/StringBasedJdbcQuery.java#L176
	}
	
	@Override
	public NativeQueryMethod getQueryMethod() {
		return (NativeQueryMethod) super.getQueryMethod();
	}
	
	public Limit getLimit() {
		return limit;
	}
	
	@Override
	public R execute(Object[] parameters) {
		StalactiteParametersParameterAccessor accessor = new StalactiteParametersParameterAccessor(method.getParameters(), parameters);
		if (getQueryMethod().isCollectionQuery()
				|| getQueryMethod().isPageQuery()
				|| getQueryMethod().isSliceQuery()) {

			// if the query is a projection then the result List must contain Map<String, Object> else it must contain entities
			AbstractNativeQueryExecutor<List<Object>, Object> queryExecutor;
			if (method.getParameters().hasDynamicProjection() && factory.getProjectionInformation(accessor.getDynamicProjectionType()).isClosed()) {
				this.projectionTypeInformationExtractor.extract(accessor.getDynamicProjectionType());
				IdentityHashMap<Selectable<?>, String> aliases = projectionTypeInformationExtractor.getAliases();
				IdentityHashMap<Selectable<?>, PropertyPath> columnToProperties = projectionTypeInformationExtractor.getColumnToProperties();
				
				queryExecutor = (AbstractNativeQueryExecutor) new TupleNativeQueryExecutor(getQueryMethod(), sql, dialect, connectionProvider, aliases, columnToProperties, this::getLimit);
			} else if (getQueryMethod().getResultProcessor().getReturnedType().isProjecting()
					&& factory.getProjectionInformation(getQueryMethod().getReturnedObjectType()).isClosed()) {
				IdentityHashMap<Selectable<?>, String> aliases = projectionTypeInformationExtractor.getAliases();
				IdentityHashMap<Selectable<?>, PropertyPath> columnToProperties = projectionTypeInformationExtractor.getColumnToProperties();
				queryExecutor = (AbstractNativeQueryExecutor) new TupleNativeQueryExecutor(getQueryMethod(), sql, dialect, connectionProvider, aliases, columnToProperties, this::getLimit);
			} else {
				queryExecutor = (AbstractNativeQueryExecutor) new BeanNativeQueryExecutor<>(getQueryMethod(), sql, relationalEntityFinder, dialect);
			}
			Supplier<List<Object>> resultSupplier = queryExecutor.buildQueryExecutor(parameters);

			R adaptation = buildResultWindower(accessor, queryExecutor.bindParameters(accessor))
					.adapt(resultSupplier)
					.apply(parameters);
			
			// - hasDynamicProjection() is for case of method that gives the expected returned type as a last argument (or a compound one by Collection or other)
			if (method.getParameters().hasDynamicProjection()) {
				return method.getResultProcessor().withDynamicProjection(new ParametersParameterAccessor(method.getParameters(), parameters)).processResult(adaptation);
			} else {
				return method.getResultProcessor().processResult(adaptation);
			}
		} else {
			// The result is a single entity or a projection
			if (getQueryMethod().getResultProcessor().getReturnedType().isProjecting()
//					&& factory.getProjectionInformation(method.getReturnedObjectType()).isClosed()
					|| getQueryMethod().getParameters().hasDynamicProjection()
			) {
//				getQueryMethod().isQueryForEntity();
				
				AbstractNativeQueryExecutor<List<Map<String, Object>>, Map<String, Object>> queryExecutor;
				if (method.getParameters().hasDynamicProjection() && factory.getProjectionInformation(accessor.getDynamicProjectionType()).isClosed()) {
					this.projectionTypeInformationExtractor.extract(accessor.getDynamicProjectionType());
					IdentityHashMap<Selectable<?>, String> aliases = projectionTypeInformationExtractor.getAliases();
					IdentityHashMap<Selectable<?>, PropertyPath> columnToProperties = projectionTypeInformationExtractor.getColumnToProperties();

					queryExecutor = (AbstractNativeQueryExecutor) new TupleNativeQueryExecutor(getQueryMethod(), sql, dialect, connectionProvider, aliases, columnToProperties, this::getLimit);
				} else if (getQueryMethod().getResultProcessor().getReturnedType().isProjecting()
						&& factory.getProjectionInformation(getQueryMethod().getReturnedObjectType()).isClosed()) {
					IdentityHashMap<Selectable<?>, String> aliases = projectionTypeInformationExtractor.getAliases();
					IdentityHashMap<Selectable<?>, PropertyPath> columnToProperties = projectionTypeInformationExtractor.getColumnToProperties();
					queryExecutor = (AbstractNativeQueryExecutor) new TupleNativeQueryExecutor(getQueryMethod(), sql, dialect, connectionProvider, aliases, columnToProperties, this::getLimit);
				} else {
					queryExecutor = (AbstractNativeQueryExecutor) new BeanNativeQueryExecutor<>(getQueryMethod(), sql, relationalEntityFinder, dialect);
				}
				Supplier<List<Map<String, Object>>> resultSupplier = queryExecutor.buildQueryExecutor(parameters);
				
				Map<String, Object> data = Accumulators.<Map<String, Object>>getFirstUnique().collect(resultSupplier.get());

				if (method.getParameters().hasDynamicProjection()) {
					return method.getResultProcessor().withDynamicProjection(new ParametersParameterAccessor(method.getParameters(), parameters)).processResult(data);
				} else {
					return method.getResultProcessor().processResult(data);
				}
			} else {
				BeanNativeQueryExecutor<C> beanNativeQueryExecutor = new BeanNativeQueryExecutor<>(getQueryMethod(), sql, relationalEntityFinder, dialect);
				List<C> foundBeans = beanNativeQueryExecutor.buildQueryExecutor(parameters).get();

				Collection<R> projectionResult;
				if (method.getParameters().hasDynamicProjection()) {
					projectionResult = method.getResultProcessor().withDynamicProjection(new ParametersParameterAccessor(method.getParameters(), parameters)).processResult(foundBeans);
				} else {
					projectionResult = method.getResultProcessor().processResult(foundBeans);
				}
				return Accumulators.<R>getFirstUnique().collect(projectionResult);
			}
		}
	}
	
	private <ROW> ProjectionEngine<R, ROW> buildResultWindower(StalactiteParametersParameterAccessor accessor, Map<String, PreparedStatementWriter<?>> bindParameters) {
		ProjectionEngine<?, C> result;
		if (method.isPageQuery()) {
			result = new PagedProjectionEngine<>(this, () -> {
				if (sqlCount == null || sqlCount.isEmpty()) {
					MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
					Executable countQueryAccessor = methodReferenceCapturer.findExecutable(NativeQuery::countQuery);
					throw new IllegalStateException("Count query is mandatory for paged queries, please provide one through " + Reflections.toString(countQueryAccessor));
				}
				StringParamedSQL query = new StringParamedSQL(sqlCount, bindParameters);
				query.setValues(accessor.getNamedValues());

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
			});
		} else if (method.isSliceQuery()) {
			result = new SlicedProjectionEngine<>(this);
		} else if (method.isCollectionQuery()) {
			result = new CollectionProjectionEngine<>();
		} else {
			result = new SingleProjectionEngine<>();
		}
		return (ProjectionEngine<R, ROW>) result;
	}
	
	@Override
	public void limit(int count) {
		limit = new Limit(count);
	}

	@Override
	public void limit(int count, Integer offset) {
		limit = new Limit(count, offset);
	}
}
