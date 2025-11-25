package org.codefilarete.stalactite.spring.repository.query.bean;

import javax.annotation.Nullable;
import java.lang.reflect.Executable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.spring.repository.query.BeanQuery;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.spring.repository.query.execution.AbstractQueryExecutor;
import org.codefilarete.stalactite.spring.repository.query.execution.AbstractRepositoryQuery;
import org.codefilarete.stalactite.spring.repository.query.execution.StalactiteQueryMethodInvocationParameters;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.MutableLong;

/**
 * Class to treat @{@link BeanQuery} annotated methods.
 * 
 * @param <O> entity type
 * @param <R> result type, which can be O or a composed type of O
 * @author Guillaume Mary
 */
public class QueryRepositoryQuery<O, R> extends AbstractRepositoryQuery<O, R> {
	
	private final ExecutableQuery<O> entityQuery;
	@Nullable
	private final ExecutableProjectionQuery<O, ?> countQuery;
	private final Dialect dialect;
	
	public QueryRepositoryQuery(StalactiteQueryMethod queryMethod,
								ExecutableQuery entityQuery,
								@Nullable ExecutableProjectionQuery<O, ?> countQuery,
								Dialect dialect) {
		super(queryMethod);
		this.entityQuery = entityQuery;
		this.countQuery = countQuery;
		this.dialect = dialect;
		
		// TODO: when upgrading to Spring Data 3.x.y, add an assertion on Limit parameter presence as it's done in StringBasedJdbcQuery
		// https://github.com/spring-projects/spring-data-relational/blob/main/spring-data-jdbc/src/main/java/org/springframework/data/jdbc/repository/query/StringBasedJdbcQuery.java#L176
	}
	
	@Override
	protected AbstractQueryExecutor<List<Object>, Object> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters) {
		return new AbstractQueryExecutor<List<Object>, Object>(getQueryMethod()) {
			@Override
			public Supplier<List<Object>> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters) {
				return () -> {
					Limit limit = invocationParameters.getLimit();
					if (limit != null) {
//						((ExecutableEntityQuery) entityQuery).limit(limit.getCount(), limit.getOffset());
					}
//					invocationParameters.getNamedValues().forEach(entityQuery::set);
					
					Map<String, Object> values = new HashMap<>();
					values.putAll(invocationParameters.getNamedValues());
					return (List<Object>) entityQuery.execute(Accumulators.toList());
				};
			}
		};
	}
	
	@Override
	protected LongSupplier buildCountSupplier(StalactiteQueryMethodInvocationParameters invocationParameters) {
		if (countQuery == null) {
			MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
			Executable countQueryAccessor = methodReferenceCapturer.findExecutable(BeanQuery::counterBean);
			throw new IllegalStateException("Count query is mandatory for paged queries, please provide one through " + Reflections.toString(countQueryAccessor));
		} else return () -> {
			Holder<Selectable<Long>> countSelectable = new Holder<>();
			// Looking for the unique selectable in the select, it should be the only one, to make it available below during the values reading phase,
			// because values are only accessible through Selectable objects.
			countQuery.selectInspector(select -> {
				countSelectable.set((Selectable<Long>) Iterables.first(select));
			});
			invocationParameters.getNamedValues().forEach(countQuery::set);
			return countQuery.execute(new Accumulator<Function<Selectable<Long>, Long>, MutableLong, Long>() {
				@Override
				public Supplier<MutableLong> supplier() {
					return MutableLong::new;
				}
				
				@Override
				public BiConsumer<MutableLong, Function<Selectable<Long>, Long>> aggregator() {
					return (modifiableInt, selectableObjectFunction) -> {
						Long apply = selectableObjectFunction.apply(countSelectable.get());
						modifiableInt.reset(apply);
					};
				}
				
				@Override
				public Function<MutableLong, Long> finisher() {
					return MutableLong::getValue;
				}
			});
		};
	}
}
