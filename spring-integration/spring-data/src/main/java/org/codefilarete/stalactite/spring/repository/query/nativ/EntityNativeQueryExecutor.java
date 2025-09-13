package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.runtime.RelationalEntityFinder;
import org.codefilarete.stalactite.spring.repository.query.AbstractQueryExecutor;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethodInvocationParameters;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.sql.Dialect;

/**
 * Classes for cases where the SQL given by @{@link org.codefilarete.stalactite.spring.repository.query.NativeQuery} annotation is expected to give
 * a result that is some entities.
 * 
 * @param <C> entity type
 * @author Guillaume Mary
 */
public class EntityNativeQueryExecutor<C> extends AbstractQueryExecutor<List<C>, C> {

	private final String sql;
	private final RelationalEntityFinder<C, ?, ?> relationalEntityFinder;

	public EntityNativeQueryExecutor(StalactiteQueryMethod method,
									 String sql,
									 RelationalEntityFinder<C, ?, ?> relationalEntityFinder,
									 Dialect dialect) {
		super(method, dialect);
		this.sql = sql;
		this.relationalEntityFinder = relationalEntityFinder;
	}

	@Override
	public Supplier<List<C>> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters) {
		return () -> {
			Set<C> cs = relationalEntityFinder.selectFromQueryBean(sql, invocationParameters.getNamedValues(), invocationParameters.bindParameters(dialect));
			return new ArrayList<>(cs);
		};
	}
}