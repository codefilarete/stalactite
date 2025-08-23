package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.runtime.RelationalEntityFinder;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.spring.repository.query.projection.StalactiteParametersParameterAccessor;
import org.codefilarete.stalactite.sql.Dialect;

public class BeanNativeQueryExecutor<C> extends AbstractNativeQueryExecutor<List<C>, C> {

	private final String sql;
	private final RelationalEntityFinder<C, ?, ?> relationalEntityFinder;

	public BeanNativeQueryExecutor(StalactiteQueryMethod method,
								   String sql,
								   RelationalEntityFinder<C, ?, ?> relationalEntityFinder,
								   Dialect dialect) {
		super(method, dialect);
		this.sql = sql;
		this.relationalEntityFinder = relationalEntityFinder;
	}

	@Override
	public Supplier<List<C>> buildQueryExecutor(Object[] parameters) {
		StalactiteParametersParameterAccessor accessor = new StalactiteParametersParameterAccessor(method.getParameters(), parameters);
		
		return () -> {
			Set<C> cs = relationalEntityFinder.selectFromQueryBean(sql, accessor.getNamedValues(), bindParameters(accessor));
			return new ArrayList<>(cs);
		};
	}
}