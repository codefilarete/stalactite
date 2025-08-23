package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.result.Accumulators;

public class QueryResultSingler<R, I> implements QueryResultReducer<R, I> {

	public QueryResultSingler() {
	}

	@Override
	public Function<Object[], R> adapt(Supplier<List<I>> resultSupplier) {
		return objects -> (R) Accumulators.<I>getFirstUnique().collect(resultSupplier.get());
	}

}
