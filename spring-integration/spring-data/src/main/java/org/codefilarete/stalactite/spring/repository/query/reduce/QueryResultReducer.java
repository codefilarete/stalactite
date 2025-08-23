package org.codefilarete.stalactite.spring.repository.query.reduce;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Parent interface for classes that reduce the result of a query.
 * Those classes are expected to handle chunk cases (Slice, Page) and usual cases like Collection or simple single result.
 * 
 * @param <R> final result type
 * @param <I> input coming from the query execution
 * @author Guillaume Mary
 */
public interface QueryResultReducer<R, I> {

	Function<Object[], R> adapt(Supplier<List<I>> resultSupplier);
}
