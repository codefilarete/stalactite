package org.codefilarete.stalactite.engine.runtime;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;

import org.codefilarete.stalactite.query.model.OrderByChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Query.FluentOrderByClause;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Union.PseudoColumn;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * Wrapper around an {@link OrderByChain} that takes the {@link Column} from the clones {@link Map}.
 * Made to take {@link Column} aliases of a {@link Query} into account when caller doesn't know them.
 *
 * @author Guillaume Mary
 */
class ColumnCloneAwareOrderBy implements OrderByChain {
	
	private final OrderByChain delegate;
	private final IdentityHashMap<Selectable<?>, Selectable<?>> columnClones;
	
	ColumnCloneAwareOrderBy(FluentOrderByClause delegate, IdentityHashMap<Selectable<?>, Selectable<?>> columnClones) {
		this.delegate = delegate;
		this.columnClones = columnClones;
	}
	
	@Override
	public OrderByChain add(Selectable column, Order order) {
		return delegate.add(getColumn(column), order);
	}
	
	private Selectable getColumn(Selectable column) {
		if (column instanceof Column || column instanceof PseudoColumn) {
			return columnClones.get(column);
		} else {
			// function case
			return column;
		}
	}
	
	@Override
	public OrderByChain add(Selectable col1, Order order1, Selectable col2, Order order2) {
		return delegate.add(
				getColumn(col1), order1,
				getColumn(col2), order2);
	}
	
	@Override
	public OrderByChain add(Selectable col1, Order order1, Selectable col2, Order order2, Selectable col3, Order order3) {
		return delegate.add(
				getColumn(col1), order1,
				getColumn(col2), order2,
				getColumn(col3), order3);
	}
	
	@Override
	public OrderByChain add(String column, Order order) {
		return delegate.add(column, order);
	}
	
	@Override
	public OrderByChain add(String col1, Order order1, String col2, Order order2) {
		return delegate.add(col1, order1, col2, order2);
	}
	
	@Override
	public OrderByChain add(String col1, Order order1, String col2, Order order2, String col3, Order order3) {
		return delegate.add(col1, order1, col2, order2, col3, order3);
	}
	
	@Override
	public OrderByChain add(Selectable column, Selectable... columns) {
		return delegate.add(
				getColumn(column),
				Arrays.stream(columns).map(this::getColumn).toArray(Column[]::new));
	}
	
	@Override
	public OrderByChain add(String column, String... columns) {
		return delegate.add(column, columns);
	}
}
