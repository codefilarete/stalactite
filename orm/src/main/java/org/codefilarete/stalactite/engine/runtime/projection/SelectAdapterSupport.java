package org.codefilarete.stalactite.engine.runtime.projection;

import java.util.List;
import java.util.Set;

import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityPersister.SelectAdapter;
import org.codefilarete.stalactite.engine.runtime.query.AggregateAccessPointToColumnMapping;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.api.Selectable;

/**
 * Implementation of {@link SelectAdapter} that wraps {@link Select} to build its select clause.
 * 
 * @param <C>
 * @author Guillaume Mary
 */
public class SelectAdapterSupport<C> implements SelectAdapter<C> {
	
	private final Select select;
	private final AggregateAccessPointToColumnMapping<C> accessPointToColumn;
	
	public SelectAdapterSupport(Select select, AggregateAccessPointToColumnMapping<C> accessPointToColumn) {
		this.select = select;
		this.accessPointToColumn = accessPointToColumn;
		// we clear the select clause because in most of the cases we create a brand new object from the query, and
		// "old" columns pollute the select clause
		this.select.clear();
	}
	
	@Override
	public Set<Selectable<?>> getColumns() {
		return select.getColumns();
	}
	
	@Override
	public SelectAdapter<C> distinct() {
		this.select.distinct();
		return this;
	}
	
	@Override
	public SelectAdapter<C> setDistinct(boolean distinct) {
		this.select.setDistinct(distinct);
		return this;
	}
	
	@Override
	public SelectAdapter<C> add(Selectable<?> column) {
		this.select.add(column);
		return this;
	}
	
	@Override
	public SelectAdapter<C> add(Selectable<?> column, String alias) {
		this.select.add(column, alias);
		return this;
	}
	
	@Override
	public Selectable<?> giveColumn(List<ValueAccessPoint<?>> property) {
		return accessPointToColumn.giveColumn(property);
	}
}
