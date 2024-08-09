package org.codefilarete.stalactite.query.model;

import java.util.Iterator;
import java.util.Map;

import org.codefilarete.stalactite.query.model.Selectable.SelectableString;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.reflect.MethodDispatcher;

/**
 * A support for the select part of a SQL query
 *
 * @author Guillaume Mary
 */
public class Select implements FluentSelect<Select> {
	
	/** Items in select and their aliases (null value means no alias) */
	private final KeepOrderMap<Selectable<?>, String> columns = new KeepOrderMap<>();
	
	private boolean distinct = false;
	
	@Override
	public KeepOrderSet<Selectable<?>> getColumns() {
		return new KeepOrderSet<>(columns.keySet());
	}
	
	@Override
	public Select add(Iterable<? extends Selectable<?>> expressions) {
		expressions.forEach(this::add);
		return this;
	}
	
	@Override
	public Select add(Selectable<?> expression, Selectable<?>... expressions) {
		add(expression);
		for (Selectable<?> expr : expressions) {
			add(expr);
		}
		return this;
	}
	
	@Override
	public AliasableExpression<Select> add(String expression, Class<?> javaType) {
		SelectableString<?> selectable = new SelectableString<>(expression, javaType);
		add(selectable);
		return new MethodDispatcher()
				.redirect(Aliasable.class, alias -> {
					columns.put(selectable, alias);
					return null;	// we don't care about returned object since proxy is returned
				}, this)
				.redirect(SelectChain.class, this)
				.build((Class<AliasableExpression<Select>>) (Class<?>) AliasableExpression.class);
	}
	
	@Override
	public Select add(Selectable<?> column, String alias) {
		this.columns.put(column, alias);
		return this;
	}
	
	@Override
	public Select add(Map<? extends Selectable<?>, String> aliasedColumns) {
		this.columns.putAll(aliasedColumns);
		return this;
	}
	
	public Select remove(Selectable<?> selectable) {
		this.columns.remove(selectable);
		return this;
	}
	
	/**
	 * Gives column aliases. If a {@link Column} is not present in result, then it means it has no alias
	 * 
	 * @return {@link Column} aliases of this instance, an empty {@link Map} if no {@link Column} was added to this instance
	 */
	public Map<Selectable<?>, String> getAliases() {
		// TODO : remove this computation by a storage of aliased to avoid this algorithm at each call and complexity about ConcurrentModificationException
		KeepOrderMap<Selectable<?>, String> result = new KeepOrderMap<>();
		// don't use constructor parameter instead of putAll(..) because it gives it as surrogate, then, since we remove elements afterward,
		// it throws ConcurrentModificationException while rendering SQL which need iteration over elements and access to aliases
		result.putAll(columns);
		result.values().remove(null);
		return result;
	}
	
	public boolean isDistinct() {
		return distinct;
	}
	
	@Override
	public Select distinct() {
		this.distinct = true;
		return this;
	}
	
	public void setDistinct(boolean distinct) {
		this.distinct = distinct;
	}
	
	public KeepOrderMap<Selectable<?>, String> clear() {
		KeepOrderMap<Selectable<?>, String> result = new KeepOrderMap<>(this.columns);
		this.columns.clear();
		return result;
	}
	
	@Override
	public Iterator<Selectable<?>> iterator() {
		return this.columns.keySet().iterator();
	}
}
