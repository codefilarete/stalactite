package org.codefilarete.stalactite.query.model;

import java.util.Iterator;
import java.util.Map;

import org.codefilarete.stalactite.query.model.Selectable.SimpleSelectable;
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
	
	public Map<Selectable<?>, String> getColumnPerAlias() {
		return columns;
	}
	
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
		SimpleSelectable<?> selectable = new SimpleSelectable<>(expression, javaType);
		add(selectable);
		return new MethodDispatcher()
				.redirect(Aliasable.class, alias -> {
					columns.put(selectable, alias);
					return null;	// we don't care about the returned object since a proxy is returned
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
	 * Gives column aliases. If a {@link Selectable} refers to a null value in returned {@link Map}, it means it has no alias
	 * 
	 * @return {@link Selectable} aliases of this instance
	 */
	@Override
	public Map<Selectable<?>, String> getAliases() {
		return new KeepOrderMap<>(columns);
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
