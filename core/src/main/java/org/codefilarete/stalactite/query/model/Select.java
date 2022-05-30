package org.codefilarete.stalactite.query.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.codefilarete.stalactite.query.model.Selectable.SelectableString;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * A support for the select part of a SQL query
 *
 * @author Guillaume Mary
 */
public class Select implements Iterable<Selectable /* String, Column or AliasedColumn */>, SelectChain<Select>, SelectablesPod {
	
	/** String, Column or {@link AliasedColumn} */
	private final KeepOrderSet<Selectable> columns = new KeepOrderSet<>();
	
	private boolean distinct = false;
	
	@Override
	public KeepOrderSet<Selectable> getColumns() {
		return columns;
	}
	
	private Select add(Selectable column) {
		this.columns.add(column);
		return this;
	}
	
	@Override
	public Select add(Iterable<? extends Selectable> expressions) {
		expressions.forEach(this::add);
		return this;
	}
	
	@Override
	public Select add(Selectable expression, Selectable... expressions) {
		add(expression);
		for (Selectable expr : expressions) {
			add(expr);
		}
		return this;
	}
	
	@Override
	public Select add(String expression, String... expressions) {
		add(new SelectableString(expression));
		for (String expr : expressions) {
			add(new SelectableString(expr));
		}
		return this;
	}
	
	@Override
	public Select add(Column<?, ?> column, String alias) {
		return add(new AliasedColumn(column, alias));
	}
	
	@Override
	public Select add(Map<Column, String> aliasedColumns) {
		for (Entry<Column, String> aliasedColumn : aliasedColumns.entrySet()) {
			add(new AliasedColumn(aliasedColumn.getKey(), aliasedColumn.getValue()));
		}
		return this;
	}
	
	/**
	 * Gives column aliases : works for {@link Column} declared through {@link #add(Column, String)} and {@link #add(Map)}.
	 * 
	 * @return {@link Column} aliases of this instance, an empty {@link Map} if no {@link Column} was added to this instance
	 */
	public Map<Column, String> giveColumnAliases() {
		Map<Column, String> aliases = new HashMap<>();
		for (Object column : columns) {
			if (column instanceof AliasedColumn) {
				aliases.put(((AliasedColumn) column).getColumn(), ((AliasedColumn) column).getAlias());
			}
		}
		return aliases;
	}
	
	public boolean isDistinct() {
		return distinct;
	}
	
	@Override
	public Select distinct() {
		this.distinct = true;
		return this;
	}
	
	public void removeAt(int index) {
		this.columns.removeAt(index);
	}
	
	public KeepOrderSet<Selectable> clear() {
		KeepOrderSet<Selectable> result = new KeepOrderSet<>(this.columns);
		this.columns.clear();
		return result;
	}
	
	@Override
	public Iterator<Selectable> iterator() {
		return this.columns.iterator();
	}
	
	public static class AliasedColumn<O> extends Aliased implements Selectable {
		
		private final Column<?, O> column;
		
		public AliasedColumn(Column<?, O> column, String alias) {
			super(alias);
			this.column = column;
		}
		
		public Column<?, O> getColumn() {
			return column;
		}
		
		@Override
		public String getExpression() {
			return column.getName();
		}
	}
	
}
