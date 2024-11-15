package org.codefilarete.stalactite.query.model;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Defines a SQL Union between several queries.
 * Available {@link Column}s as selectable elements must be declared through {@link #registerColumn(String, Class)} 
 * 
 * @author Guillaume Mary
 */
public class Union implements QueryStatement, UnionAware, QueryProvider<Union> {
	
	private final KeepOrderSet<Query> queries;
	
	private final Map<Selectable<?>, String> aliases = new HashMap<>();
	
	private final KeepOrderSet<PseudoColumn<Object>> columns = new KeepOrderSet<>();
	
	public Union(Collection<Query> queries) {
		this.queries = new KeepOrderSet<>(queries);
	}
	
	public Union(Query ... queries) {
		this.queries = new KeepOrderSet<>(queries);
	}
	
	public KeepOrderSet<Query> getQueries() {
		return queries;
	}
	
	@Override
	public Union getQuery() {
		return this;
	}
	
	/**
	 * Declares a column to this union.
	 * May do nothing if a column already exists with same name and type.
	 * Will throw an exception if a column with same name but with different type already exists.
	 *
	 * @param expression column name
	 * @param javaType column type
	 * @param <O> column type
	 * @return the created column or the existing one
	 */
	public <O> PseudoColumn<O> addColumn(String expression, Class<O> javaType) {
		return addColumn(expression, javaType, null);
	}
	
	/**
	 * Declares a column to this union with an alias.
	 * May do nothing if a column already exists with same name and type.
	 * Will throw an exception if a column with same name but with different type already exists.
	 *
	 * @param expression column name
	 * @param javaType column type
	 * @param alias column alias (optional)
	 * @param <O> column type
	 * @return the created column or the existing one
	 */
	public <O> PseudoColumn<O> addColumn(String expression, Class<O> javaType, @Nullable String alias) {
		return addertColumn(new PseudoColumn<>(this, expression, javaType), alias);
	}
	
	/**
	 * Adds a column with presence assertion (add + assert = addert, poor naming)
	 *
	 * @param <O> column type
	 * @param column the column to be added
	 * @param alias column alias (optional)
	 * @return given column
	 */
	private <O> PseudoColumn<O> addertColumn(PseudoColumn<O> column, @Nullable String alias) {
		// Quite close to Table.addertColumn(..)
		PseudoColumn<O> existingColumn = findColumn(column.getExpression(), alias);
		if (existingColumn != null && (Objects.equals(alias, aliases.get(existingColumn))) && !existingColumn.getJavaType().equals(column.getJavaType())) {
			throw new IllegalArgumentException("Trying to add a column '" + existingColumn.getExpression() + "' that already exists with a different type : "
													   + Reflections.toString(existingColumn.getJavaType()) + " vs " + Reflections.toString(column.getJavaType()));
		}
		if (existingColumn == null) {
			columns.add((PseudoColumn<Object>) column);
			return column;
		} else {
			return existingColumn;
		}
	}
	
	private <C extends Selectable<?>> C findColumn(String columnName, @Nullable String alias) {
		if (alias != null) {
			for (Entry<? extends Selectable<?>, String> aliasPawn : getAliases().entrySet()) {
				if (aliasPawn.getValue().equals(alias)) {
					return (C) aliasPawn.getKey();
				}
			}
		} else {
			for (Selectable<?> column : getColumns()) {
				if (column instanceof JoinLink && column.getExpression().equals(columnName)) {
					return (C) column;
				}
			}
		}
		return null;
	}
	
	public <O> PseudoColumn<O> registerColumn(String expression, Class<O> javaType) {
		return addColumn(expression, javaType);
	}
	
	public <O> PseudoColumn<O> registerColumn(String expression, Class<O> javaType, String alias) {
		return addColumn(expression, javaType, alias);
	}
	
	@Override
	public Set<PseudoColumn<?>> getColumns() {
		return (Set) columns;
	}
	
	@Override
	public Map<Selectable<?>, String> getAliases() {
		return aliases;
	}
	
	@Override
	public Union unionAll(QueryProvider<Query> query) {
		queries.add(query.getQuery());
		return this;
	}
	
	/**
	 * @return columns of this instance per their name and alias
	 */
	@Override
	public Map<String, Selectable<?>> mapColumnsOnName() {
		Map<String, Selectable<?>> result = new HashMap<>();
		for (PseudoColumn<?> column : getColumns()) {
			result.put(column.getExpression(), column);
		}
		for (Entry<? extends Selectable<?>, String> alias : getAliases().entrySet()) {
			result.put(alias.getValue(), alias.getKey());
		}
		return result;
	}
}
