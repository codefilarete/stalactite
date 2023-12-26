package org.codefilarete.stalactite.query.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
	 * Adds a column to this table.
	 * May do nothing if a column already exists with same name and type.
	 * Will throw an exception if a column with same name but with different type already exists.
	 *
	 * @param expression column name
	 * @param javaType column type
	 * @param <O> column type
	 * @return the created column or the existing one
	 */
	public <O> PseudoColumn<O> addColumn(String expression, Class<O> javaType) {
		return addertColumn(new PseudoColumn<>(this, expression, javaType));
	}
	
	/**
	 * Adds with presence assertion (add + assert = addert, poor naming)
	 *
	 * @param column the column to be added
	 * @param <O> column type
	 * @return given column
	 */
	private <O> PseudoColumn<O> addertColumn(PseudoColumn<O> column) {
		// Quite close to Table.addertColumn(..)
		PseudoColumn<O> existingColumn = findColumn(column.getExpression());
		if (existingColumn != null && (!existingColumn.getJavaType().equals(column.getJavaType()))) {
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
	
	public <O> PseudoColumn<O> registerColumn(String expression, Class<O> javaType) {
		return addColumn(expression, javaType);
	}
	
	public <O> PseudoColumn<O> registerColumn(String expression, Class<O> javaType, String alias) {
		PseudoColumn<O> newColumn = registerColumn(expression, javaType);
		this.aliases.put(newColumn, alias);
		return newColumn;
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
	
	public UnionInFrom asPseudoTable(String name) {
		return new UnionInFrom(name, this);
	}
	
	/**
	 * Overridden to use {@link PseudoColumn} type instead of {@link Column}
	 *
	 * @return columns of this instance per their name and alias
	 */
	@Override
	public Map<String, ? extends Selectable<?>> mapColumnsOnName() {
		Map<String, Selectable<?>> result = new HashMap<>();
		for (PseudoColumn<?> column : getColumns()) {
			result.put(column.getExpression(), column);
		}
		for (Entry<? extends Selectable<?>, String> alias : getAliases().entrySet()) {
			result.put(alias.getValue(), alias.getKey());
		}
		return result;
	}
	
	static public class UnionInFrom implements Fromable {
		
		private final String name;
		
		private final Union union;
		
		private final KeepOrderSet<PseudoColumn<Object>> columns = new KeepOrderSet<>();
		
		private final Map<Selectable<?>, String> aliases = new HashMap<>();
		
		public UnionInFrom(String name, Union union) {
			this.name = name;
			this.union = union;
			Map<Selectable<?>, String> unionAliases = union.getAliases();
			for (PseudoColumn<?> column : union.getColumns()) {
				PseudoColumn<?> newPseudoColumn = new PseudoColumn<>(this, column.getExpression(), column.getJavaType());
				columns.add((PseudoColumn<Object>) newPseudoColumn);
				String alias = unionAliases.get(column);
				if (alias != null) {
					this.aliases.put(newPseudoColumn, alias);
				}
			}
		}
		
		public Union getUnion() {
			return union;
		}
		
		@Override
		public String getName() {
			return name;
		}
		
		@Override
		public String getAbsoluteName() {
			return getName();
		}
		
		@Override
		public Set<PseudoColumn<?>> getColumns() {
			return (Set) this.columns;
		}
		
		@Override
		public Map<Selectable<?>, String> getAliases() {
			return this.aliases;
		}
		
		/**
		 * Overridden to use {@link PseudoColumn} type instead of {@link Column}
		 * 
		 * @return columns of this instance per their name and alias
		 */
		@Override
		public Map<String, ? extends Selectable<?>> mapColumnsOnName() {
			return union.mapColumnsOnName();
		}
	}
	
	public static class PseudoColumn<O> implements Selectable<O>, JoinLink<UnionInFrom, O> {
		
		private final SelectablesPod union;	// Union or UnionInFrom
		
		private final String name;
		
		private final Class<O> javaType;
		
		
		private PseudoColumn(SelectablesPod union, String name, Class<O> javaType) {
			this.union = union;
			this.name = name;
			this.javaType = javaType;
		}
		
		/**
		 * To be called only for pseudo-column in a {@link UnionInFrom}, else would throw a {@link ClassCastException}
		 * @return UnionInFrom owning this column
		 */
		@Override
		public UnionInFrom getOwner() {
			return (UnionInFrom) union;
		}
		
		@Override
		public String getExpression() {
			return name;
		}
		
		@Override
		public Class<O> getJavaType() {
			return javaType;
		}
	}
}
