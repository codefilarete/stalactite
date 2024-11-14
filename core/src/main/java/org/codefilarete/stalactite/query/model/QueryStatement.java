package org.codefilarete.stalactite.query.model;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @author Guillaume Mary
 */
public interface QueryStatement extends SelectablesPod {
	
	Set<Query> getQueries();
	
	/**
	 * Wraps current instance to make it appendable into a From clause
	 * @return an instance containing current instance and appendable into a From clause
	 */
	default QueryInFrom asPseudoTable() {
		return asPseudoTable(null);
	}
	
	/**
	 * Wraps current instance to make it appendable into a From clause
	 * 
	 * @param name an optional name that serves as an alias of this instance into the From clause
	 * @return an instance containing current instance and appendable into a From clause
	 */
	default QueryInFrom asPseudoTable(@Nullable String name) {
		return new QueryInFrom(this, name);
	}
	
	class QueryInFrom implements Fromable {
		
		@Nullable
		private final String name;
		
		private final QueryStatement queryStatement;
		
		private final KeepOrderSet<PseudoColumn<Object>> columns = new KeepOrderSet<>();
		
		private final Map<Selectable<?>, String> aliases = new HashMap<>();
		
		public QueryInFrom(QueryStatement queryStatement, @Nullable String name) {
			this.name = name;
			this.queryStatement = queryStatement;
			Map<Selectable<?>, String> unionAliases = queryStatement.getAliases();
			for (Selectable<?> column : queryStatement.getColumns()) {
				PseudoColumn<?> newPseudoColumn = new PseudoColumn<>(this, column.getExpression(), column.getJavaType());
				columns.add((PseudoColumn<Object>) newPseudoColumn);
				String alias = unionAliases.get(column);
				if (alias != null) {
					this.aliases.put(newPseudoColumn, alias);
				}
			}
		}
		
		public QueryStatement getQueryStatement() {
			return queryStatement;
		}
		
		@Nullable
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
		 * @return union columns per name
		 */
		@Override
		public Map<String, ? extends Selectable<?>> mapColumnsOnName() {
			return queryStatement.mapColumnsOnName();
		}
	}
	
	class PseudoColumn<O> implements Selectable<O>, JoinLink<Fromable, O> {
		
		private final SelectablesPod union;	// Union or UnionInFrom
		
		private final String name;
		
		private final Class<O> javaType;
		
		
		public PseudoColumn(SelectablesPod union, String name, Class<O> javaType) {
			this.union = union;
			this.name = name;
			this.javaType = javaType;
		}
		
		/**
		 * To be called only for pseudo-column in a {@link QueryInFrom}, else would throw a {@link ClassCastException}
		 * @return {@link QueryInFrom} owning this column
		 */
		@Override
		public Fromable getOwner() {
			return (Fromable) union;
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
