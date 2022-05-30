package org.codefilarete.stalactite.query.model;

import java.util.Collections;
import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Defines a SQL Union between several queries.
 * Available {@link Column}s as selectable elements must be declared through {@link #registerColumn(String, Class)} 
 * 
 * @author Guillaume Mary
 */
public class Union implements QueryStatement, UnionAware, QueryProvider<Union> {
	
	private final KeepOrderSet<Query> queries;
	
	/** The "false" Table than can represent this Union in a From clause */
	private final Table<?> pseudoTable;
	
	public Union(Query firstQuery, Query ... otherQueries) {
		this.pseudoTable = new Table<>("");
		this.queries = new KeepOrderSet<>(firstQuery);
		Collections.addAll(this.queries, otherQueries);
	}
	
	public KeepOrderSet<Query> getQueries() {
		return queries;
	}
	
	@Override
	public Union getQuery() {
		return this;
	}
	
	public <O> Column<?, O> registerColumn(String name, Class<O> javaType) {
		return this.pseudoTable.addColumn(name, javaType);
	}
	
	@Override
	public Set<Selectable> getColumns() {
		return (Set) this.pseudoTable.getColumns();
	}
	
	@Override
	public Union unionAll(QueryProvider<Query> query) {
		queries.add(query.getQuery());
		return this;
	}
	
	public Fromable asPseudoTable(String name) {
		return new UnionInFrom(name, this);
	}
	
	static public class UnionInFrom implements Fromable {
		
		private final String name;
		
		private final Union union;
		
		public UnionInFrom(String name, Union union) {
			this.name = name;
			this.union = union;
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
			return name;
		}
	}
}
