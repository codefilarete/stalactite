package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.query.builder.FromSQLBuilderFactory;
import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory;
import org.codefilarete.stalactite.query.builder.OperatorSQLBuilderFactory;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.builder.SelectSQLBuilderFactory;
import org.codefilarete.stalactite.query.builder.WhereSQLBuilderFactory;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;

/**
 * A fluent builder made to create a {@link QuerySQLBuilderFactory}.
 * Made as such, instead of having a bunch of factories, due to the nature of {@link QuerySQLBuilder} : it embeds
 * several factories, one per query clause, and some are reused between each other. Hence, all this requires some
 * algorithm to assembly the final factory.
 * 
 * @author Guillaume Mary
 * @see #build()
 */
public class QuerySQLBuilderFactoryBuilder {
	
	protected final ColumnBinderRegistry parameterBinderRegistry;
	protected final JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;
	protected SelectSQLBuilderFactory selectBuilderFactory;
	protected FromSQLBuilderFactory fromSqlBuilderFactory;
	protected WhereSQLBuilderFactory whereSqlBuilderFactory;
	protected WhereSQLBuilderFactory havingBuilderFactory;
	protected OperatorSQLBuilderFactory operatorSQLBuilderFactory;
	protected FunctionSQLBuilderFactory functionSQLBuilderFactory;
	
	public QuerySQLBuilderFactoryBuilder(ColumnBinderRegistry parameterBinderRegistry, JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.javaTypeToSqlTypeMapping = javaTypeToSqlTypeMapping;
	}
	
	public QuerySQLBuilderFactoryBuilder withSelectBuilderFactory(SelectSQLBuilderFactory selectBuilderFactory) {
		this.selectBuilderFactory = selectBuilderFactory;
		return this;
	}
	
	public QuerySQLBuilderFactoryBuilder withFromSqlBuilderFactory(FromSQLBuilderFactory fromSqlBuilderFactory) {
		this.fromSqlBuilderFactory = fromSqlBuilderFactory;
		return this;
	}
	
	public QuerySQLBuilderFactoryBuilder withWhereSqlBuilderFactory(WhereSQLBuilderFactory whereSqlBuilderFactory) {
		this.whereSqlBuilderFactory = whereSqlBuilderFactory;
		return this;
	}
	
	public QuerySQLBuilderFactoryBuilder withHavingBuilderFactory(WhereSQLBuilderFactory havingBuilderFactory) {
		this.havingBuilderFactory = havingBuilderFactory;
		return this;
	}
	
	public QuerySQLBuilderFactoryBuilder withOperatorSQLBuilderFactory(OperatorSQLBuilderFactory operatorSQLBuilderFactory) {
		this.operatorSQLBuilderFactory = operatorSQLBuilderFactory;
		return this;
	}
	
	public QuerySQLBuilderFactoryBuilder withFunctionSQLBuilderFactory(FunctionSQLBuilderFactory functionSQLBuilderFactory) {
		this.functionSQLBuilderFactory = functionSQLBuilderFactory;
		return this;
	}
	
	public QuerySQLBuilderFactory build() {
		if (operatorSQLBuilderFactory == null) {
			withOperatorSQLBuilderFactory(new OperatorSQLBuilderFactory());
		}
		
		if (functionSQLBuilderFactory == null) {
			withFunctionSQLBuilderFactory(new FunctionSQLBuilderFactory(javaTypeToSqlTypeMapping));
		}
		
		if (whereSqlBuilderFactory == null) {
			withWhereSqlBuilderFactory(new WhereSQLBuilderFactory(parameterBinderRegistry, operatorSQLBuilderFactory, functionSQLBuilderFactory));
		}
		
		if (havingBuilderFactory == null) {
			withHavingBuilderFactory(whereSqlBuilderFactory);
		}
		
		if (fromSqlBuilderFactory == null) {
			withFromSqlBuilderFactory(new FromSQLBuilderFactory());
		}
		
		if (selectBuilderFactory == null) {
			withSelectBuilderFactory(new SelectSQLBuilderFactory(functionSQLBuilderFactory));
		}
		return new QuerySQLBuilderFactory(parameterBinderRegistry, selectBuilderFactory, fromSqlBuilderFactory, whereSqlBuilderFactory, havingBuilderFactory);
	}
}
