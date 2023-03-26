package org.codefilarete.stalactite.query.builder;

import java.util.Map;

import org.codefilarete.stalactite.query.model.*;
import org.codefilarete.stalactite.query.model.AbstractCriterion.LogicalOperator;
import org.codefilarete.stalactite.query.model.operator.SQLFunction;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.VisibleForTesting;

/**
 * A class made to print a where clause (widened to {@link CriteriaChain})
 * 
 * @author Guillaume Mary
 */
public class WhereSQLBuilder implements SQLBuilder, PreparedSQLBuilder {

	public static final String AND = "and";
	public static final String OR = "or";

	private final CriteriaChain where;
	
	private final DMLNameProvider dmlNameProvider;
	private final Dialect dialect;
	
	@VisibleForTesting
	WhereSQLBuilder(CriteriaChain where, Map<Table, String> tableAliases) {
		this(where, new DMLNameProvider(tableAliases), new Dialect());
	}
	
	public WhereSQLBuilder(CriteriaChain where, DMLNameProvider dmlNameProvider, Dialect dialect) {
		this.where = where;
		this.dmlNameProvider = dmlNameProvider;
		this.dialect = dialect;
	}
	
	@Override
	public String toSQL() {
		return appendSQL(new StringAppender());
	}
	
	public String appendSQL(StringAppender sql) {
		return appendSQL(new StringAppenderWrapper(sql, dmlNameProvider));
	}
	
	public String appendSQL(SQLAppender sql) {
		WhereAppender whereAppender = new WhereAppender(sql, dmlNameProvider, dialect);
		whereAppender.cat(where);
		return sql.getSQL();
	}
	
	@Override
	public PreparedSQL toPreparedSQL(ColumnBinderRegistry parameterBinderRegistry) {
		return toPreparedSQL(new StringAppender(), parameterBinderRegistry);
	}
	
	public PreparedSQL toPreparedSQL(StringAppender sql, ColumnBinderRegistry parameterBinderRegistry) {
		PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(new StringAppenderWrapper(sql, dmlNameProvider), parameterBinderRegistry, dmlNameProvider);
		return toPreparedSQL(preparedSQLWrapper);
	}
	
	public PreparedSQL toPreparedSQL(PreparedSQLWrapper preparedSQLWrapper) {
		WhereAppender whereAppender = new WhereAppender(preparedSQLWrapper, dmlNameProvider, dialect);
		whereAppender.cat(where);
		PreparedSQL result = new PreparedSQL(preparedSQLWrapper.getSQL(), preparedSQLWrapper.getParameterBinders());
		result.setValues(preparedSQLWrapper.getValues());
		return result;
	}
	
	public static class WhereAppender {
		
		private final SQLAppender sql;
		
		private final OperatorSQLBuilder operatorSqlBuilder;
		
		private final FunctionSQLBuilder functionSQLBuilder;
		
		private final DMLNameProvider dmlNameProvider;
		
		public WhereAppender(SQLAppender sql, DMLNameProvider dmlNameProvider, Dialect dialect) {
			this.sql = sql;
			this.operatorSqlBuilder = new OperatorSQLBuilder();
			this.functionSQLBuilder = new FunctionSQLBuilder(dmlNameProvider, dialect.getSqlTypeRegistry().getJavaTypeToSqlTypeMapping());
			this.dmlNameProvider = dmlNameProvider;
		}
		
		public void cat(Object o) {
			if (o instanceof CharSequence) {
				sql.cat(o.toString());
			} else if (o instanceof CriteriaChain) {
				sql.cat("(");
				cat((CriteriaChain) o);
				sql.cat(")");
			} else if (o instanceof RawCriterion) {
				cat((RawCriterion) o);
			} else if (o instanceof ColumnCriterion) {
				cat((ColumnCriterion) o);
			}
		}
		
		public void cat(CriteriaChain criteria) {
			boolean isNotFirst = false;
			for (Object criterion : criteria) {
				if (isNotFirst) {
					cat(((AbstractCriterion) criterion).getOperator());
				} else {
					isNotFirst = true;
				}
				cat(criterion);
			}
		}
		
		public void cat(RawCriterion criterion) {
			for (Object o : criterion.getCondition()) {
				if (o instanceof ColumnCriterion) {
					cat((ColumnCriterion) o);
				} else if (o instanceof CharSequence) {
					sql.cat(o.toString());
				} else if (o instanceof CriteriaChain) {
					sql.cat("(");
					cat((CriteriaChain) o);
					sql.cat(")");
				} else if (o instanceof Column) {
					cat((Column) o);
				} else if (o instanceof TupleIn) {	// "if" to be done before "if" about ConditionalOperator to take inheritance into account  
					catTupledIn((TupleIn) o);
				} else if (o instanceof ConditionalOperator) {
					cat((ConditionalOperator) o);
				} else if (o instanceof SQLFunction) {	// made for having(sum(col), eq(..))
					cat((SQLFunction) o);
				} else {
					throw new IllegalArgumentException("Unknown criterion type " + Reflections.toString(o.getClass()));
				}
			}
		}
		
		public void cat(ColumnCriterion criterion) {
			cat(criterion.getColumn());
			sql.cat(" ");
			Object o = criterion.getCondition();
			if (o instanceof CharSequence) {
				sql.cat(o.toString());
			} else if (o instanceof ConditionalOperator) {
				cat(criterion.getColumn(), (ConditionalOperator) o);
			} else {
				throw new IllegalArgumentException("Unknown criterion type " + Reflections.toString(o.getClass()));
			}
		}
		
		public void cat(Column column) {
			// delegated to dmlNameProvider
			sql.cat(this.dmlNameProvider.getName(column));
		}
		
		public void cat(LogicalOperator operator) {
			if (operator != null) {
				sql.cat(" ", getName(operator), " ");
			}
		}
		
		public void cat(ConditionalOperator operator) {
			operatorSqlBuilder.cat(operator, sql);
		}
		
		public void catTupledIn(TupleIn operator) {
			operatorSqlBuilder.catTupledIn(operator, sql);
		}
		
		public void cat(SQLFunction sqlFunction) {
			functionSQLBuilder.cat(sqlFunction, sql);
		}
		
		public void cat(Column column, ConditionalOperator operator) {
			operatorSqlBuilder.cat(column, operator, sql);
		}
		
		public String getName(LogicalOperator operator) {
			switch (operator) {
				case AND:
					return AND;
				case OR:
					return OR;
				default:
					throw new IllegalArgumentException("Operator " + operator + " is unknown");
			}
		}
		
	}
}
