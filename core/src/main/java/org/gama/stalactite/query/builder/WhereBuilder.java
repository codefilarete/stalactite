package org.gama.stalactite.query.builder;

import java.util.Map;

import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.OperandBuilder.PreparedSQLWrapper;
import org.gama.stalactite.query.builder.OperandBuilder.SQLAppender;
import org.gama.stalactite.query.builder.OperandBuilder.StringAppenderWrapper;
import org.gama.stalactite.query.model.AbstractCriterion;
import org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator;
import org.gama.stalactite.query.model.ColumnCriterion;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Operand;
import org.gama.stalactite.query.model.RawCriterion;

/**
 * A class made to print a where clause (widened to {@link CriteriaChain})
 * 
 * @author Guillaume Mary
 */
public class WhereBuilder implements SQLBuilder, PreparedSQLBuilder {

	public static final String AND = "and";
	public static final String OR = "or";

	private final CriteriaChain where;
	
	private final DMLNameProvider dmlNameProvider;
	
	public WhereBuilder(CriteriaChain where, Map<Table, String> tableAliases) {
		this(where, new DMLNameProvider(tableAliases));
	}
	
	public WhereBuilder(CriteriaChain where, DMLNameProvider dmlNameProvider) {
		this.where = where;
		this.dmlNameProvider = dmlNameProvider;
	}
	
	@Override
	public String toSQL() {
		return appendSQL(new StringAppender());
	}
	
	public String appendSQL(StringAppender sql) {
		return appendSQL(new StringAppenderWrapper(sql, dmlNameProvider));
	}
	
	public String appendSQL(SQLAppender sql) {
		WhereAppender whereAppender = new WhereAppender(sql, dmlNameProvider);
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
		WhereAppender whereAppender = new WhereAppender(preparedSQLWrapper, dmlNameProvider);
		whereAppender.cat(where);
		PreparedSQL result = new PreparedSQL(preparedSQLWrapper.getSQL(), preparedSQLWrapper.getParameterBinders());
		result.setValues(preparedSQLWrapper.getValues());
		return result;
	}
	
	public static class WhereAppender {
		
		private final SQLAppender sql;
		
		private final OperandBuilder operandBuilder;
		
		private final DMLNameProvider dmlNameProvider;
		
		public WhereAppender(SQLAppender sql, DMLNameProvider dmlNameProvider) {
			this.sql = sql;
			this.operandBuilder = new OperandBuilder(dmlNameProvider);
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
				} else if (o instanceof Operand) {
					cat((Operand) o);
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
			} else if (o instanceof Operand) {
				cat(criterion.getColumn(), (Operand) o);
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
		
		public void cat(Operand operand) {
			operandBuilder.cat(operand, sql);
		}
		
		public void cat(Column column, Operand operand) {
			operandBuilder.cat(column, operand, sql);
		}
		
		public String getName(LogicalOperator operator) {
			switch (operator) {
				case And:
					return AND;
				case Or:
					return OR;
				default:
					throw new IllegalArgumentException("Operator " + operator + " is unknown");
			}
		}
		
	}
}
