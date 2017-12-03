package org.gama.stalactite.query.builder;

import java.util.Map;

import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.sql.binder.ParameterBinderRegistry;
import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.builder.OperandBuilder.PreparedSQLWrapper;
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
public class WhereBuilder extends AbstractDMLBuilder implements PreparedSQLBuilder {

	public static final String AND = "and";
	public static final String OR = "or";

	private final CriteriaChain where;
	
	private final OperandBuilder operandBuilder = new OperandBuilder();
	
	private final StringAppender sql = new StringAppender();
	
	public WhereBuilder(CriteriaChain where, Map<Table, String> tableAliases) {
		super(tableAliases);
		this.where = where;
	}
	
	@Override
	public String toSQL() {
		return toSQL(sql);
	}
	
	public String toSQL(StringAppender sql) {
		WhereAppender whereAppender = new WhereAppender(new StringAppenderWrapper(sql));
		whereAppender.cat(where);
		return this.sql.toString();
	}
	
	@Override
	public PreparedSQL toPreparedSQL(ParameterBinderRegistry parameterBinderRegistry) {
		return toPreparedSQL(sql, parameterBinderRegistry);
	}
	
	public PreparedSQL toPreparedSQL(StringAppender sql, ParameterBinderRegistry parameterBinderRegistry) {
		PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(new StringAppenderWrapper(sql), parameterBinderRegistry);
		return toPreparedSQL(preparedSQLWrapper);
	}
	
	public PreparedSQL toPreparedSQL(PreparedSQLWrapper preparedSQLWrapper) {
		WhereAppender whereAppender = new WhereAppender(preparedSQLWrapper);
		whereAppender.cat(where);
		PreparedSQL result = new PreparedSQL(this.sql.toString(), preparedSQLWrapper.getParameterBinders());
		result.setValues(preparedSQLWrapper.getValues());
		return result;
	}
	
	/**
	 * The contract for printing a where clause : need to prin a String and a value.
	 * Then you can print a prepared statement or a valued statement.
	 */
	public interface SQLAppender {
		
		/**
		 * Appends a {@link String} to the underlying result. Used for keywords, column name, etc
		 * @param s a basic {@link String}
		 * @return this
		 */
		SQLAppender cat(String s, String... ss);
		
		/**
		 * Called when a value must be "printed" to the underlying result. Implementations will differs on this point depending on the target goal:
		 * values printed in the SQL statement (bad practive because of SQL injection) or prepared statement
		 * @param value the object to be added/printed to the statement
		 * @return this
		 */
		SQLAppender catValue(Object value);
		
		default SQLAppender catIf(boolean condition, String s) {
			if (condition) {
				cat(s);
			}
			return this;
		}
	}
	
	private class WhereAppender {
		
		private final SQLAppender sql;
		
		private WhereAppender(SQLAppender sql) {
			this.sql = sql;
		}
		
		public void cat(Object o) {
			if (o instanceof String) {
				sql.cat((String) o);
			} else if (o instanceof CriteriaChain) {
				cat("(");
				cat((CriteriaChain) o);
				cat(")");
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
				} else if (o instanceof String) {
					sql.cat((String) o);
				} else if (o instanceof CriteriaChain) {
					cat("(");
					cat((CriteriaChain) o);
					cat(")");
				} else if (o instanceof Column) {
					sql.cat(WhereBuilder.this.getName((Column) o));
				} else if (o instanceof Operand) {
					cat((Operand) o);
				} else {
					throw new IllegalArgumentException("Unknown criterion type " + Reflections.toString(o.getClass()));
				}
			}
		}
		
		public void cat(ColumnCriterion criterion) {
			sql.cat(WhereBuilder.this.getName(criterion.getColumn()), " ");
			Object o = criterion.getCondition();
			if (o instanceof String) {
				sql.cat((String) o);
			} else if (o instanceof Operand) {
				cat((Operand) o);
			} else {
				throw new IllegalArgumentException("Unknown criterion type " + Reflections.toString(o.getClass()));
			}
		}
		
		public void cat(LogicalOperator operator) {
			if (operator != null) {
				sql.cat(" ", getName(operator), " ");
			}
		}
		
		public void cat(Operand operand) {
			operandBuilder.cat(operand, sql);
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
