package org.codefilarete.stalactite.query.builder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.trace.ModifiableInt;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.persistence.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;
import org.codefilarete.stalactite.query.model.operator.Between;
import org.codefilarete.stalactite.query.model.operator.Between.Interval;
import org.codefilarete.stalactite.query.model.operator.Count;
import org.codefilarete.stalactite.query.model.operator.Equals;
import org.codefilarete.stalactite.query.model.operator.Greater;
import org.codefilarete.stalactite.query.model.operator.In;
import org.codefilarete.stalactite.query.model.operator.IsNull;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.stalactite.query.model.operator.Lower;
import org.codefilarete.stalactite.query.model.operator.Max;
import org.codefilarete.stalactite.query.model.operator.Min;
import org.codefilarete.stalactite.query.model.operator.Sum;

/**
 * A class made to print a {@link AbstractRelationalOperator}
 * 
 * @author Guillaume Mary
 */
public class OperatorBuilder {
	
	private final DMLNameProvider dmlNameProvider;
	
	public OperatorBuilder() {
		this(Collections.emptyMap());
	}
	
	public OperatorBuilder(Map<? extends Table, String> tableAliases) {
		this(new DMLNameProvider(tableAliases));
	}
	
	public OperatorBuilder(DMLNameProvider dmlNameProvider) {
		this.dmlNameProvider = dmlNameProvider;
	}
	
	public void cat(AbstractRelationalOperator operator, SQLAppender sql) {
		cat(null, operator, sql);
	}
	
	/**
	 * Main entry point
	 */
	public void cat(Column column, AbstractRelationalOperator operator, SQLAppender sql) {
		if (operator.isNull()) {
			catNullValue(operator.isNot(), sql);
		} else {
			// uggly way of dispatching concatenation, can't find a better way without heavying classes or struggling with single responsability design
			if (operator instanceof Equals) {
				catEquals((Equals) operator, sql, column);
			} else if (operator instanceof Lower) {
				catLower((Lower) operator, sql, column);
			} else if (operator instanceof Greater) {
				catGreater((Greater) operator, sql, column);
			} else if (operator instanceof Between) {
				catBetween((Between) operator, sql, column);
			} else if (operator instanceof In) {
				catIn((In) operator, sql, column);
			} else if (operator instanceof Like) {
				catLike((Like) operator, sql, column);
			} else if (operator instanceof IsNull) {
				catIsNull((IsNull) operator, sql);
			} else if (operator instanceof Sum) {
				catSum((Sum) operator, sql);
			} else if (operator instanceof Count) {
				catCount((Count) operator, sql);
			} else if (operator instanceof Min) {
				catMin((Min) operator, sql);
			} else if (operator instanceof Max) {
				catMax((Max) operator, sql);
			} else {
				throw new UnsupportedOperationException("Operator " + Reflections.toString(operator.getClass()) + " is not implemented");
			}
		}
	}
	
	public void catNullValue(boolean not, SQLAppender sql) {
		// "= NULL" is incorrect and will return no result (answer from Internet) and should be replaced by "is null"
		sql.cat("is").catIf(not, " not").cat(" null");
	}
	
	public void catIsNull(IsNull isNull, SQLAppender sql) {
		catNullValue(isNull.isNot(), sql);
	}
	
	public void catLike(Like like, SQLAppender sql, Column column) {
		String value = (String) like.getValue();
		if (like.withLeadingStar()) {
			value = '%' + value;
		}
		if (like.withEndingStar()) {
			value += '%';
		}
		sql.catIf(like.isNot(), "not ").cat("like ").catValue(column, value);
	}
	
	public void catIn(In in, SQLAppender sql, Column column) {
		// we take collection into account : iterating over it to cat all values
		Iterable value = in.getValue();
		sql.catIf(in.isNot(), "not ").cat("in (");
		catInValue(value, sql, column);
		sql.cat(")");
	}
	
	/**
	 * Appends {@link Iterable} values to the given appender. Essentially done for "in" operator
	 * @param sql the appender
	 * @param column
	 * @param value the iterable to be appended, not null (but may be empty, this method doesn't care)
	 */
	private void catInValue(Iterable value, SQLAppender sql, Column column) {
		// appending values (separated by a comma, boilerplate code)
		boolean isFirst = true;
		for (Object v : value) {
			if (!isFirst) {
				sql.cat(", ");
			} else {
				isFirst = false;
			}
			sql.catValue(column, v);
		}
	}
	
	public void catBetween(Between between, SQLAppender sql, Column column) {
		Interval interval = between.getValue();
		if (interval.getValue1() == null) {
			sql.cat(between.isNot() ? ">= " : "< ").catValue(null, interval.getValue2());
		} else if (interval.getValue2() == null) {
			sql.cat(between.isNot() ? "<= " : "> ").catValue(null, interval.getValue1());
		} else {
			sql.catIf(between.isNot(), "not ").cat("between ")
					.catValue(column, interval.getValue1()).cat(" and ").catValue(column, interval.getValue2());
		}
	}
	
	@SuppressWarnings("squid:S3358")	// we can afford nesting ternary operators here, not so complex to understand 
	public void catGreater(Greater greater, SQLAppender sql, Column column) {
		sql.cat(greater.isNot()
				? (greater.isEquals() ? "< ": "<= ")
				: (greater.isEquals() ? ">= ": "> "))
			.catValue(column, greater.getValue());
	}
	
	@SuppressWarnings("squid:S3358")	// we can afford nesting ternary operators here, not so complex to understand
	public void catLower(Lower lower, SQLAppender sql, Column column) {
		sql.cat(lower.isNot()
				? (lower.isEquals() ? "> ": ">= ")
				: (lower.isEquals() ? "<= ": "< "))
			.catValue(column, lower.getValue());
	}
	
	public void catEquals(Equals equals, SQLAppender sql, Column column) {
		sql.catIf(equals.isNot(), "!").cat("= ").catValue(column, equals.getValue());
	}
	
	public void catSum(Sum sum, SQLAppender sqlAppender) {
		sqlAppender.cat("sum(", dmlNameProvider.getName(((Column) sum.getValue())), ")");
	}
	
	public void catCount(Count count, SQLAppender sqlAppender) {
		sqlAppender.cat("count(", dmlNameProvider.getName(((Column) count.getValue())), ")");
	}
	
	public void catMin(Min min, SQLAppender sqlAppender) {
		sqlAppender.cat("min(", dmlNameProvider.getName(((Column) min.getValue())), ")");
	}
	
	public void catMax(Max max, SQLAppender sqlAppender) {
		sqlAppender.cat("max(", dmlNameProvider.getName(((Column) max.getValue())), ")");
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
		 * @param column
		 * @param value the object to be added/printed to the statement
		 * @return this
		 */
		SQLAppender catValue(Column column, Object value);
		
		default SQLAppender catIf(boolean condition, String s) {
			if (condition) {
				cat(s);
			}
			return this;
		}
		
		String getSQL();
	}
	
	/**
	 * A basic appender to a {@link StringAppender}
	 */
	public static class StringAppenderWrapper implements SQLAppender {
		
		private final StringAppender surrogate;
		private final DMLNameProvider dmlNameProvider;
		
		public StringAppenderWrapper(StringAppender stringAppender, DMLNameProvider dmlNameProvider) {
			surrogate = stringAppender;
			this.dmlNameProvider = dmlNameProvider;
		}
		
		@Override
		public StringAppenderWrapper cat(String s, String... ss) {
			surrogate.cat(s).cat(ss);
			return this;
		}
		
		@Override
		public StringAppenderWrapper catValue(Column column, Object value) {
			if (value instanceof CharSequence) {
				// specialized case to espace single quotes
				surrogate.cat("'", value.toString().replace("'", "''"), "'");
			} else if (value instanceof Column) {
				// Columns are simply appended (no binder needed nor index increment)
				surrogate.cat(dmlNameProvider.getName((Column) value));
			} else {
				surrogate.cat(value);
			}
			return this;
		}
		
		@Override
		public String getSQL() {
			return surrogate.toString();
		}
	}
	
	/**
	 * An appender to a {@link org.codefilarete.stalactite.sql.statement.PreparedSQL}
	 */
	public static class PreparedSQLWrapper implements SQLAppender {
		
		private final SQLAppender surrogate;
		private final ColumnBinderRegistry parameterBinderRegistry;
		private final Map<Integer, ParameterBinder> parameterBinders;
		private final Map<Integer, Object> values;
		private final ModifiableInt paramCounter = new ModifiableInt(1);
		private final DMLNameProvider dmlNameProvider;
		
		public PreparedSQLWrapper(SQLAppender sqlAppender, ColumnBinderRegistry parameterBinderRegistry, DMLNameProvider dmlNameProvider) {
			this.surrogate = sqlAppender;
			this.parameterBinderRegistry = parameterBinderRegistry;
			this.dmlNameProvider = dmlNameProvider;
			this.parameterBinders = new HashMap<>();
			this.values = new HashMap<>();
		}
		
		public Map<Integer, Object> getValues() {
			return values;
		}
		
		public Map<Integer, ParameterBinder> getParameterBinders() {
			return parameterBinders;
		}
		
		@Override
		public PreparedSQLWrapper cat(String s, String... ss) {
			surrogate.cat(s, ss);
			return this;
		}
		
		/**
		 * Implemented such it adds the value as a {@link java.sql.PreparedStatement} mark (?) and keeps it for future use in the value list.
		 * @param column
		 * @param value the object to be added/printed to the statement
		 * @return this
		 */
		@Override
		public PreparedSQLWrapper catValue(Column column, Object value) {
			ParameterBinder<?> binder;
			if (value instanceof Column) {
				// Columns are simply appended (no binder needed nor index increment)
				surrogate.cat(dmlNameProvider.getName((Column) value));
			} else {
				if (column != null) {
					binder = parameterBinderRegistry.getBinder(column);
				} else {
					Class<?> binderType = value.getClass().isArray() ? value.getClass().getComponentType() : value.getClass();
					binder = parameterBinderRegistry.getBinder(binderType);
				}
				surrogate.cat("?");
				values.put(paramCounter.getValue(), value);
				parameterBinders.put(paramCounter.getValue(), binder);
				paramCounter.increment();
			}
			return this;
		}
		
		@Override
		public String getSQL() {
			return surrogate.getSQL();
		}
	}
}
