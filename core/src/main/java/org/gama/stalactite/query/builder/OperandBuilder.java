package org.gama.stalactite.query.builder;

import java.util.HashMap;
import java.util.Map;

import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.ArrayIterator;
import org.gama.lang.collection.Iterables;
import org.gama.lang.trace.IncrementableInt;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderRegistry;
import org.gama.stalactite.query.model.Operand;
import org.gama.stalactite.query.model.operand.Between;
import org.gama.stalactite.query.model.operand.Between.Interval;
import org.gama.stalactite.query.model.operand.Equals;
import org.gama.stalactite.query.model.operand.Greater;
import org.gama.stalactite.query.model.operand.In;
import org.gama.stalactite.query.model.operand.IsNull;
import org.gama.stalactite.query.model.operand.Like;
import org.gama.stalactite.query.model.operand.Lower;

/**
 * A class made to print a {@link Operand}
 * 
 * @author Guillaume Mary
 */
public class OperandBuilder {
	
	/**
	 * Main entry point
	 */
	public void cat(Operand operand, SQLAppender sql) {
		if (operand.getValue() == null) {
			catNullValue(operand.isNot(), sql);
		} else {
			if (operand instanceof Equals) {
				catEquals((Equals) operand, sql);
			} else if (operand instanceof Lower) {
				catLower((Lower) operand, sql);
			} else if (operand instanceof Greater) {
				catGreater((Greater) operand, sql);
			} else if (operand instanceof Between) {
				catBetween((Between) operand, sql);
			} else if (operand instanceof In) {
				catIn((In) operand, sql);
			} else if (operand instanceof Like) {
				catLike((Like) operand, sql);
			} else if (operand instanceof IsNull) {
				catIsNull((IsNull) operand, sql);
			} else {
				throw new UnsupportedOperationException("Operator " + Reflections.toString(operand.getClass()) + " is not implemented");
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
	
	public void catLike(Like like, SQLAppender sql) {
		String value = (String) like.getValue();
		if (like.withLeadingStar()) {
			value = '%' + value;
		}
		if (like.withEndingStar()) {
			value += '%';
		}
		sql.catIf(like.isNot(), "not ").cat("like ").catValue(value);
	}
	
	public void catIn(In in, SQLAppender sql) {
		// we take collection into account : iterating over it to cat all values
		Object value = in.getValue();
		// we adapt the value to an Iterable, avoiding multiple cases and falling into a simple foreach loop 
		if (!(value instanceof Iterable)) {
			if (!value.getClass().isArray()) {
				value = new Object[] { value };
			}
			value = Iterables.asIterable(new ArrayIterator<>((Object[]) value));
		}
		sql.catIf(in.isNot(), "not ").cat("in (");
		// appending values (separated by a comma, boilerplate code)
		boolean isFirst = true;
		for (Object v : (Iterable) value) {
			if (!isFirst) {
				sql.cat(", ");
			} else {
				isFirst = false;
			}
			sql.catValue(v);
		}
		sql.cat(")");
	}
	
	public void catBetween(Between between, SQLAppender sql) {
		Interval interval = between.getValue();
		if (interval.getValue1() == null) {
			sql.cat(between.isNot() ? ">= " : "< ").catValue(interval.getValue2());
		} else if (interval.getValue2() == null) {
			sql.cat(between.isNot() ? "<= " : "> ").catValue(interval.getValue1());
		} else {
			sql.catIf(between.isNot(), "not ").cat("between ")
					.catValue(interval.getValue1()).cat(" and ").catValue(interval.getValue2());
		}
	}
	
	public void catGreater(Greater greater, SQLAppender sql) {
		sql.cat(greater.isNot()
				? (greater.isEquals() ? "< ": "<= ")
				: (greater.isEquals() ? ">= ": "> "))
			.catValue(greater.getValue());
	}
	
	public void catLower(Lower lower, SQLAppender sql) {
		sql.cat(lower.isNot()
				? (lower.isEquals() ? "> ": ">= ")
				: (lower.isEquals() ? "<= ": "< "))
			.catValue(lower.getValue());
	}
	
	public void catEquals(Equals equals, SQLAppender sql) {
		sql.catIf(equals.isNot(), "!").cat("= ").catValue(equals.getValue());
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
	
	/**
	 * A basic appender to a {@link StringAppender}
	 */
	public static class StringAppenderWrapper implements SQLAppender {
		
		private final StringAppender surrogate;
		
		public StringAppenderWrapper(StringAppender stringAppender) {
			surrogate = stringAppender;
		}
		
		@Override
		public StringAppenderWrapper cat(String s, String... ss) {
			surrogate.cat(s).cat(ss);
			return this;
		}
		
		@Override
		public StringAppenderWrapper catValue(Object value) {
			if (value instanceof CharSequence) {
				surrogate.cat("'", value.toString().replace("'", "''"), "'");
			} else {
				surrogate.cat(value);
			}
			return this;
		}
	}
	
	/**
	 * An appender to a {@link org.gama.sql.dml.PreparedSQL}
	 */
	public static class PreparedSQLWrapper implements SQLAppender {
		
		private final SQLAppender surrogate;
		private final ParameterBinderRegistry parameterBinderRegistry;
		private final Map<Integer, ParameterBinder> parameterBinders;
		private final Map<Integer, Object> values;
		private final IncrementableInt paramCounter = new IncrementableInt(1);
		
		public PreparedSQLWrapper(SQLAppender sqlAppender, ParameterBinderRegistry parameterBinderRegistry) {
			this.surrogate = sqlAppender;
			this.parameterBinderRegistry = parameterBinderRegistry;
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
		 * @param value the object to be added/printed to the statement
		 * @return this
		 */
		@Override
		public PreparedSQLWrapper catValue(Object value) {
			Class<?> binderType = value.getClass().isArray() ? value.getClass().getComponentType() : value.getClass();
			values.put(paramCounter.getValue(), value);
			parameterBinders.put(paramCounter.getValue(), parameterBinderRegistry.getBinder(binderType));
			paramCounter.increment();
			surrogate.cat("?");
			return this;
		}
	}
}
