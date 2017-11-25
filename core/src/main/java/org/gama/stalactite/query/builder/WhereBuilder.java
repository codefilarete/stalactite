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
import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractCriterion;
import org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator;
import org.gama.stalactite.query.model.ColumnCriterion;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Operand;
import org.gama.stalactite.query.model.RawCriterion;
import org.gama.stalactite.query.model.operand.Between;
import org.gama.stalactite.query.model.operand.Between.Interval;
import org.gama.stalactite.query.model.operand.Equals;
import org.gama.stalactite.query.model.operand.Greater;
import org.gama.stalactite.query.model.operand.In;
import org.gama.stalactite.query.model.operand.IsNull;
import org.gama.stalactite.query.model.operand.Like;
import org.gama.stalactite.query.model.operand.Lower;

/**
 * @author Guillaume Mary
 */
public class WhereBuilder extends AbstractDMLBuilder {

	public static final String AND = "and";
	public static final String OR = "or";

	private final CriteriaChain where;

	public WhereBuilder(CriteriaChain where, Map<Table, String> tableAliases) {
		super(tableAliases);
		this.where = where;
	}
	
	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(200);
		cat(where, sql);
		return sql.toString();
	}
	
	public PreparedSQL toPreparedSQL(ParameterBinderRegistry parameterBinderRegistry) {
		StringAppender sql = new StringAppender(200);
		PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(sql, parameterBinderRegistry);
		cat(where, preparedSQLWrapper);
		PreparedSQL result = new PreparedSQL(sql.toString(), preparedSQLWrapper.parameterBinders);
		result.setValues(preparedSQLWrapper.values);
		return result;
	}
	
	protected void cat(CriteriaChain criteria, StringAppender sql) {
		StringAppenderWrapper appender = new StringAppenderWrapper(sql);
		cat(criteria, appender);
	}
	
	protected void cat(CriteriaChain criteria, WhereAppender sql) {
		boolean isNotFirst = false;
		for (Object criterion : criteria) {
			if (isNotFirst) {
				cat(((AbstractCriterion) criterion).getOperator(), sql);
			} else {
				isNotFirst = true;
			}
			if (criterion instanceof RawCriterion) {
				cat((RawCriterion) criterion, sql);
			} else if (criterion instanceof ColumnCriterion) {
				cat((ColumnCriterion) criterion, sql);
			} else if (criterion instanceof CriteriaChain) {
				sql.cat("(");
				cat((CriteriaChain) criterion, sql);
				sql.cat(")");
			}
		}
	}
	
	protected void cat(RawCriterion criterion, WhereAppender sql) {
		for (Object o : criterion.getCondition()) {
			if (o instanceof ColumnCriterion) {
				cat((ColumnCriterion) o, sql);
			} else
				if (o instanceof String) {
				sql.cat((String) o);
			} else if (o instanceof CriteriaChain) {
				sql.cat("(");
				cat((CriteriaChain) o, sql);
				sql.cat(")");
			} else {
				throw new IllegalArgumentException("Unknown criterion type " + Reflections.toString(o.getClass()));
			}
		}
	}
	
	protected void cat(ColumnCriterion criterion, WhereAppender sql) {
		sql.cat(getName(criterion.getColumn()), " ");
		Object o = criterion.getCondition();
		if (o instanceof String) {
			sql.cat((String) o);
		} else if (o instanceof Operand) {
			cat((Operand) o, sql);
		} else {
			throw new IllegalArgumentException("Unknown criterion type " + Reflections.toString(o.getClass()));
		}
	}
	
	private void cat(Operand operand, WhereAppender sql) {
		if (operand.getValue() == null) {
			catNullValue(operand, sql);
			
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
				throw new UnsupportedOperationException("Operator " + operand.getClass() + " is not implemented");
			}
		}
	}
	
	private void catNullValue(Operand operand, WhereAppender sql) {
		// "= NULL" is incorrect and will return no result (answer from Internet) and should be replaced by "is null"
		sql.cat("is").catIf(operand.isNot(), " not").cat(" null");
	}
	
	private void catIsNull(IsNull isNull, WhereAppender sql) {
		catNullValue(isNull, sql);
	}
	
	private void catLike(Like like, WhereAppender sql) {
		String value = (String) like.getValue();
		if (like.withLeadingStar()) {
			value = '%' + value;
		}
		if (like.withEndingStar()) {
			value += '%';
		}
		sql.catIf(like.isNot(), "not ").cat("like ").catValue(value);
	}
	
	private void catIn(In in, WhereAppender sql) {
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
	
	private void catBetween(Between between, WhereAppender sql) {
		Interval interval = (Interval) between.getValue();
		if (interval.getValue1() == null) {
			sql.cat(between.isNot() ? ">= " : "< ").catValue(interval.getValue2());
		} else if (interval.getValue2() == null) {
			sql.cat(between.isNot() ? "<= " : "> ").catValue(interval.getValue1());
		} else {
			sql.catIf(between.isNot(), "not ").cat("between ").catValue(interval.getValue1())
					.cat(" and ").catValue(interval.getValue2());
		}
	}
	
	private void catGreater(Greater greater, WhereAppender sql) {
		sql.cat(greater.isNot()
				? (greater.isEquals() ? "< ": "<= ")
				: (greater.isEquals() ? ">= ": "> "))
				.catValue(greater.getValue());
	}
	
	private void catLower(Lower lower, WhereAppender sql) {
		sql.cat(lower.isNot()
				? (lower.isEquals() ? "> ": ">= ")
				: (lower.isEquals() ? "<= ": "< "))
				.catValue(lower.getValue());
	}
	
	private void catEquals(Equals equals, WhereAppender sql) {
		sql.catIf(equals.isNot(), "!").cat("= ").catValue(equals.getValue());
	}
	
	private void cat(LogicalOperator operator, WhereAppender sql) {
		if (operator != null) {
			sql.cat(" ", getName(operator), " ");
		}
	}
	
	private String getName(LogicalOperator operator) {
		switch (operator) {
			case And:
				return AND;
			case Or:
				return OR;
			default:
				throw new IllegalArgumentException("Operator " + operator + " is unknown");
		}
	}
	
	/**
	 * The contract definining necessary methods to "print" a query.
	 * Implementations 
	 */
	interface WhereAppender {
		
		/**
		 * Appends a {@link String} to the underlying result. Used fo rkeywords, column name, etc
		 * @param s a basic {@link String}
		 * @return this
		 */
		WhereAppender cat(String s);
		
		/**
		 * Called when a value must be "printed" to the underlying result. Implementations will differs on this point depending on the target goal:
		 * values printed in the SQL statement (bad practive because of SQL injection) or prepared statement
		 * @param value the object to be added/printed to the statement
		 * @return this
		 */
		WhereAppender catValue(Object value);
		
		default WhereAppender cat(String s1, String s2) {
			// we skip an array creation by calling cat() multiple times
			return cat(s1).cat(s2);
		}
		
		default WhereAppender cat(String s1, String s2, String s3) {
			// we skip an array creation by calling cat() multiple times
			return cat(s1).cat(s2).cat(s3);
		}
		
		default WhereAppender cat(String... ss) {
			for (String s : ss) {
				cat(s);
			}
			return this;
		}
		
		default WhereAppender catIf(boolean condition, String s) {
			if (condition) {
				cat(s);
			}
			return this;
		}
	}
	
	/**
	 * A basic appender to a {@link StringAppender}
	 */
	private static class StringAppenderWrapper implements WhereAppender {
		
		private final StringAppender surrogate;
		
		private StringAppenderWrapper(StringAppender stringAppender) {
			surrogate = stringAppender;
		}
		
		@Override
		public WhereAppender cat(String s) {
			surrogate.cat(s);
			return this;
		}
		
		@Override
		public WhereAppender catValue(Object value) {
			if (value instanceof CharSequence) {
				surrogate.cat("'", value.toString().replace("'", "''"), "'");
			} else {
				surrogate.cat(value);
			}
			return this;
		}
	}
	
	/**
	 * An appender to a {@link PreparedSQL}
	 */
	private static class PreparedSQLWrapper implements WhereAppender {
		
		private final StringAppender surrogate;
		private final ParameterBinderRegistry parameterBinderRegistry;
		private final Map<Integer, ParameterBinder> parameterBinders;
		private final Map<Integer, Object> values;
		private final IncrementableInt paramCounter = new IncrementableInt(1);
		
		private PreparedSQLWrapper(StringAppender stringAppender, ParameterBinderRegistry parameterBinderRegistry) {
			this.surrogate = stringAppender;
			this.parameterBinderRegistry = parameterBinderRegistry;
			this.parameterBinders = new HashMap<>();
			this.values = new HashMap<>();
		}
		
		@Override
		public WhereAppender cat(String s) {
			surrogate.cat(s);
			return this;
		}
		
		/**
		 * Implemented such it adds the value as a {@link java.sql.PreparedStatement} mark (?) and keeps it for future use in the value list.
		 * @param value the object to be added/printed to the statement
		 * @return this
		 */
		@Override
		public WhereAppender catValue(Object value) {
			Class<?> binderType = value.getClass().isArray() ? value.getClass().getComponentType() : value.getClass();
			values.put(paramCounter.getValue(), value);
			parameterBinders.put(paramCounter.getValue(), parameterBinderRegistry.getBinder(binderType));
			paramCounter.increment();
			surrogate.cat("?");
			return this;
		}
	}
}
