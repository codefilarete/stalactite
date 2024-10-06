package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;

import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory.FunctionSQLBuilder;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.ValueWrapper;
import org.codefilarete.stalactite.query.model.ValueWrapper.SQLFunctionWrapper;
import org.codefilarete.stalactite.query.model.operator.Between;
import org.codefilarete.stalactite.query.model.operator.Between.Interval;
import org.codefilarete.stalactite.query.model.operator.Equals;
import org.codefilarete.stalactite.query.model.operator.Greater;
import org.codefilarete.stalactite.query.model.operator.In;
import org.codefilarete.stalactite.query.model.operator.IsNull;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.stalactite.query.model.operator.Lower;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Reflections;

/**
 * A class made to print a {@link ConditionalOperator}
 * 
 * @author Guillaume Mary
 */
public class OperatorSQLBuilderFactory {
	
	public OperatorSQLBuilderFactory() {
	}
	
	public OperatorSQLBuilder operatorSQLBuilder(FunctionSQLBuilder functionSQLBuilder) {
		return new OperatorSQLBuilder(functionSQLBuilder);
	}
	
	public static class OperatorSQLBuilder {
		
		private final FunctionSQLBuilder functionSQLBuilder;
		
		public OperatorSQLBuilder(FunctionSQLBuilder functionSQLBuilder) {
			this.functionSQLBuilder = functionSQLBuilder;
		}
		
		/**
		 * Main entry point
		 */
		public void cat(ConditionalOperator operator, SQLAppender sql) {
			if (operator instanceof TupleIn) {
				catTupledIn((TupleIn) operator, sql);
			} else {
				cat(null, operator, sql);
			}
		}
		
		public <V> void cat(Selectable<V> column, ConditionalOperator<?, V> operator, SQLAppender sql) {
			if (operator.isNull()) {
				catNullValue(operator.isNot(), sql);
			} else {
				// ugly way of dispatching concatenation, can't find a better way without heaving classes or struggling with single responsibility design
				if (operator instanceof Equals) {
					catEquals((Equals<V>) operator, sql, column);
				} else if (operator instanceof Lower) {
					catLower((Lower<V>) operator, sql, column);
				} else if (operator instanceof Greater) {
					catGreater((Greater<V>) operator, sql, column);
				} else if (operator instanceof Between) {
					catBetween((Between<V>) operator, sql, column);
				} else if (operator instanceof In) {
					catIn((In<V>) operator, sql, column);
				} else if (operator instanceof Like) {
					catLike((Like) operator, sql, (Selectable<CharSequence>) column);
				} else if (operator instanceof IsNull) {
					catIsNull((IsNull) operator, sql);
				} else {
					throw new UnsupportedOperationException("Operator " + Reflections.toString(operator.getClass()) + " is not implemented");
				}
			}
		}
		
		void catNullValue(boolean not, SQLAppender sql) {
			// "= NULL" is incorrect and will return no result (answer from Internet) and should be replaced by "is null"
			sql.cat("is").catIf(not, " not").cat(" null");
		}
		
		void catIsNull(IsNull isNull, SQLAppender sql) {
			catNullValue(isNull.isNot(), sql);
		}
		
		void catLike(Like<?> like, SQLAppender sql, Selectable<CharSequence> column) {
			LikePatternAppender likePatternAppender = new LikePatternAppender(like, sql);
			sql.catIf(like.isNot(), "not ").cat("like ");
			if (like.getValueWrapper() instanceof ValueWrapper.RawValueWrapper) {
				likePatternAppender.catValue(column, like.getValue());
			} else if (like.getValueWrapper() instanceof ValueWrapper.SQLFunctionWrapper) {
				functionSQLBuilder.cat(((ValueWrapper.SQLFunctionWrapper) like.getValueWrapper()).getFunction(), likePatternAppender);
			}
		}
		
		<V> void catIn(In<V> in, SQLAppender sql, Selectable<V> column) {
			// we take collection into account : iterating over it to cat all values
			Iterable<V> value = in.getValue();
			sql.catIf(in.isNot(), "not ").cat("in (");
			catInValue(value, sql, column);
			sql.cat(")");
		}
		
		void catTupledIn(TupleIn in, SQLAppender sql) {
			// we take collection into account : iterating over it to cat all values
			Column[] columns = in.getColumns();
			Iterable<Object[]> values = in.getValue();
			sql.catIf(in.isNot(), "not ");
			sql.cat("(");
			Iterator<Column> columnIterator = Arrays.stream(columns).iterator();
			while (columnIterator.hasNext()) {
				sql.catColumn(columnIterator.next()).catIf(columnIterator.hasNext(), ", ");
			}
			sql.cat(")");
			
			sql.cat(" in (");
			if (values == null) {
				for (int i = 0, columnCount = columns.length; i < columnCount; i++) {
					sql.catValue(columns[i], null).catIf(i < columnCount - 1, ", ");
				}
			} else {
				Iterator<Object[]> valuesIterator = values.iterator();
				while (valuesIterator.hasNext()) {
					Object[] vals = valuesIterator.next();
					sql.cat("(");
					for (int i = 0, columnCount = columns.length; i < columnCount; i++) {
						sql.catValue(columns[i], vals[i]).catIf(i < columnCount - 1, ", ");
					}
					sql.cat(")").catIf(valuesIterator.hasNext(), ", ");
				}
			}
			sql.cat(")");
		}
		
		/**
		 * Appends {@link Iterable} values to the given appender. Essentially done for "in" operator
		 *
		 * @param sql the appender
		 * @param column
		 * @param value the iterable to be appended, not null (but may be empty, this method doesn't care)
		 */
		private <V> void catInValue(Iterable<V> value, SQLAppender sql, Selectable<V> column) {
			// appending values (separated by a comma, boilerplate code)
			boolean isFirst = true;
			for (Object v : value) {
				if (!isFirst) {
					sql.cat(", ");
				} else {
					isFirst = false;
				}
				sql.catValue(column, (V) v);
			}
		}
		
		<V> void catBetween(Between<V> between, SQLAppender sql, Selectable<V> column) {
			Interval<V> interval = between.getValue();
			if (interval.getValue1() == null) {
				sql.cat(between.isNot() ? ">= " : "< ").catValue(column, interval.getValue2());
			} else if (interval.getValue2() == null) {
				sql.cat(between.isNot() ? "<= " : "> ").catValue(column, interval.getValue1());
			} else {
				sql.catIf(between.isNot(), "not ").cat("between ")
						.catValue(column, interval.getValue1()).cat(" and ").catValue(column, interval.getValue2());
			}
		}
		
		@SuppressWarnings("squid:S3358")	// we can afford nesting ternary operators here, not so complex to understand 
		<V> void catGreater(Greater<V> greater, SQLAppender sql, Selectable<V> column) {
			sql.cat(greater.isNot()
							? (greater.isEquals() ? "< " : "<= ")
							: (greater.isEquals() ? ">= " : "> "));
			catValue(column, greater.getValueWrapper(), sql);
		}
		
		@SuppressWarnings("squid:S3358")	// we can afford nesting ternary operators here, not so complex to understand
		<V> void catLower(Lower<V> lower, SQLAppender sql, Selectable<V> column) {
			sql.cat(lower.isNot()
							? (lower.isEquals() ? "> " : ">= ")
							: (lower.isEquals() ? "<= " : "< "));
			catValue(column, lower.getValueWrapper(), sql);
		}
		
		<V> void catEquals(Equals<V> equals, SQLAppender sql, Selectable<V> column) {
			sql.catIf(equals.isNot(), "!").cat("= ");
			catValue(column, equals.getValueWrapper(), sql);
		}
		
		<V> void catValue(@Nullable Selectable<V> column, ValueWrapper<V> value, SQLAppender sql) {
			if (value instanceof ValueWrapper.SQLFunctionWrapper) {
				functionSQLBuilder.cat(((SQLFunctionWrapper<?, ?, ?>) value).getFunction(), sql);
			} else {
				sql.catValue(column, value.getValue());
			}
		}
		
		/**
		 * Adds leading and ending "%" while appending Like values.
		 * Made for cases composed Like with function as argument (lower, upper, ...) because they are not aware of being embedded in a Like operator.
		 * 
		 * @author Guillaume Mary
		 */
		private static class LikePatternAppender implements SQLAppender {
			private final Like<?> like;
			private final SQLAppender sql;
			
			public LikePatternAppender(Like<?> like, SQLAppender sql) {
				this.like = like;
				this.sql = sql;
			}
			
			@Override
			public <V> SQLAppender catValue(@Nullable Selectable<V> column, V value) {
				return sql.catValue((Selectable<CharSequence>) column, addWildcards(like.getValue()));
			}
			
			private CharSequence addWildcards(CharSequence effectiveValue) {
				if (like.withLeadingStar()) {
					effectiveValue = "%" + effectiveValue;
				}
				if (like.withEndingStar()) {
					effectiveValue += "%";
				}
				return effectiveValue;
			}
			
			@Override
			public SQLAppender catValue(Object value) {
				if (value instanceof Selectable) {
					// unwrapping raw Selectable
					value = ((Selectable<CharSequence>) value).getExpression();
				}
				return sql.catValue(addWildcards((CharSequence) value));
			}
			
			@Override
			public SQLAppender cat(String s, String... ss) {
				return sql.cat(s, ss);
			}
			
			@Override
			public SQLAppender catColumn(Column column) {
				return sql.catColumn(column);
			}
			
			@Override
			public SQLAppender removeLastChars(int length) {
				return sql.removeLastChars(length);
			}
			
			@Override
			public String getSQL() {
				return sql.getSQL();
			}
		}
	}
}