package org.codefilarete.stalactite.query.builder;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory.FunctionSQLBuilder;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.model.Variable;
import org.codefilarete.stalactite.query.model.operator.Between;
import org.codefilarete.stalactite.query.model.operator.Between.Interval;
import org.codefilarete.stalactite.query.model.operator.Equals;
import org.codefilarete.stalactite.query.model.operator.Greater;
import org.codefilarete.stalactite.query.model.operator.In;
import org.codefilarete.stalactite.query.model.operator.IsNull;
import org.codefilarete.stalactite.query.model.operator.Lesser;
import org.codefilarete.stalactite.query.model.operator.Like;
import org.codefilarete.stalactite.query.model.operator.SQLFunction;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;

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
				} else if (operator instanceof Lesser) {
					catLower((Lesser<V>) operator, sql, column);
				} else if (operator instanceof Greater) {
					catGreater((Greater<V>) operator, sql, column);
				} else if (operator instanceof Between) {
					catBetween((Between<V>) operator, sql, column);
				} else if (operator instanceof In) {
					catIn((In<V>) operator, sql, column);
				} else if (operator instanceof Like) {
					catLike((Like) operator, sql, column);
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
		
		void catLike(Like like, SQLAppender sql, Selectable<?> column) {
			sql.catIf(like.isNot(), "not ").cat("like ");
			LikePatternAppender likePatternAppender = new LikePatternAppender(like, sql);
			if (like.getValue() instanceof ValuedVariable) {
				Object value = ((ValuedVariable<?>) like.getValue()).getValue();
				if (value instanceof SQLFunction) {
					functionSQLBuilder.cat((SQLFunction) value, likePatternAppender);
				} else {
					likePatternAppender.catValue(column, like.getValue());
				}
			} else {
				likePatternAppender.catValue(column, like.getValue());
			}
		}
		
		<V> void catIn(In<V> in, SQLAppender sql, Selectable<V> column) {
			// we take collection into account : iterating over it to cat all values
			Variable<Iterable<V>> value = in.getValue();
			sql.catIf(in.isNot(), "not ").cat("in (");
			catInValue(value, sql, column);
			sql.cat(")");
		}
		
		void catTupledIn(TupleIn in, SQLAppender sql) {
			// we take collection into account : iterating over it to cat all values
			Column[] columns = in.getColumns();
			sql.catIf(in.isNot(), "not ");
			sql.cat("(");
			Iterator<Column> columnIterator = Arrays.stream(columns).iterator();
			while (columnIterator.hasNext()) {
				sql.catColumn(columnIterator.next()).catIf(columnIterator.hasNext(), ", ");
			}
			sql.cat(")");
			
			sql.cat(" in (");
			Variable<List<Object[]>> value = in.getValue();
			if (value instanceof ValuedVariable<?>) {
				List<Object[]> values = ((ValuedVariable<List<Object[]>>) value).getValue();
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
		private <V> void catInValue(Variable<Iterable<V>> value, SQLAppender sql, Selectable<V> column) {
			// appending values (separated by a comma, boilerplate code)
			boolean isFirst = true;
			if (value instanceof ValuedVariable) {
				for (Object v : ((ValuedVariable<Iterable<V>>) value).getValue()) {
					if (!isFirst) {
						sql.cat(", ");
					} else {
						isFirst = false;
					}
					if (v instanceof SQLFunction) {
						functionSQLBuilder.cat((SQLFunction) v, sql);
					} else {
						sql.catValue(column, v);
					}
				}
			} else {
				// we know this cast is wrong : value is Variable<Iterable<V>>. But adding a signature for it is too much work for few usage
				sql.catValue(column, value);
			}
		}
		
		<V> void catBetween(Between<V> between, SQLAppender sql, Selectable<V> column) {
			Variable<Interval<V>> value = between.getValue();
			if (value instanceof ValuedVariable) {
				Interval<V> interval = ((ValuedVariable<Interval<V>>) value).getValue();
				if (interval.getValue1() == null) {
					sql.cat(between.isNot() ? ">= " : "< ").catValue(column, interval.getValue2());
				} else if (interval.getValue2() == null) {
					sql.cat(between.isNot() ? "<= " : "> ").catValue(column, interval.getValue1());
				} else {
					sql.catIf(between.isNot(), "not ").cat("between ")
							.catValue(column, interval.getValue1())
							.cat(" and ")
							.catValue(column, interval.getValue2());
				}
			}
		}
		
		@SuppressWarnings("squid:S3358")	// we can afford nesting ternary operators here, not so complex to understand 
		<V> void catGreater(Greater<V> greater, SQLAppender sql, Selectable<V> column) {
			sql.cat(greater.isNot()
							? (greater.isEquals() ? "< " : "<= ")
							: (greater.isEquals() ? ">= " : "> "));
			catValue(column, greater.getValue(), sql);
		}
		
		@SuppressWarnings("squid:S3358")	// we can afford nesting ternary operators here, not so complex to understand
		<V> void catLower(Lesser<V> lesser, SQLAppender sql, Selectable<V> column) {
			sql.cat(lesser.isNot()
							? (lesser.isEquals() ? "> " : ">= ")
							: (lesser.isEquals() ? "<= " : "< "));
			catValue(column, lesser.getValue(), sql);
		}
		
		<V> void catEquals(Equals<V> equals, SQLAppender sql, Selectable<V> column) {
			sql.catIf(equals.isNot(), "!").cat("= ");
			catValue(column, equals.getValue(), sql);
		}
		
		<V> void catValue(@Nullable Selectable<V> column, Variable<V> variable, SQLAppender sql) {
			if (variable instanceof ValuedVariable) {
				V value = ((ValuedVariable<V>) variable).getValue();
				if (value instanceof SQLFunction) {
					functionSQLBuilder.cat((SQLFunction<?, ?>) value, sql);
				} else {
					sql.catValue(column, variable);
				}
			} else {
				sql.catValue(column, variable);
			}
		}
		
		/**
		 * Adds leading and ending "%" while appending Like values.
		 * Made for cases composed Like with function as argument (lower, upper, ...) because they are not aware of being embedded in a Like operator.
		 * 
		 * @author Guillaume Mary
		 */
		@VisibleForTesting
		static class LikePatternAppender implements SQLAppender {
			private final Like<?> like;
			private final SQLAppender sql;
			
			@VisibleForTesting
			LikePatternAppender(Like<?> like, SQLAppender sql) {
				this.like = like;
				this.sql = sql;
			}
			
			@Override
			public <V> SQLAppender catValue(@Nullable Selectable<V> column, Object variable) {
				if (variable instanceof ValuedVariable) {
					V value = ((ValuedVariable<V>) variable).getValue();
					if (value instanceof CharSequence) {
						sql.catValue((Selectable<CharSequence>) column, addWildcards((CharSequence) value));
					} else {
						sql.catValue(column, variable);
					}
				} else {
					sql.catValue(column, variable);
				}
				return this;
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
				if (value instanceof ValuedVariable) {
					value = ((ValuedVariable<?>) value).getValue();
				}
				if (value instanceof Selectable) {
					// unwrapping raw Selectable
					sql.catValue(addWildcards(((Selectable<CharSequence>) value).getExpression()));
				} else if (value instanceof CharSequence) {
					sql.catValue(addWildcards((CharSequence) value));
				} else {
					throw new UnsupportedOperationException("Appending '" + value + "'"
							+ (value == null ? "" : " (type " + Reflections.toString(value.getClass()) + ")") + " is not supported");
				}
				return this;
			}
			
			@Override
			public SQLAppender cat(String s, String... ss) {
				sql.cat(s, ss);
				return this;
			}
			
			@Override
			public SQLAppender catColumn(Selectable<?> column) {
				sql.catColumn(column);
				return this;
			}
			
			@Override
			public SQLAppender catTable(Fromable table) {
				sql.catTable(table);
				return this;
			}
			
			@Override
			public SQLAppender removeLastChars(int length) {
				sql.removeLastChars(length);
				return this;
			}
			
			@Override
			public String getSQL() {
				return sql.getSQL();
			}
			
			@Override
			public SubSQLAppender newSubPart(DMLNameProvider dmlNameProvider) {
				SQLAppender self = this;
				return new DefaultSubSQLAppender(new LikePatternAppender(this.like, this.sql.newSubPart(dmlNameProvider))) {
					@Override
					public SQLAppender close() {
						// nothing special;
						return self;
					}
				};
			}
		}
	}
}