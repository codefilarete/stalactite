package org.codefilarete.stalactite.query.builder;

import java.util.Arrays;
import java.util.Iterator;

import org.codefilarete.stalactite.query.model.ConditionalOperator;
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
	
	public OperatorSQLBuilder operatorSQLBuilder() {
		return new OperatorSQLBuilder();
	}
	
	public static class OperatorSQLBuilder {
		
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
		
		public void cat(Column column, ConditionalOperator operator, SQLAppender sql) {
			if (operator.isNull()) {
				catNullValue(operator.isNot(), sql);
			} else {
				// ugly way of dispatching concatenation, can't find a better way without heaving classes or struggling with single responsibility design
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
		
		void catLike(Like like, SQLAppender sql, Column column) {
			String value = (String) like.getValue();
			if (like.withLeadingStar()) {
				value = '%' + value;
			}
			if (like.withEndingStar()) {
				value += '%';
			}
			sql.catIf(like.isNot(), "not ").cat("like ").catValue(column, value);
		}
		
		void catIn(In in, SQLAppender sql, Column column) {
			// we take collection into account : iterating over it to cat all values
			Iterable value = in.getValue();
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
		
		void catBetween(Between between, SQLAppender sql, Column column) {
			Interval interval = between.getValue();
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
		void catGreater(Greater greater, SQLAppender sql, Column column) {
			sql.cat(greater.isNot()
							? (greater.isEquals() ? "< " : "<= ")
							: (greater.isEquals() ? ">= " : "> "))
					.catValue(column, greater.getValue());
		}
		
		@SuppressWarnings("squid:S3358")	// we can afford nesting ternary operators here, not so complex to understand
		void catLower(Lower lower, SQLAppender sql, Column column) {
			sql.cat(lower.isNot()
							? (lower.isEquals() ? "> " : ">= ")
							: (lower.isEquals() ? "<= " : "< "))
					.catValue(column, lower.getValue());
		}
		
		void catEquals(Equals equals, SQLAppender sql, Column column) {
			sql.catIf(equals.isNot(), "!").cat("= ").catValue(column, equals.getValue());
		}
	}
}