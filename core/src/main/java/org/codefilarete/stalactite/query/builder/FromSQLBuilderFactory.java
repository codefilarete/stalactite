package org.codefilarete.stalactite.query.builder;

import java.util.Iterator;

import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.builder.UnionSQLBuilderFactory.UnionSQLBuilder;
import org.codefilarete.stalactite.query.model.From;
import org.codefilarete.stalactite.query.model.From.AbstractJoin;
import org.codefilarete.stalactite.query.model.From.AbstractJoin.JoinDirection;
import org.codefilarete.stalactite.query.model.From.ColumnJoin;
import org.codefilarete.stalactite.query.model.From.CrossJoin;
import org.codefilarete.stalactite.query.model.From.Join;
import org.codefilarete.stalactite.query.model.From.KeyJoin;
import org.codefilarete.stalactite.query.model.From.RawTableJoin;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Union.UnionInFrom;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.collection.PairIterator;

/**
 * Factory for {@link FromSQLBuilder}. It's overridable by giving your own implementation to
 * {@link QuerySQLBuilderFactory#QuerySQLBuilderFactory(ColumnBinderRegistry, SelectSQLBuilderFactory, FromSQLBuilderFactory, WhereSQLBuilderFactory, WhereSQLBuilderFactory)}
 * 
 * @author Guillaume Mary
 */
public class FromSQLBuilderFactory {
	
	public FromSQLBuilderFactory() {
	}
	
	public FromSQLBuilder fromBuilder(From from, DMLNameProvider dmlNameProvider, QuerySQLBuilderFactory querySQLBuilderFactory) {
		return new FromSQLBuilder(from, dmlNameProvider, querySQLBuilderFactory);
	}
	
	/**
	 * Formats {@link From} object to an SQL {@link String} through {@link #toSQL()}.
	 * Requires some {@link QuerySQLBuilderFactory} to also transform {@link Query} in case the {@link From} clause
	 * contains a sub-query or is an union.
	 *
	 * @author Guillaume Mary
	 * @see #toSQL()
	 */
	public static class FromSQLBuilder implements SQLBuilder {
		
		private final From from;
		
		private final DMLNameProvider dmlNameProvider;
		
		private final QuerySQLBuilderFactory querySQLBuilderFactory;
		
		public FromSQLBuilder(From from, DMLNameProvider dmlNameProvider, QuerySQLBuilderFactory querySQLBuilderFactory) {
			this.from = from;
			this.dmlNameProvider = dmlNameProvider;
			this.querySQLBuilderFactory = querySQLBuilderFactory;
		}
		
		@Override
		public String toSQL() {
			if (from.getRoot() == null) {
				// invalid SQL
				throw new IllegalArgumentException("Empty from");
			}
			StringAppender fromGenerator = new FromGenerator();
			fromGenerator.cat(from.getRoot());
			Iterator<Join> joinIterator = from.getJoins().iterator();
			joinIterator.forEachRemaining(fromGenerator::cat);
			return fromGenerator.toString();
		}
		
		/**
		 * A dedicated {@link StringAppender} for the From clause
		 */
		private class FromGenerator extends StringAppender {
			
			private static final String INNER_JOIN = " inner join ";
			private static final String LEFT_OUTER_JOIN = " left outer join ";
			private static final String RIGHT_OUTER_JOIN = " right outer join ";
			private static final String CROSS_JOIN = " cross join ";
			private static final String ON = " on ";
			
			public FromGenerator() {
				super(200);
			}
			
			/**
			 * Overridden to dispatch to dedicated cat methods
			 */
			@Override
			public StringAppender cat(Object o) {
				if (o instanceof String) {
					return cat((String) o);
				} else if (o instanceof Table) {
					return cat((Table) o);
				} else if (o instanceof Query) {
					return cat((Query) o);
				} else if (o instanceof UnionInFrom) {
					return cat((UnionInFrom) o);
				} else if (o instanceof CrossJoin) {
					return cat((CrossJoin) o);
				} else if (o instanceof AbstractJoin) {
					return cat((AbstractJoin) o);
				} else {
					return super.cat(o);
				}
			}
			
			/**
			 * Made to skip going through cat(Object) while calling it from internal method that passes String as argument. Small optimization, eases
			 * debug, simplifies call.
			 *
			 * @param s any String
			 * @return this
			 */
			private StringAppender cat(String s) {
				super.appender.append(s);
				return this;
			}
			
			private StringAppender cat(Table table) {
				String tableAlias = dmlNameProvider.getAlias(table);
				return cat(table.getName()).catIf(!Strings.isEmpty(tableAlias), " as " + tableAlias);
			}
			
			private StringAppender cat(Query query) {
				QuerySQLBuilder unionBuilder = querySQLBuilderFactory.queryBuilder(query);
				return cat(unionBuilder.toSQL());
			}
			
			private StringAppender cat(UnionInFrom union) {
				UnionSQLBuilder unionSqlBuilder = new UnionSQLBuilder(union.getUnion(), querySQLBuilderFactory);
				// tableAlias may be null which produces invalid SQL in a majority of cases, but not when it is the only element in the From clause ...
				return cat("(", unionSqlBuilder.toSQL(), ") as ").cat(getAliasOrDefault(union));
			}
			
			private StringAppender cat(CrossJoin join) {
				catIf(length() > 0, CROSS_JOIN).cat(join.getRightTable());
				return this;
			}
			
			private StringAppender cat(AbstractJoin join) {
				cat(join.getJoinDirection(), join.getRightTable());
				if (join instanceof RawTableJoin) {
					cat(((RawTableJoin) join).getJoinClause());
				} else if (join instanceof ColumnJoin) {
					ColumnJoin columnJoin = (ColumnJoin) join;
					CharSequence leftPrefix = getAliasOrDefault(columnJoin.getLeftColumn().getOwner());
					CharSequence rightPrefix = getAliasOrDefault(columnJoin.getRightColumn().getOwner());
					cat(leftPrefix, ".", columnJoin.getLeftColumn().getExpression(), " = ", rightPrefix, ".", columnJoin.getRightColumn().getExpression());
				} else if (join instanceof KeyJoin) {
					KeyJoin keyJoin = (KeyJoin) join;
					PairIterator<JoinLink<?, ?>, JoinLink<?, ?>> joinColumnsPairs = new PairIterator<>(keyJoin.getLeftKey().getColumns(), keyJoin.getRightKey().getColumns());
					joinColumnsPairs.forEachRemaining(duo -> {
						CharSequence leftPrefix = getAliasOrDefault(duo.getLeft().getOwner());
						CharSequence rightPrefix = getAliasOrDefault(duo.getRight().getOwner());
						cat(leftPrefix, ".", duo.getLeft().getExpression(), " = ", rightPrefix, ".", duo.getRight().getExpression(), " and ");
					});
					cutTail(" and ".length());
				} else {
					// did I miss something ?
					throw new UnsupportedOperationException("From building is not implemented for " + join.getClass().getName());
				}
				return this;
			}
			
			private String getAliasOrDefault(Fromable fromable) {
				return Strings.preventEmpty(dmlNameProvider.getAlias(fromable), fromable.getName());
			}
			
			protected void cat(JoinDirection joinDirection, Fromable table) {
				String joinType;
				switch (joinDirection) {
					case INNER_JOIN:
						joinType = INNER_JOIN;
						break;
					case LEFT_OUTER_JOIN:
						joinType = LEFT_OUTER_JOIN;
						break;
					case RIGHT_OUTER_JOIN:
						joinType = RIGHT_OUTER_JOIN;
						break;
					default:
						throw new IllegalArgumentException("Join type not implemented");
				}
				cat(joinType, table, ON);
			}
		}
	}
}