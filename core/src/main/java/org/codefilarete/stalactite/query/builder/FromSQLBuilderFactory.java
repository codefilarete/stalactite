package org.codefilarete.stalactite.query.builder;

import java.util.Iterator;

import org.codefilarete.stalactite.query.builder.PseudoTableSQLBuilderFactory.PseudoTableSQLBuilder;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
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
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.collection.PairIterator;

/**
 * Factory for {@link FromSQLBuilder}. It's overridable by giving your own implementation to
 * {@link QuerySQLBuilderFactory#QuerySQLBuilderFactory(ColumnBinderRegistry, SelectSQLBuilderFactory,
 * FromSQLBuilderFactory, PseudoTableSQLBuilderFactory, WhereSQLBuilderFactory, WhereSQLBuilderFactory, FunctionSQLBuilderFactory)}
 * 
 * @author Guillaume Mary
 */
public class FromSQLBuilderFactory {
	
	public FromSQLBuilderFactory() {
	}
	
	public FromSQLBuilder fromBuilder(From from, DMLNameProvider dmlNameProvider, QuerySQLBuilderFactory querySQLBuilderFactory, PseudoTableSQLBuilderFactory pseudoTableSQLBuilderFactory) {
		return new FromSQLBuilder(from, dmlNameProvider, querySQLBuilderFactory, pseudoTableSQLBuilderFactory);
	}
	
	/**
	 * Formats {@link From} object to an SQL {@link String} through {@link #toSQL()}.
	 * Requires some {@link QuerySQLBuilderFactory} to also transform {@link Query} in case the {@link From} clause
	 * contains a sub-query or is an union.
	 *
	 * @author Guillaume Mary
	 * @see #toSQL()
	 */
	public static class FromSQLBuilder implements SQLBuilder, PreparableSQLBuilder {
		
		private final From from;
		
		private final DMLNameProvider dmlNameProvider;
		
		private final QuerySQLBuilderFactory querySQLBuilderFactory;
		
		private final PseudoTableSQLBuilderFactory pseudoTableSQLBuilderFactory;
		
		public FromSQLBuilder(From from, DMLNameProvider dmlNameProvider, QuerySQLBuilderFactory querySQLBuilderFactory, PseudoTableSQLBuilderFactory pseudoTableSQLBuilderFactory) {
			this.from = from;
			this.dmlNameProvider = dmlNameProvider;
			this.querySQLBuilderFactory = querySQLBuilderFactory;
			this.pseudoTableSQLBuilderFactory = pseudoTableSQLBuilderFactory;
		}
		
		@Override
		public String toSQL() {
			StringAppender result = new StringAppender();
			appendTo(new StringSQLAppender(result, dmlNameProvider));
			return result.toString();
		}
		
		@Override
		public ExpandableSQLAppender toPreparableSQL() {
			ExpandableSQLAppender preparedSQLAppender = new ExpandableSQLAppender(querySQLBuilderFactory.getParameterBinderRegistry(), dmlNameProvider);
			appendTo(preparedSQLAppender);
			return preparedSQLAppender;
		}
		
		public void appendTo(SQLAppender preparedSQLAppender) {
			if (from.getRoot() == null) {
				// invalid SQL
				throw new IllegalArgumentException("Empty from");
			}
			FromGenerator fromGenerator = new FromGenerator(preparedSQLAppender, dmlNameProvider) {
				
				/** Overridden to make it call pseudoTableBuilder.toPreparableSQL(..) instead of toSQL(..). Not a really good design */
				@Override
				void cat(Query query) {
					QuerySQLBuilder unionBuilder = querySQLBuilderFactory.queryBuilder(query);
					unionBuilder.appendTo(preparedSQLAppender);
				}
				
				/** Overridden to make it call pseudoTableBuilder.toPreparableSQL(..) instead of toSQL(..). Not a really good design */
				@Override
				void cat(PseudoTable pseudoTable) {
					PseudoTableSQLBuilder pseudoTableSqlBuilder = pseudoTableSQLBuilderFactory.pseudoTableBuilder(pseudoTable.getQueryStatement(), querySQLBuilderFactory);
					// tableAlias may be null which produces invalid SQL in a majority of cases, but not when it is the only element in the From clause ...
					sql.cat("(");
					pseudoTableSqlBuilder.appendTo(preparedSQLAppender);
					String alias = getAliasOrDefault(pseudoTable);
					sql.cat(")").catIf(alias != null, " as " + alias);
				}
			};
			fromGenerator.cat(from.getRoot());
			Iterator<Join> joinIterator = from.getJoins().iterator();
			joinIterator.forEachRemaining(fromGenerator::cat);
		}
		
		/**
		 * A dedicated {@link StringAppender} for the From clause
		 */
		public class FromGenerator {
			
			private static final String INNER_JOIN = " inner join ";
			private static final String LEFT_OUTER_JOIN = " left outer join ";
			private static final String RIGHT_OUTER_JOIN = " right outer join ";
			private static final String CROSS_JOIN = " cross join ";
			private static final String ON = " on ";
			
			final SQLAppender sql;
			private final DMLNameProvider dmlNameProvider;
			
			public FromGenerator(SQLAppender sql,
								 DMLNameProvider dmlNameProvider) {
				
				this.sql = sql;
				this.dmlNameProvider = dmlNameProvider;
			}
			
			/**
			 * Overridden to dispatch to dedicated cat methods
			 */
			public void cat(Object o) {
				if (o instanceof String) {
					sql.cat((String) o);
				} else if (o instanceof Table) {
					cat((Table) o);
				} else if (o instanceof Query) {
					cat((Query) o);
				} else if (o instanceof PseudoTable) {
					cat((PseudoTable) o);
				} else if (o instanceof CrossJoin) {
					cat((CrossJoin) o);
				} else if (o instanceof AbstractJoin) {
					cat((AbstractJoin) o);
				} else {
					throw new UnsupportedOperationException("Unknown From element " + Reflections.toString(o.getClass()));
				}
			}
			
			private void cat(Table table) {
				String tableAlias = dmlNameProvider.getAlias(table);
				cat(table.getName());
				sql.catIf(!Strings.isEmpty(tableAlias), " as " + tableAlias);
			}
			
			void cat(Query query) {
				QuerySQLBuilder unionBuilder = querySQLBuilderFactory.queryBuilder(query);
				unionBuilder.appendTo(sql);
			}
			
			void cat(PseudoTable pseudoTable) {
				PseudoTableSQLBuilder pseudoTableSqlBuilder = pseudoTableSQLBuilderFactory.pseudoTableBuilder(pseudoTable.getQueryStatement(), querySQLBuilderFactory);
				// tableAlias may be null which produces invalid SQL in a majority of cases, but not when it is the only element in the From clause ...
				sql.cat("(");
				pseudoTableSqlBuilder.appendTo(sql);
				String alias = getAliasOrDefault(pseudoTable);
				sql.cat(")").catIf(alias != null, " as " + alias);
			}
			
			private void cat(CrossJoin join) {
				sql.catIf(!sql.isEmpty(), CROSS_JOIN);
				cat(join.getRightTable());
			}
			
			private void cat(AbstractJoin join) {
				cat(join.getJoinDirection(), join.getRightTable());
				if (join instanceof RawTableJoin) {
					cat(((RawTableJoin) join).getJoinClause());
				} else if (join instanceof ColumnJoin) {
					ColumnJoin columnJoin = (ColumnJoin) join;
					CharSequence leftPrefix = getAliasOrDefault(columnJoin.getLeftColumn().getOwner());
					CharSequence rightPrefix = getAliasOrDefault(columnJoin.getRightColumn().getOwner());
					sql.cat(leftPrefix.toString(), ".", columnJoin.getLeftColumn().getExpression(), " = ", rightPrefix.toString(), ".", columnJoin.getRightColumn().getExpression());
				} else if (join instanceof KeyJoin) {
					KeyJoin keyJoin = (KeyJoin) join;
					PairIterator<JoinLink<?, ?>, JoinLink<?, ?>> joinColumnsPairs = new PairIterator<>(keyJoin.getLeftKey().getColumns(), keyJoin.getRightKey().getColumns());
					joinColumnsPairs.forEachRemaining(duo -> {
						CharSequence leftPrefix = getAliasOrDefault(duo.getLeft().getOwner());
						CharSequence rightPrefix = getAliasOrDefault(duo.getRight().getOwner());
						sql.cat(leftPrefix.toString(), ".", duo.getLeft().getExpression(), " = ", rightPrefix.toString(), ".", duo.getRight().getExpression(), " and ");
					});
					sql.removeLastChars(" and ".length());
				} else {
					// did I miss something ?
					throw new UnsupportedOperationException("From building is not implemented for " + join.getClass().getName());
				}
			}
			
			String getAliasOrDefault(Fromable fromable) {
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
				sql.cat(joinType);
				cat(table);
				sql.cat(ON);
			}
		}
	}
}