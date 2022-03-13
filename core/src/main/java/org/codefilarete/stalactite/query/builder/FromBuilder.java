package org.codefilarete.stalactite.query.builder;

import java.util.Iterator;

import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.model.From;
import org.codefilarete.stalactite.query.model.From.AbstractJoin;
import org.codefilarete.stalactite.query.model.From.AbstractJoin.JoinDirection;
import org.codefilarete.stalactite.query.model.From.ColumnJoin;
import org.codefilarete.stalactite.query.model.From.CrossJoin;
import org.codefilarete.stalactite.query.model.From.Join;
import org.codefilarete.stalactite.query.model.From.RawTableJoin;

/**
 * @author Guillaume Mary
 */
public class FromBuilder implements SQLBuilder {
	
	private final From from;
	
	private final DMLNameProvider dmlNameProvider;

	public FromBuilder(From from) {
		this.from = from;
		this.dmlNameProvider = new DMLNameProvider(from.getTableAliases()::get);
	}

	@Override
	public String toSQL() {
		StringAppender sql = new FromGenerator();
		
		Iterator<Join> joinIterator = from.getJoins().iterator();
		if (!joinIterator.hasNext()) {
			// invalid SQL
			throw new IllegalArgumentException("Empty from");
		}
		joinIterator.forEachRemaining(sql::cat);
		return sql.toString();
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
		
		/** Overriden to dispatch to dedicated cat methods */
		@Override
		public StringAppender cat(Object o) {
			if (o instanceof Table) {
				return cat((Table) o);
			} else if (o instanceof CrossJoin) {
				return cat((CrossJoin) o);
			} else if (o instanceof AbstractJoin) {
				return cat((AbstractJoin) o);
			} else {
				return super.cat(o);
			}
		}
		
		private StringAppender cat(Table table) {
			String tableAlias = dmlNameProvider.getAlias(table);
			return cat(table.getName()).catIf(!Strings.isEmpty(tableAlias), " as " + tableAlias);
		}
		
		private StringAppender cat(CrossJoin join) {
			catIf(length() > 0, CROSS_JOIN).cat(join.getLeftTable());
			return this;
		}
		
		private StringAppender cat(AbstractJoin join) {
			catIf(length() == 0, join.getLeftTable());
			cat(join.getJoinDirection(), join.getRightTable());
			if (join instanceof RawTableJoin) {
				cat(((RawTableJoin) join).getJoinClause());
			} else if (join instanceof ColumnJoin) {
				ColumnJoin columnJoin = (ColumnJoin) join;
				String leftTableAlias = dmlNameProvider.getAlias(columnJoin.getLeftTable());
				String rightTableAlias = dmlNameProvider.getAlias(columnJoin.getRightTable());
				CharSequence leftPrefix = Strings.preventEmpty(leftTableAlias, columnJoin.getLeftTable().getName());
				CharSequence rightPrefix = Strings.preventEmpty(rightTableAlias, columnJoin.getRightTable().getName());
				cat(leftPrefix, ".", columnJoin.getLeftColumn().getName(), " = ", rightPrefix, ".", columnJoin.getRightColumn().getName());
			} else {
				// did I miss something ?
				throw new UnsupportedOperationException("From building is not implemented for " + join.getClass().getName());
			}
			return this;
		}
		
		protected void cat(JoinDirection joinDirection, Table table) {
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
