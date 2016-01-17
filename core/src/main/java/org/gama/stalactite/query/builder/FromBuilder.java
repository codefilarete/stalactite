package org.gama.stalactite.query.builder;

import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.lang.bean.Objects;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.From;
import org.gama.stalactite.query.model.From.*;

/**
 * @author Guillaume Mary
 */
public class FromBuilder extends AbstractDMLBuilder implements SQLBuilder {
	
	private static final String INNER_JOIN = " inner join ";
	private static final String LEFT_OUTER_JOIN = " left outer join ";
	private static final String RIGHT_OUTER_JOIN = " right outer join ";
	
	private static final Objects.BiPredicate<String, String> EQUALS_IGNORE_CASE_PREDICATE = new Objects.BiPredicate<String, String>() {
		@Override
		public boolean test(String s, String s2) {
			return s.compareToIgnoreCase(s2) == 0;
		}
	};
	
	
	private final From from;

	public FromBuilder(From from) {
		super(from.getTableAliases());
		this.from = from;
	}

	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(200);
		
		if (from.getJoins().isEmpty()) {
			// invalid SQL
			throw new IllegalArgumentException("Empty from");
		} else {
			// necessary for join table calculation
			AbstractJoin previousJoin = null;
			boolean isFirst = true;
			for (AbstractJoin abstractJoin : from) {
				if (isFirst) {
					cat(abstractJoin.getLeftTable(), sql);
				}
				if (abstractJoin instanceof Join) {
					Join localPreviousJoin = null;
					if (previousJoin instanceof CrossJoin) {
						sql.cat(" cross join ");
						cat(abstractJoin.getLeftTable(), sql);
					} else {
						localPreviousJoin = (Join) previousJoin;
					}
					Join join = (Join) abstractJoin;
					Table joinTable = localPreviousJoin == null ? join.getRightTable().getTable() : diff(localPreviousJoin, join);
					String joinClause = null;
					if (join instanceof RawTableJoin) {
						joinClause = ((RawTableJoin) join).getJoinClause();
					} else if (join instanceof ColumnJoin) {
						joinClause = getName(((ColumnJoin) join).getLeftColumn()) + " = " + getName(((ColumnJoin) join).getRightColumn());
					} else {
						// did I miss something ?
						throw new UnsupportedOperationException("From building is not implemented for " + join.getClass().getName());
					}
					cat(joinTable, join.getJoinDirection(), joinClause, sql);
				} else if (abstractJoin instanceof CrossJoin) {
					if (!isFirst) {
						sql.cat(" cross join ");
						cat(abstractJoin.getLeftTable(), sql);
					}
				}
				previousJoin = abstractJoin;
				isFirst = false;
			}
			
		}
		
		return sql.toString();
	}

	/**
	 * @return the table in join2 that is missing in join1, throw an exception if all table of join 2 are in join1
	 */
	private Table diff(Join join1, Join join2) {
		AliasedTable commonTable = null;
		if (equals(join2.getRightTable(), join1.getRightTable()) || equals(join2.getRightTable(), join1.getLeftTable())) {
			commonTable = join2.getLeftTable();
		} else if (equals(join2.getLeftTable(), join1.getRightTable()) || equals(join2.getLeftTable(), join1.getLeftTable())) {
			commonTable = join2.getRightTable();
		} else {
			throw new IllegalArgumentException("Second join doesn't join on new table");
		}
		return commonTable.getTable();
	}
	
	private boolean equals(AliasedTable t1, AliasedTable t2) {
		// NB: il faut comparer les tables avec leur alias, mais pas celui de AliasedTable car ici il peut ne pas
		// avoir d'alias (selon la méthode de joiture appelée), c'est celui du From qui fait foi
		return t1.getTable().getName().compareToIgnoreCase(t2.getTable().getName()) == 0
				&& Objects.equalsWithNull(getAlias(t1.getTable()), getAlias(t2.getTable()), EQUALS_IGNORE_CASE_PREDICATE);
	}

	protected void cat(Table rightTable, Boolean joinDirection, String joinClause, StringAppender sql) {
		String joinType;
		if (joinDirection == null) {
			joinType = INNER_JOIN;
		} else if (!joinDirection) {
			joinType = LEFT_OUTER_JOIN;
		} else {
			joinType = RIGHT_OUTER_JOIN;
		}
		sql.cat(joinType);
		cat(rightTable, sql);
		sql.cat(" on ", joinClause);
	}

	private void cat(AliasedTable leftTable, StringAppender sql) {
		cat(leftTable.getTable(), Objects.preventNull(leftTable.getAlias(), getAlias(leftTable.getTable())), sql);
	}

	private void cat(Table leftTable, StringAppender sql) {
		cat(leftTable, getAlias(leftTable), sql);
	}

	private void cat(Table leftTable, String tableAlias, StringAppender sql) {
		sql.cat(leftTable.getName()).catIf(!Strings.isEmpty(tableAlias), " as ", tableAlias);
	}

}
