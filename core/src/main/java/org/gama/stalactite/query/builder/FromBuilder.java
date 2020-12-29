package org.gama.stalactite.query.builder;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.ValueFactoryMap;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.From;
import org.gama.stalactite.query.model.From.AbstractJoin;
import org.gama.stalactite.query.model.From.AbstractJoin.JoinDirection;
import org.gama.stalactite.query.model.From.ColumnJoin;
import org.gama.stalactite.query.model.From.CrossJoin;
import org.gama.stalactite.query.model.From.IJoin;
import org.gama.stalactite.query.model.From.RawTableJoin;

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
		
		Iterator<IJoin> joinIterator = from.getJoins().iterator();
		if (!joinIterator.hasNext()) {
			// invalid SQL
			throw new IllegalArgumentException("Empty from");
		}
		
		ValueFactoryMap<Table, Set<String>> tablesInJoins = new ValueFactoryMap<>(
				// we use an IdentityHashMap to support presence of table clone : same name but not same instance. Needed in particular for cycling.
				new IdentityHashMap<>(),
				k -> new TreeSet<>(Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)));
		IJoin firstJoin = joinIterator.next();
		
		if (firstJoin instanceof AbstractJoin) {
			AbstractJoin join = (AbstractJoin) firstJoin;
			
			Table leftTable = firstJoin.getLeftTable();
			String tableAlias = dmlNameProvider.getAlias(leftTable);
			tablesInJoins.get(leftTable).add(tableAlias);
			
			Table rightTable = join.getRightTable();
			String rightTableAlias = dmlNameProvider.getAlias(rightTable);
			if (tablesInJoins.get(rightTable).contains(rightTableAlias)) {
				throw new UnsupportedOperationException("Join is declared on and already-added table : " + toString(rightTable, rightTableAlias));
			}
			
			tablesInJoins.get(rightTable).add(rightTableAlias);
		} else if (firstJoin instanceof CrossJoin) {
			Table tableToBeAdded = firstJoin.getLeftTable();
			String tableAlias = dmlNameProvider.getAlias(tableToBeAdded);
			tablesInJoins.get(tableToBeAdded).add(tableAlias);
		}
		sql.cat(firstJoin);
		
		joinIterator.forEachRemaining(iJoin -> {
			
			Table tableToBeAdded = null;
			if (iJoin instanceof AbstractJoin) {
				AbstractJoin join = (AbstractJoin) iJoin;
				
				Collection<Table> tablesToBeAdded = new HashSet<>();
				Table leftTable = join.getLeftTable();
				String leftTableAlias = dmlNameProvider.getAlias(leftTable);
				if (!tablesInJoins.get(leftTable).contains(leftTableAlias)) {
					tablesToBeAdded.add(leftTable);
				}
				
				Table rightTable = join.getRightTable();
				String rightTableAlias = dmlNameProvider.getAlias(rightTable);
				if (!tablesInJoins.get(rightTable).contains(rightTableAlias)) {
					tablesToBeAdded.add(rightTable);
				}
				
				if (tablesToBeAdded.isEmpty()) {
					throw new UnsupportedOperationException("Join is declared on already-added tables : "
							+ toString(leftTable, leftTableAlias) + " and " + toString(rightTable, rightTableAlias));
				} else if (tablesToBeAdded.size() == 2) {
					throw new UnsupportedOperationException("Join is declared on non-added tables : "
							+ toString(leftTable, leftTableAlias) + " and " + toString(rightTable, rightTableAlias));
				}
				
				tableToBeAdded = Iterables.first(tablesToBeAdded);
			} else if (iJoin instanceof CrossJoin) {
				Table joinTable = iJoin.getLeftTable();
				String joinTableAlias = dmlNameProvider.getAlias(joinTable);
				if (tablesInJoins.get(joinTable).contains(joinTableAlias)) {
					throw new UnsupportedOperationException("Join is declared on an already-added table : " + toString(joinTable, joinTableAlias));
				}
				tableToBeAdded = iJoin.getLeftTable();
			}
			sql.cat(iJoin);
			String tableAlias = dmlNameProvider.getAlias(tableToBeAdded);
			tablesInJoins.get(tableToBeAdded).add(tableAlias);
		});
		return sql.toString();
	}
	
	private static String toString(Table table, String tableAlias) {
		return table.getAbsoluteName() + " (alias = " + tableAlias + ")";
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
