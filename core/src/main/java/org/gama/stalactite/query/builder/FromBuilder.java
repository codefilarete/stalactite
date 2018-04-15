package org.gama.stalactite.query.builder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.From;
import org.gama.stalactite.query.model.From.AbstractJoin;
import org.gama.stalactite.query.model.From.AbstractJoin.JoinDirection;
import org.gama.stalactite.query.model.From.AliasedTable;
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
		this.dmlNameProvider = new DMLNameProvider(from.getTableAliases());
	}

	@Override
	public String toSQL() {
		StringAppender sql = new FromGenerator();
		
		if (from.getJoins().isEmpty()) {
			// invalid SQL
			throw new IllegalArgumentException("Empty from");
		} else {
			Map<Table, Set<String>> addedTables = new HashMap<>();
			for (IJoin iJoin : from) {
				if (iJoin instanceof AbstractJoin) {
					AbstractJoin join = (AbstractJoin) iJoin;
					AliasedTable leftTable = join.getLeftTable();
					if (addedTables.isEmpty()) {
						Set<String> aliases = addedTables.computeIfAbsent(leftTable.getTable(), k -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER));
						if (!Strings.isEmpty(leftTable.getAlias())) {
							aliases.add(leftTable.getAlias());
						}
					}
					
					Collection<AliasedTable> nonAddedTable = new ArrayList<>();
					AliasedTable rightTable = join.getRightTable();
					if (!addedTables.containsKey(leftTable.getTable())
							|| (!Strings.isEmpty(leftTable.getAlias()) && addedTables.get(leftTable.getTable()).contains(leftTable.getAlias()))) {
						nonAddedTable.add(rightTable);
					} else {
						if (!addedTables.containsKey(rightTable.getTable())
								|| (!Strings.isEmpty(rightTable.getAlias()) && addedTables.get(rightTable.getTable()).contains(rightTable.getAlias()))) {
							nonAddedTable.add(leftTable);
						}
					}
					if (nonAddedTable.isEmpty()) {
						throw new UnsupportedOperationException("Join is declared on non-added tables : "
								+ toString(leftTable) + " / " + toString(rightTable));
					} else if (nonAddedTable.size() == 2) {
						throw new UnsupportedOperationException("Join is declared on already-added tables : "
								+ toString(leftTable) + " / " + toString(rightTable));
					}
					
					sql.cat(join);
					AliasedTable aliasedTable = Iterables.first(nonAddedTable);
					addedTables.computeIfAbsent(aliasedTable.getTable(), k -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
							.add(Objects.preventNull(aliasedTable.getAlias()));
					
				} else if (iJoin instanceof CrossJoin) {
					sql.cat(iJoin);
					addedTables.computeIfAbsent(iJoin.getLeftTable().getTable(), k -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
							.add(Objects.preventNull(iJoin.getLeftTable().getAlias()));
				}
			}
		}
		return sql.toString();
	}
	
	private static String toString(AliasedTable table) {
		return table.getTable().getAbsoluteName() + " as " + table.getAlias();
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
			if (o instanceof AliasedTable) {
				return cat((AliasedTable) o);
			} else if (o instanceof CrossJoin) {
				return cat((CrossJoin) o);
			} else if (o instanceof AbstractJoin) {
				return cat((AbstractJoin) o);
			} else {
				return super.cat(o);
			}
		}
		
		private StringAppender cat(AliasedTable aliasedTable) {
			Table table = aliasedTable.getTable();
			String tableAlias = Objects.preventNull(aliasedTable.getAlias(), dmlNameProvider.getAlias(table));
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
				CharSequence leftPrefix = Strings.preventEmpty(columnJoin.getLeftTable().getAlias(), columnJoin.getLeftTable().getTable().getName());
				CharSequence rightPrefix = Strings.preventEmpty(columnJoin.getRightTable().getAlias(), columnJoin.getRightTable().getTable().getName());
				cat(leftPrefix, ".", columnJoin.getLeftColumn().getName(), " = ", rightPrefix, ".", columnJoin.getRightColumn().getName());
			} else {
				// did I miss something ?
				throw new UnsupportedOperationException("From building is not implemented for " + join.getClass().getName());
			}
			return this;
		}
		
		protected void cat(JoinDirection joinDirection, AliasedTable joinTable) {
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
			cat(joinType, joinTable, ON);
		}
	}
}
