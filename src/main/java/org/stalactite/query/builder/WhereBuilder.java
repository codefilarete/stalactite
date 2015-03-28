package org.stalactite.query.builder;

import java.util.EnumMap;
import java.util.Map;

import org.stalactite.lang.StringAppender;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.query.model.AbstractCriterion;
import org.stalactite.query.model.AbstractCriterion.LogicalOperator;
import org.stalactite.query.model.ColumnCriterion;
import org.stalactite.query.model.Criteria;
import org.stalactite.query.model.RawCriterion;

/**
 * @author mary
 */
public class WhereBuilder extends AbstractDMLBuilder {

	private static final EnumMap<LogicalOperator, String> LOGICAL_OPERATOR_NAMES = new EnumMap<>(LogicalOperator.class);
	
	public static final String AND = "and";
	public static final String OR = "or";

	static {
		LOGICAL_OPERATOR_NAMES.put(LogicalOperator.And, AND);
		LOGICAL_OPERATOR_NAMES.put(LogicalOperator.Or, OR);
	}
	
	private final Criteria where;

	public WhereBuilder(Criteria where, Map<Table, String> tableAliases) {
		super(tableAliases);
		this.where = where;
	}
	
	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(200);
		cat(where, sql);
		return sql.toString();
	}
	
	protected void cat(Criteria criteria, StringAppender sql) {
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
			} else if (criterion instanceof Criteria) {
				sql.cat(" (");
				cat((Criteria) criterion, sql);
				sql.cat(")");
			}
		}
	}
	
	protected void cat(RawCriterion criterion, StringAppender sql) {
		for (Object o : criterion.getCondition()) {
			if (o instanceof Column) {
				sql.cat(getName((Column) o));
			} else if (o instanceof CharSequence) {
				sql.cat((CharSequence) o);
			} else {
				throw new IllegalArgumentException("Unknown criterion type " + criterion.getClass());
			}
		}
	}

	protected void cat(ColumnCriterion criterion, StringAppender sql) {
		sql.cat(getName(criterion.getColumn()), " ", criterion.getCondition());
	}
	
	private void cat(LogicalOperator operator, StringAppender sql) {
		if (operator != null) {
			sql.cat(" ", getName(operator), " ");
		}
	}
	
//	protected void cat(Criteria criteriaSuite, StringAppender sql) {
//		WhereBuilder whereBuilder = new WhereBuilder(criteriaSuite, tableAliases);
//		sql.cat("(", whereBuilder.toSQL(), ")");
//	}

	String getName(LogicalOperator operator) {
		return LOGICAL_OPERATOR_NAMES.get(operator);
	}
}
