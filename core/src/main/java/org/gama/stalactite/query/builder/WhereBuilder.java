package org.gama.stalactite.query.builder;

import java.util.Map;

import org.gama.lang.StringAppender;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.AbstractCriterion;
import org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator;
import org.gama.stalactite.query.model.ColumnCriterion;
import org.gama.stalactite.query.model.Criteria;
import org.gama.stalactite.query.model.RawCriterion;

/**
 * @author mary
 */
public class WhereBuilder extends AbstractDMLBuilder {

	public static final String AND = "and";
	public static final String OR = "or";

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
				sql.cat("(");
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
				sql.cat(o);
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
	
	private String getName(LogicalOperator operator) {
		switch (operator) {
			case And:
				return AND;
			case Or:
				return OR;
			default:
				throw new IllegalArgumentException("Operator " + operator + " is unknown");
		}
	}
}
