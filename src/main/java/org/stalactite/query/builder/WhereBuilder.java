package org.stalactite.query.builder;

import java.util.EnumMap;
import java.util.Map;

import org.stalactite.lang.StringAppender;
import org.stalactite.persistence.structure.Table;
import org.stalactite.query.model.ClosedCriteria;
import org.stalactite.query.model.Criteria;
import org.stalactite.query.model.Criteria.LogicalOperator;
import org.stalactite.query.model.CriteriaSuite;

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
	
	private final CriteriaSuite where;

	public WhereBuilder(CriteriaSuite where, Map<Table, String> tableAliases) {
		super(tableAliases);
		this.where = where;
	}
	
	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(200);

		for (Object o : this.where) {
			if (o instanceof String) {
				catString((String) o, sql);
			} else if (o instanceof Criteria) {
				cat((Criteria) o, sql);
			} else if (o instanceof ClosedCriteria) {
				cat((ClosedCriteria) o, sql);
			}
		}
		// il y a toujours un espace en trop en tête, on le supprime
		sql.cutHead(1);
		return sql.toString();
	}

	protected void catString(String o, StringAppender sql) {
		sql.cat(o);
	}

	protected void cat(Criteria criteria, StringAppender sql) {
		if (criteria.getOperator() != null) {
			sql.cat(" ", getName(criteria.getOperator()));
		}
		sql.cat(" ", getName(criteria.getColumn()), " ", criteria.getCondition());
	}

	protected void cat(ClosedCriteria closedCriteria, StringAppender sql) {
		WhereBuilder whereBuilder = new WhereBuilder(closedCriteria, tableAliases);
		sql.cat(" ", getName(closedCriteria.getOperator()), " (", whereBuilder.toSQL(), ")");
	}

	private String getName(LogicalOperator operator) {
		return LOGICAL_OPERATOR_NAMES.get(operator);
	}
}
