package org.gama.stalactite.query.builder;

import org.gama.lang.StringAppender;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.GroupBy;
import org.gama.stalactite.query.model.Having;
import org.gama.stalactite.query.model.OrderBy;
import org.gama.stalactite.query.model.SelectQuery;

/**
 * @author Guillaume Mary
 */
public class SelectQueryBuilder extends AbstractDMLBuilder {
	
	private final SelectQuery selectQuery;
	private final SelectBuilder selectBuilder;
	private final FromBuilder fromBuilder;
	private final WhereBuilder whereBuilder;
	
	public SelectQueryBuilder(SelectQuery selectQuery) {
		super(selectQuery.getFromSurrogate().getTableAliases());
		this.selectQuery = selectQuery;
		this.selectBuilder = new SelectBuilder(selectQuery.getSelectSurrogate(), tableAliases);
		this.fromBuilder = new FromBuilder(selectQuery.getFromSurrogate());
		this.whereBuilder = new WhereBuilder(selectQuery.getWhere(), tableAliases);
	}
	
	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(500);
		
		sql.cat("select ", selectBuilder.toSQL());
		sql.cat(" from ", fromBuilder.toSQL());
		if (!selectQuery.getWhereSurrogate().getConditions().isEmpty()) {
			sql.cat(" where ", whereBuilder.toSQL());
		}
		
		GroupBy groupBy = selectQuery.getGroupBySurrogate();
		if (!groupBy.getGroups().isEmpty()) {
			cat(groupBy, sql.cat(" group by "));
		}
		
		Having having = selectQuery.getHavingSurrogate();
		if (!having.getConditions().isEmpty()) {
			cat(having, sql.cat(" having "));
		}
		
		OrderBy orderBy = selectQuery.getOrderBySurrogate();
		if (!orderBy.getColumns().isEmpty()) {
			cat(orderBy, sql.cat(" order by "));
		}
		
		return sql.toString();
	}
	
	private void cat(OrderBy orderBy, StringAppender sql) {
		catColumns(orderBy, sql);
	}
	
	private void cat(GroupBy groupBy, StringAppender sql) {
		catColumns(groupBy, sql);
	}
	
	private void catColumns(Iterable<Object> orderBy, StringAppender sql) {
		for (Object o : orderBy) {
			if (o instanceof String) {
				sql.cat(o);
			} else if (o instanceof Column) {
				sql.cat(getName((Column) o));
			}
			sql.cat(", ");
		}
		sql.cutTail(2);
	}
	
	private void cat(Having having, StringAppender sql) {
		whereBuilder.cat(having, sql);
	}
}
