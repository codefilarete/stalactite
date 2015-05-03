package org.stalactite.query.builder;

import org.gama.lang.StringAppender;
import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.query.model.SelectQuery;
import org.stalactite.query.model.SelectQuery.GroupBy;
import org.stalactite.query.model.SelectQuery.Having;

/**
 * @author Guillaume Mary
 */
public class SelectQueryBuilder extends AbstractDMLBuilder {
	
	private final SelectQuery selectQuery;
	private SelectBuilder selectBuilder;
	private FromBuilder fromBuilder;
	private WhereBuilder whereBuilder;
	
	public SelectQueryBuilder(SelectQuery selectQuery) {
		super(selectQuery.getFrom().getTableAliases());
		this.selectQuery = selectQuery;
		this.selectBuilder = new SelectBuilder(selectQuery.getSelect(), tableAliases);
		this.fromBuilder = new FromBuilder(selectQuery.getFrom());
		this.whereBuilder = new WhereBuilder(selectQuery.getWhere(), tableAliases);
	}
	
	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(500);
		
		sql.cat("select ", selectBuilder.toSQL());
		sql.cat(" from ", fromBuilder.toSQL());
		if (!selectQuery.getWhere().getConditions().isEmpty()) {
			sql.cat(" where ", whereBuilder.toSQL());
		}
		
		GroupBy groupBy = selectQuery.getGroupBy();
		if (!groupBy.getGroups().isEmpty()) {
			cat(groupBy, sql.cat(" group by "));
		}
		
		Having having = groupBy.getHaving();
		if (!having.getConditions().isEmpty()) {
			cat(having, sql.cat(" having "));
		}
		
		return sql.toString();
	}
	
	private void cat(GroupBy groupBy, StringAppender sql) {
		for (Object o : groupBy.getGroups()) {
			if (o instanceof String) {
				sql.cat((String) o);
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
