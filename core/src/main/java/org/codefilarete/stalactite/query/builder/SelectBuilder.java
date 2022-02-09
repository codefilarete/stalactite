package org.codefilarete.stalactite.query.builder;

import java.util.Map;

import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.query.builder.OperatorBuilder.SQLAppender;
import org.codefilarete.stalactite.query.builder.OperatorBuilder.StringAppenderWrapper;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Select.AliasedColumn;

/**
 * @author Guillaume Mary
 */
public class SelectBuilder implements SQLBuilder {
	
	private final Select select;
	private final DMLNameProvider dmlNameProvider;
	private final OperatorBuilder operatorBuilder;
	
	public SelectBuilder(Select select, Map<Table, String> tableAliases) {
		this(select, new DMLNameProvider(tableAliases));
	}
	
	public SelectBuilder(Select select, DMLNameProvider dmlNameProvider) {
		this.select = select;
		this.dmlNameProvider = dmlNameProvider;
		this.operatorBuilder = new OperatorBuilder(this.dmlNameProvider);
	}
	
	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(200);
		sql.catIf(this.select.isDistinct(), "distinct ");
		cat(this.select, sql);
		return sql.toString();
	}
	
	private void cat(Iterable<Object /* String, Column or AliasedColumn */> select, StringAppender sql) {
		StringAppenderWrapper appenderWrapper = new StringAppenderWrapper(sql, dmlNameProvider);
		for (Object o : select) {
			if (o instanceof String) {
				cat((String) o, sql);
			} else if (o instanceof Column) {
				cat((Column) o, sql);
			} else if (o instanceof AliasedColumn) {
				cat((AliasedColumn) o, sql);
			} else if (o instanceof AbstractRelationalOperator) {
				cat((AbstractRelationalOperator) o, appenderWrapper);
			} else if (o instanceof Iterable) {
				cat((Iterable<Object /* String, Column or AliasedColumn */>) o, sql);
			} else {
				throw new UnsupportedOperationException("Operator " + Reflections.toString(o.getClass()) + " is not implemented");
			}
			sql.cat(", ");
		}
		if (Strings.tail(sql, 2).equals(", ")) {	// if not, means select was empty
			// cut the traling comma
			sql.cutTail(2);
		}
	}
	
	protected void cat(String s, StringAppender sql) {
		sql.cat(s);
	}
	
	protected void cat(Column column, StringAppender sql) {
		sql.cat(dmlNameProvider.getName(column));
	}
	
	protected void cat(AliasedColumn column, StringAppender sql) {
		sql.cat(dmlNameProvider.getName(column.getColumn())).catIf(!Strings.isEmpty(column.getAlias()), " as ", column.getAlias());
	}
	
	private void cat(AbstractRelationalOperator operator, SQLAppender appenderWrapper) {
		operatorBuilder.cat(operator, appenderWrapper);
	}
}
