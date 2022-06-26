package org.codefilarete.stalactite.query.builder;

import java.util.Map;

import org.codefilarete.stalactite.query.builder.OperatorSQLBuilder.SQLAppender;
import org.codefilarete.stalactite.query.builder.OperatorSQLBuilder.StringAppenderWrapper;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;

/**
 * @author Guillaume Mary
 */
public class SelectBuilder implements SQLBuilder {
	
	private final Select select;
	private final DMLNameProvider dmlNameProvider;
	private final OperatorSQLBuilder operatorSqlBuilder;
	
	public SelectBuilder(Select select, Map<Table, String> tableAliases) {
		this(select, new DMLNameProvider(tableAliases));
	}
	
	public SelectBuilder(Select select, DMLNameProvider dmlNameProvider) {
		this.select = select;
		this.dmlNameProvider = dmlNameProvider;
		this.operatorSqlBuilder = new OperatorSQLBuilder(this.dmlNameProvider);
	}
	
	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(200);
		sql.catIf(this.select.isDistinct(), "distinct ");
		cat(this.select, sql);
		return sql.toString();
	}
	
	private void cat(Iterable<? extends Selectable<?> /* String, Column or AliasedColumn */> select, StringAppender sql) {
		StringAppenderWrapper appenderWrapper = new StringAppenderWrapper(sql, dmlNameProvider);
		for (Object o : select) {
			if (o instanceof AbstractRelationalOperator) {
				cat((AbstractRelationalOperator) o, appenderWrapper);
			} else if (o instanceof Selectable) {	// must be after previous ifs because they deal with dedicated Selectable cases
				cat((Selectable) o, sql);
			} else if (o instanceof Iterable) {
				cat((Iterable<? extends Selectable<?> /* String, Column or AliasedColumn */>) o, sql);
			} else {
				throw new UnsupportedOperationException("Operator " + Reflections.toString(o.getClass()) + " is not implemented");
			}
			sql.cat(", ");
		}
		if (Strings.tail(sql, 2).equals(", ")) {	// if not, means select was empty
			// cut the trailing comma
			sql.cutTail(2);
		}
	}
	
	protected void cat(Selectable<?> column, StringAppender sql) {
		String alias = select.getAliases().get(column);
		sql.cat(dmlNameProvider.getName(column)).catIf(!Strings.isEmpty(alias), " as ", alias);
	}
	
	private void cat(AbstractRelationalOperator<?> operator, SQLAppender appenderWrapper) {
		String alias = select.getAliases().get(operator);	// can be UnitaryOperator which is Selectable
		operatorSqlBuilder.cat(operator, appenderWrapper);
		appenderWrapper.catIf(!Strings.isEmpty(alias), " as " + alias);
	}
}
