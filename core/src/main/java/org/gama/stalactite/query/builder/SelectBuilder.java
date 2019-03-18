package org.gama.stalactite.query.builder;

import java.util.Map;

import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.builder.OperandBuilder.StringAppenderWrapper;
import org.gama.stalactite.query.model.Operand;
import org.gama.stalactite.query.model.Select;
import org.gama.stalactite.query.model.Select.AliasedColumn;

/**
 * @author Guillaume Mary
 */
public class SelectBuilder implements SQLBuilder {
	
	private final Select select;
	private final DMLNameProvider dmlNameProvider;
	private final OperandBuilder operandBuilder;
	
	public SelectBuilder(Select select, Map<Table, String> tableAliases) {
		this(select, new DMLNameProvider(tableAliases));
	}
	
	public SelectBuilder(Select select, DMLNameProvider dmlNameProvider) {
		this.select = select;
		this.dmlNameProvider = dmlNameProvider;
		this.operandBuilder = new OperandBuilder(this.dmlNameProvider);
	}
	
	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(200);
		StringAppenderWrapper appenderWrapper = new StringAppenderWrapper(sql, dmlNameProvider);
		sql.catIf(this.select.isDistinct(), "distinct ");
		for (Object o : this.select) {
			if (o instanceof String) {
				cat((String) o, sql);
			} else if (o instanceof Column) {
				cat((Column) o, sql);
			} else if (o instanceof AliasedColumn) {
				cat((AliasedColumn) o, sql);
			} else if (o instanceof Operand) {
				cat((Operand) o, appenderWrapper);
			} else {
				throw new UnsupportedOperationException("Operator " + Reflections.toString(o.getClass()) + " is not implemented");
			}
			sql.cat(", ");
		}
		// cut the always traling comma
		sql.cutTail(2);
		return sql.toString();
	}
	
	protected void cat(String o, StringAppender sql) {
		sql.cat(o);
	}
	
	protected void cat(Column o, StringAppender sql) {
		sql.cat(dmlNameProvider.getName(o));
	}
	
	protected void cat(AliasedColumn o, StringAppender sql) {
		sql.cat(dmlNameProvider.getName(o.getColumn())).catIf(!Strings.isEmpty(o.getAlias()), " as ", o.getAlias());
	}
	
	private void cat(Operand operand, StringAppenderWrapper appenderWrapper) {
		operandBuilder.cat(operand, appenderWrapper);
	}
}
