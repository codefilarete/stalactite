package org.gama.stalactite.query.builder;

import java.util.Map;

import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.Select;
import org.gama.stalactite.query.model.Select.AliasedColumn;

/**
 * @author Guillaume Mary
 */
public class SelectBuilder extends AbstractDMLBuilder implements SQLBuilder {
	
	private static final String COLUMN_SEPARATOR = ", ";
	private final Select select;
	
	public SelectBuilder(Select select, Map<Table, String> tableAliases) {
		super(tableAliases);
		this.select = select;
	}
	
	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(200);
		sql.catIf(this.select.isDistinct(), "distinct ");
		for (Object o : this.select) {
			if (o instanceof String) {
				cat((String) o, sql);
			} else if (o instanceof Column) {
				cat((Column) o, sql);
			} else if (o instanceof AliasedColumn) {
				cat((AliasedColumn) o, sql);
			}
		}
		// cut the always traling comma
		sql.cutTail(2);
		return sql.toString();
	}
	
	protected void cat(String o, StringAppender sql) {
		sql.cat(o, COLUMN_SEPARATOR);
	}
	
	protected void cat(Column o, StringAppender sql) {
		sql.cat(getName(o), COLUMN_SEPARATOR);
	}
	
	protected void cat(AliasedColumn o, StringAppender sql) {
		sql.cat(getName(o.getColumn())).catIf(!Strings.isEmpty(o.getAlias()), " as ", o.getAlias()).cat(COLUMN_SEPARATOR);
	}
}
