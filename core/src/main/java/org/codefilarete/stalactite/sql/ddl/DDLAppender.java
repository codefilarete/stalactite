package org.codefilarete.stalactite.sql.ddl;

import org.codefilarete.tool.StringAppender;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;

/**
 * A {@link StringAppender} that automatically appends {@link Table} and {@link Column} names
 */
public class DDLAppender extends StringAppender {
	
	protected final DMLNameProvider dmlNameProvider;
	
	public DDLAppender(DMLNameProvider dmlNameProvider, Object... o) {
		this.dmlNameProvider = dmlNameProvider;
		cat(o);
	}
	
	public DDLAppender(StringBuilder delegate, DMLNameProvider dmlNameProvider, Object... o) {
		super(delegate);
		this.dmlNameProvider = dmlNameProvider;
		cat(o);
	}
	
	/**
	 * Overridden to append {@link Table} and {@link Column} names according to {@link DMLNameProvider} given at construction time
	 * 
	 * @param o any object
	 * @return this
	 */
	@Override
	public StringAppender cat(Object o) {
		if (o instanceof Table) {
			return super.cat(dmlNameProvider.getName((Table) o));
		} else if (o instanceof Column) {
			return super.cat(dmlNameProvider.getSimpleName((Column) o));
		} else {
			return super.cat(o);
		}
	}
}
