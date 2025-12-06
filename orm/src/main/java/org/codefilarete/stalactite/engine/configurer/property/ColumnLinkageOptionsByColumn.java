package org.codefilarete.stalactite.engine.configurer.property;

import javax.annotation.Nullable;

import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

public class ColumnLinkageOptionsByColumn implements LocalColumnLinkageOptions {
	
	private final Column column;
	
	public ColumnLinkageOptionsByColumn(Column column) {
		this.column = column;
	}
	
	public Column getColumn() {
		return column;
	}
	
	@Override
	public String getColumnName() {
		return this.column.getName();
	}
	
	public Class<?> getColumnType() {
		return this.column.getJavaType();
	}
	
	@Nullable
	@Override
	public Size getColumnSize() {
		return this.column.getSize();
	}
	
	@Override
	public void setColumnName(String columnName) {
		// no-op, column is already defined
	}
	
	@Override
	public void setColumnSize(Size columnSize) {
		// no-op, column is already defined
	}
}
