package org.codefilarete.stalactite.engine.configurer.property;

import javax.annotation.Nullable;

import org.codefilarete.stalactite.sql.ddl.Size;

public class ColumnLinkageOptionsSupport implements LocalColumnLinkageOptions {
	
	private String columnName;
	
	private Size columnSize;
	
	public ColumnLinkageOptionsSupport() {
	}
	
	public ColumnLinkageOptionsSupport(@Nullable String columnName) {
		this.columnName = columnName;
	}
	
	public ColumnLinkageOptionsSupport(@Nullable Size columnSize) {
		this.columnSize = columnSize;
	}
	
	@Nullable
	@Override
	public String getColumnName() {
		return this.columnName;
	}
	
	@Override
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	@Nullable
	@Override
	public Size getColumnSize() {
		return columnSize;
	}
	
	@Override
	public void setColumnSize(Size columnSize) {
		this.columnSize = columnSize;
	}
}
