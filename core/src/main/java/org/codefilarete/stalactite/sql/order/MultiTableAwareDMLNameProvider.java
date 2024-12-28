package org.codefilarete.stalactite.sql.order;

import java.util.Collections;
import java.util.HashMap;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.tool.trace.ModifiableBoolean;

/**
 * A name provider that add table prefix to columns in case of multi table o avoid column name conflict.
 * This behavior must be deactivated through {@link #setMultiTable(boolean)}.
 * 
 * Designed as both inheriting from {@link DMLNameProvider} for type compatibility with {@link org.codefilarete.stalactite.query.builder.SQLAppender}s
 * methods, and as a wrapper of a {@link DMLNameProvider} to let user call a shared {@link DMLNameProviderFactory}.
 * 
 * @see #setMultiTable(boolean)
 */
public class MultiTableAwareDMLNameProvider extends DMLNameProvider {
	
	/** State defining if this name provider should add table prefix to column */
	private final ModifiableBoolean multiTable = new ModifiableBoolean(false);
	
	private final DMLNameProvider delegate;
	
	public MultiTableAwareDMLNameProvider(DMLNameProviderFactory delegateFactory) {
		super(Collections.emptyMap());
		this.delegate = delegateFactory.build(new HashMap<>());
	}
	
	/**
	 * Overridden to add column prefix when multi-table is on
	 * @param column a column
	 * @return
	 */
	@Override
	public String getName(Selectable<?> column) {
		if (multiTable.isTrue()) {
			// default : adds table prefix
			return delegate.getName(column);
		} else {
			return delegate.getSimpleName(column);
		}
	}
	
	public void setMultiTable(boolean multiTable) {
		this.multiTable.setValue(multiTable);
	}
	
	public boolean isMultiTable() {
		return multiTable.getValue();
	}
	
	@Override
	public String getSimpleName(Selectable<?> column) {
		return delegate.getSimpleName(column);
	}
	
	@Override
	public String getAlias(Fromable table) {
		return delegate.getAlias(table);
	}
	
	@Override
	public String getTablePrefix(Fromable table) {
		return delegate.getTablePrefix(table);
	}
}
