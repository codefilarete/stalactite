package org.codefilarete.stalactite.sql.order;

import java.util.Collections;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.tool.trace.ModifiableBoolean;

/**
 * A name provider that doesn't add table prefix to columns in case of single table.
 * This behavior must be deactivated through {@link #setMultiTable(boolean)}.
 * 
 * @see #setMultiTable(boolean)
 */
public class MultiTableAwareDMLNameProvider extends DMLNameProvider {
	
	/** State defining if this name provider should add table prefix to column */
	private final ModifiableBoolean multiTable = new ModifiableBoolean(false);
	
	public MultiTableAwareDMLNameProvider() {
		super(Collections.emptyMap());
	}
	
	@Override
	public String getName(Selectable<?> column) {
		if (multiTable.isTrue()) {
			// default : adds table prefix
			return super.getName(column);
		} else {
			return getSimpleName(column);
		}
	}
	
	public void setMultiTable(boolean multiTable) {
		this.multiTable.setValue(multiTable);
	}
	
	public boolean isMultiTable() {
		return multiTable.getValue();
	}
}
