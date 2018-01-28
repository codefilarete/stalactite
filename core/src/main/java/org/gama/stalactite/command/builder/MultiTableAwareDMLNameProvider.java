package org.gama.stalactite.command.builder;

import java.util.Collections;

import org.gama.lang.trace.ModifiableBoolean;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.builder.DMLNameProvider;

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
	public String getName(Column column) {
		if (multiTable.isTrue()) {
			// default : adds table prefix
			return super.getName(column);
		} else {
			return column.getName();
		}
	}
	
	public void setMultiTable(boolean multiTable) {
		this.multiTable.setValue(multiTable);
	}
	
	public boolean isMultiTable() {
		return multiTable.getValue();
	}
}
