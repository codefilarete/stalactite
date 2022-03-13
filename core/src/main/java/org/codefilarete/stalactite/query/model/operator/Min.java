package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.query.model.UnitaryOperator;

/**
 * Represents a min operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Min extends UnitaryOperator {
	
	public Min(Column value) {
		super(value);
	}
}
