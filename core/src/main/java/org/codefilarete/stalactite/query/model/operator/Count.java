package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.query.model.UnitaryOperator;

/**
 * Represents a count operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Count extends UnitaryOperator {
	
	public Count(Column value) {
		super(value);
	}
}
