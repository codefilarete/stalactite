package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.UnitaryOperator;

/**
 * Represents a sum operation (on a column)
 * 
 * @author Guillaume Mary
 */
public class Sum<N extends Number> extends UnitaryOperator<Column<?, N>> {
	
	public Sum(Column<?, N> value) {
		super(value);
	}
}
