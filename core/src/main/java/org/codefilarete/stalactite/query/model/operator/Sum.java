package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.query.model.UnitaryOperator;

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
