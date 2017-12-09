package org.gama.stalactite.query.model.operand;

import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.Operand;

/**
 * @author Guillaume Mary
 */
public class Count extends Operand {
	
	public Count(Column value) {
		super(value);
	}
}
