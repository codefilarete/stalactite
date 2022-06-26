package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.UnitaryOperator;

/**
 * @author Guillaume Mary
 */
public class Cast<C> extends UnitaryOperator<Selectable<C>> implements Selectable<C> {
	
	public Cast(Selectable<C> value) {
		super(value);
	}
	
	@Override
	public String getExpression() {
		return "cast";
	}
	
	@Override
	public Class<C> getJavaType() {
		return this.getValue().getJavaType();
	}
}
