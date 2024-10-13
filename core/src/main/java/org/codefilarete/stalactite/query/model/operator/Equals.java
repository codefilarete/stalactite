package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.UnitaryOperator;

/**
 * Represents an equals comparison
 * 
 * @author Guillaume Mary
 */
public class Equals<O> extends UnitaryOperator<O> {
	
	public Equals() {
	}
	
	public Equals(O value) {
		super(value);
	}
	
	public BiOperandOperator<CharSequence> ignoringCase() {
		return new BiOperandOperator<CharSequence>() {
			@Override
			public Object[] asRawCriterion(Selectable<CharSequence> selectable) {
				return new Object[] { new LowerCase<>(selectable), Equals.this };
			}
			
			@Override
			public void setValue(CharSequence value) {
				Equals.this.setValue((O) value);
			}
		};
	}
}
