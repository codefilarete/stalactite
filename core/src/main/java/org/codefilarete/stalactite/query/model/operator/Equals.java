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
		LowerCase<CharSequence> lowerCase = new LowerCase<>();
		Equals<LowerCase<CharSequence>> equals = new Equals<>(lowerCase);
		return new BiOperandOperator<CharSequence>() {
			@Override
			public Object[] asRawCriterion(Selectable<CharSequence> selectable) {
				return new Object[] { new LowerCase<>(selectable), equals };
			}
			
			@Override
			public void setValue(CharSequence value) {
				lowerCase.setValue(value);
			}
		};
	}
}
