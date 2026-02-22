package org.codefilarete.stalactite.query.model.operator;

import org.codefilarete.stalactite.query.api.Selectable;

public class Avg<N extends Number> extends SQLFunction<Selectable<N>, Long> {
	
	public Avg(Selectable<N> value) {
		super("avg", Long.class, value);
	}
}
