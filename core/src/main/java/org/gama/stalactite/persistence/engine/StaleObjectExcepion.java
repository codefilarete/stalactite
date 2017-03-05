package org.gama.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public class StaleObjectExcepion extends RuntimeException {
	
	private final int expectedRowCount;
	private final int effectiveRowCount;
	
	public StaleObjectExcepion(int expectedRowCount, int effectiveRowCount) {
		this.expectedRowCount = expectedRowCount;
		this.effectiveRowCount = effectiveRowCount;
	}
	
	@Override
	public String getMessage() {
		return expectedRowCount + " rows were expected to be hit but only " + effectiveRowCount + " were effectively";
	}
}
