package org.stalactite.query.model;

/**
 * @author guillaume.mary
 */
public abstract class Aliased {
	private String alias;

	public Aliased() {
	}

	public Aliased(String alias) {
		this.alias = alias;
	}

	public void as(String alias) {
		this.alias = alias;
	}

	public String getAlias() {
		return alias;
	}
}
