package org.codefilarete.stalactite.engine.model;

import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

public class Republic extends Country {
	
	private Person primeMinister;
	
	private int deputeCount;
	
	public Republic() {
	}
	
	public Republic(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Republic(Identifier<Long> id) {
		super(id);
	}
	
	public Person getPrimeMinister() {
		return primeMinister;
	}
	
	public void setPrimeMinister(Person primeMinister) {
		this.primeMinister = primeMinister;
	}
	
	public int getDeputeCount() {
		return deputeCount;
	}
	
	public void setDeputeCount(int deputeCount) {
		this.deputeCount = deputeCount;
	}
}