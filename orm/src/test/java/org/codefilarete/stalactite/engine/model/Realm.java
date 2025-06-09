package org.codefilarete.stalactite.engine.model;

import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

public class Realm extends Country {
	
	private King king;
	
	public Realm() {
	}
	
	public Realm(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Realm(Identifier<Long> id) {
		super(id);
	}
	
	public King getKing() {
		return king;
	}
	
	public void setKing(King king) {
		this.king = king;
	}
}