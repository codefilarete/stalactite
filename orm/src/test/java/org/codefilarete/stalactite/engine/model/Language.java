package org.codefilarete.stalactite.engine.model;

import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;

public class Language implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private final String code;
	
	public Language(Identifier<Long> id, String code) {
		this.id = id;
		this.code = code;
	}
	
	@Override
	public Identifier<Long> getId() {
		return id;
	}
	
	public String getCode() {
		return code;
	}
}
