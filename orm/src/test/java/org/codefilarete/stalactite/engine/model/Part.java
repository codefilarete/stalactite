package org.codefilarete.stalactite.engine.model;

public class Part extends Element {
	
	private String name;
	
	public Part() {
		this(0);
	}
	
	public Part(long id) {
		super(id);
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
}
