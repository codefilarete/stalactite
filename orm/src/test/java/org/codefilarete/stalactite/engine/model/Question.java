package org.codefilarete.stalactite.engine.model;

public class Question extends Element {
	
	private String label;
	
	public Question() {
		this(0);
	}
	
	public Question(long id) {
		super(id);
	}
	
	public String getLabel() {
		return label;
	}
	
	public Question setLabel(String label) {
		this.label = label;
		return this;
	}
}
