package org.codefilarete.stalactite.engine.model.book;

public class Publisher extends AbstractEntity {
	
	private String name;
	
	private BusinessCategory category;
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public BusinessCategory getCategory() {
		return category;
	}
	
	public void setCategory(BusinessCategory category) {
		this.category = category;
	}
}
