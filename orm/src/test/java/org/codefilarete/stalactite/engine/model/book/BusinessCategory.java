package org.codefilarete.stalactite.engine.model.book;

import java.util.HashSet;
import java.util.Set;

public class BusinessCategory extends AbstractEntity {
	
	private String name;
	
	private Set<Publisher> publishers = new HashSet<>();
	
	public BusinessCategory() {
	}
	
	public BusinessCategory(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public Set<Publisher> getPublishers() {
		return publishers;
	}
}
