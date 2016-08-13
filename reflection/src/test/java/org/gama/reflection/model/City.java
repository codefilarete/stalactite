package org.gama.reflection.model;

/**
 * @author Guillaume Mary
 */
public class City {
	
	private String name;
	
	public City(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String name() {
		return this.name;
	}
}
