package org.stalactite.lang.bean.safemodel.model;

/**
 * @author Guillaume Mary
 */
public class Phone {
	
	private String number;
	
	public Phone() {
	}
	
	public Phone(String number) {
		this.number = number;
	}
	
	public String getNumber() {
		return number;
	}
}
