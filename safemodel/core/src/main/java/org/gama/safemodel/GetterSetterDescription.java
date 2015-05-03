package org.gama.safemodel;

/**
 * @author Guillaume Mary
 */
public class GetterSetterDescription extends MethodDescription {
	
	private final String setterName;
	
	public GetterSetterDescription(Class declaringClass, String getterName, String setterName, Class... parameterTypes) {
		super(declaringClass, getterName, parameterTypes);
		this.setterName = setterName;
	}
	
	public String getGetterName() {
		return getName();
	}
	
	public String getSetterName() {
		return setterName;
	}
}
