package org.gama.safemodel.description;

import org.gama.lang.Reflections;

import java.lang.reflect.Method;

/**
 * @author Guillaume Mary
 */
public class GetterSetterDescription extends MethodDescription {
	
	public final Method setter;
	
	public GetterSetterDescription(Class declaringClass, String getterName, String setterName, Class... parameterTypes) {
		this(Reflections.findMethod(declaringClass, getterName, parameterTypes), Reflections.findMethod(declaringClass, setterName, parameterTypes));
	}
	
	public GetterSetterDescription(Method getter, Method setter) {
		super(getter);
		this.setter = setter;
	}
	
	public String getGetterName() {
		return getName();
	}
	
	public String getSetterName() {
		return setter.getName();
	}
}
