package org.gama.safemodel.description;

/**
 * @author Guillaume Mary
 */
public class MethodDescription extends AbstracNamedtMemberDescription {
	
	private final Class[] parameterTypes;
	
	public MethodDescription(Class declaringClass, String name, Class... parameterTypes) {
		super(declaringClass, name);
		this.parameterTypes = parameterTypes;
	}
	
	public Class[] getParameterTypes() {
		return parameterTypes;
	}
}
