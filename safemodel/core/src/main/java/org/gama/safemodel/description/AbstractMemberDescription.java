package org.gama.safemodel.description;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractMemberDescription {
	
	private final Class declaringClass;
	
	public AbstractMemberDescription(Class declaringClass) {
		this.declaringClass = declaringClass;
	}
	
	public Class getDeclaringClass() {
		return declaringClass;
	}
}
