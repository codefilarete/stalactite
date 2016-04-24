package org.gama.safemodel.description;

/**
 * A descrption of a member of a class.
 * Made abstract because it doesn't have a lot of value alone and must be extended for field, method, etc.
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractMemberDescription extends ContainerDescription<Class> {
	
	public AbstractMemberDescription(Class declaringClass) {
		super(declaringClass);
	}
	
	public Class getDeclaringClass() {
		return getDeclaringContainer();
	}
}
