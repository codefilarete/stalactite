package org.gama.safemodel.description;

/**
 * @author Guillaume Mary
 */
public abstract class AbstracNamedMemberDescription extends AbstractMemberDescription {
	
	private final String name;
	
	public AbstracNamedMemberDescription(Class declaringClass, String name) {
		super(declaringClass);
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
}
