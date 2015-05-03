package org.gama.safemodel.description;

/**
 * @author Guillaume Mary
 */
public abstract class AbstracNamedtMemberDescription extends AbstractMemberDescription {
	
	private final String name;
	
	public AbstracNamedtMemberDescription(Class declaringClass, String name) {
		super(declaringClass);
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
}
