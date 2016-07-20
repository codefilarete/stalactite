package org.gama.safemodel;

import org.apache.wicket.Component;
import org.gama.safemodel.description.ContainerDescription;

/**
 * @author Guillaume Mary
 */
public class ComponentDescription<C extends Component> extends ContainerDescription<Class<C>> {
	
	/** Shortcut for constructor */
	public static <C extends Component> ComponentDescription<C> component(Class<C> declaringContainer, String name) {
		return new ComponentDescription<>(declaringContainer, name);
	}
	
	private final String name;
	
	public ComponentDescription(Class<C> declaringContainer, String name) {
		super(declaringContainer);
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
}
