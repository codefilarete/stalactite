package org.gama.safemodel;

import org.apache.wicket.Component;
import org.gama.safemodel.description.ContainerDescription;

/**
 * @author Guillaume Mary
 */
public class ComponentDescription<C extends Component, T extends Component> extends ContainerDescription<Class<C>> {
	
	private final String name;
	private final Class<T> type;
	
	public ComponentDescription(Class<C> declaringContainer, String name) {
		this(declaringContainer, name, null);
	}
	
	public ComponentDescription(Class<C> declaringContainer, String name, Class<T> type) {
		super(declaringContainer);
		this.name = name;
		this.type = type;
	}
	
	/**
	 * @return the wicket identifier of the represented {@link Component}
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * @return the type of the represented {@link Component}
	 */
	public Class<T> getType() {
		return type;
	}
}
