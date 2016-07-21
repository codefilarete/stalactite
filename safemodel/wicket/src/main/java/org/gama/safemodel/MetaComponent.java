package org.gama.safemodel;

import java.util.ArrayList;
import java.util.List;

import org.apache.wicket.Component;

/**
 * @author Guillaume Mary
 * @param <O> the owning MetaModel
 * @param <C> the owning Component type
 * @param <T> the target Component type
 */
public class MetaComponent<O extends MetaModel, C extends Component, T extends Component> extends MetaModel<O, ComponentDescription<C, T>> {
	
	/** Shortcut for constructor */
	public static <C extends Component, T extends Component> ComponentDescription<C, T> component(Class<C> declaringContainer, String name, Class<T> type) {
		return new ComponentDescription<>(declaringContainer, name, type);
	}
	
	public MetaComponent() {
	}
	
	public MetaComponent(ComponentDescription<C, T> componentDescription) {
		super(componentDescription);
	}
	
	public T get(C owner) {
		return (T) owner.get(getDescription().getName());
	}
	
	/**
	 * Gives the path of this component from its root, of the form Wicket expect it.
	 * Same principle that {@link MetaModelPathComponentBuilder} (without using it)
	 * 
	 * @return a String that represents the path of this component from its root
	 */
	public String givePathFromRoot() {
		List<String> path = new ArrayList<>();
		path.add(0, getDescription().getName());
		MetaModel<?, ComponentDescription> owner = getOwner();
		while(owner != null) {
			// Root owner doesn't have description so we prevent NPE
			boolean isRoot = owner.getOwner() == null;
			if (!isRoot) {
				path.add(0, owner.getDescription().getName());
			}
			owner = owner.getOwner();
		}
		return String.join(MetaModelPathComponentBuilder.PATH_SEPARATOR, path);
	}
}
