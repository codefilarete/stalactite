package org.gama.safemodel;

import java.lang.reflect.Method;
import java.util.Iterator;

import org.gama.lang.Reflections;
import org.gama.safemodel.MetaModel.FieldDescription;
import org.gama.safemodel.MetaModel.MethodDescription;
import org.stalactite.reflection.AccessorByField;
import org.stalactite.reflection.AccessorByMethod;
import org.stalactite.reflection.AccessorChain;
import org.stalactite.reflection.ArrayAccessor;

/**
 * @author Guillaume Mary
 */
public class MetaModelAccessorBuilder<C, T> implements IMetaModelTransformer<AccessorChain<C, T>> {
	
	private AccessorChain<C, T> accessorChain;
	
	@Override
	public AccessorChain<C, T> transform(MetaModel metaModel) {
		Iterator<MetaModel> modelPathIterator = new MetaModelPathIterator(metaModel) {
			@Override
			protected void onFieldDescription(MetaModel model) {
				addFieldAccessor(model);
			}
			
			@Override
			protected void onMethodDescription(MetaModel model) {
				addMethodAccessor(model);
			}
			
			@Override
			protected void onArrayDescription(MetaModel model) {
				addArrayAccessor(model);
			}
		};
		accessorChain = new AccessorChain<>();
		// on parcourt tout les éléments, simplement pour déclencher les méthodes onXXXX() de l'Iterator
		while (modelPathIterator.hasNext()) {
			modelPathIterator.next();
		}
		return accessorChain;
	}
	
	private void addFieldAccessor(MetaModel model) {
		FieldDescription description = (FieldDescription) model.getDescription();
		this.accessorChain.add(new AccessorByField(Reflections.getField(description.getDeclaringClass(), description.getName())));
	}
	
	private void addMethodAccessor(final MetaModel model) {
		MethodDescription description = (MethodDescription) model.getDescription();
		Method method = Reflections.getMethod(description.getDeclaringClass(), description.getName(), description.getParameterTypes());
		AccessorByMethod accessorByMethod;
		if (description.getParameterTypes().length > 0) {
			if (model.getMemberParameter() != null) {
				accessorByMethod = new AccessorByMethod<>(method);
				// Passing MetaModel arguments to Accessor
				accessorByMethod.setParameters(model.getMemberParameter());
			} else {
				throw new IllegalArgumentException("Expected parameter but none was given (" + description.getDeclaringClass().getName() + "." + description.getName() + ")");
			}
		} else {
			accessorByMethod = new AccessorByMethod(method);
		}
		this.accessorChain.add(accessorByMethod);
	}
	
	private void addArrayAccessor(MetaModel model) {
		this.accessorChain.add(new ArrayAccessor<>((int) model.getMemberParameter()));
	}
}
