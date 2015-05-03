package org.gama.safemodel;

import java.util.ArrayList;
import java.util.Iterator;

import org.gama.safemodel.MetaModel.AbstractMemberDescription;
import org.gama.safemodel.MetaModel.ArrayDescription;
import org.gama.safemodel.MetaModel.FieldDescription;
import org.gama.safemodel.MetaModel.MethodDescription;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.ReadOnlyIterator;

/**
 * @author Guillaume Mary
 */
public abstract class MetaModelPathIterator extends ReadOnlyIterator<MetaModel> {
	
	private final Iterator<MetaModel> modelPathIterator;
	
	public MetaModelPathIterator(MetaModel metaModel) {
		modelPathIterator = buildMetaModelIterator(metaModel);
	}
	
	@Override
	public boolean hasNext() {
		return modelPathIterator.hasNext();
	}
	
	@Override
	public MetaModel getNext() {
		MetaModel childModel = modelPathIterator.next();
		AbstractMemberDescription description = childModel.getDescription();
		if (description instanceof FieldDescription) {
			onFieldDescription(childModel);
		} else if (description instanceof MethodDescription) {
			onMethodDescription(childModel);
		} else if (description instanceof ArrayDescription) {
			onArrayDescription(childModel);
		}
		return childModel;
	}
	
	private Iterator<MetaModel> buildMetaModelIterator(MetaModel metaModel) {
		// le paramètre d'entrée est le dernier fils, il faut inverser la relation
		// pour se simplifier la construction du chemin
		ArrayList<MetaModel> modelPath = new ArrayList<>(10);
		MetaModel owner = metaModel;
		while (owner != null) {
			modelPath.add(owner);
			owner = owner.getOwner();
		}
		return Iterables.reverseIterator(modelPath);
	}
	
	protected abstract void onFieldDescription(MetaModel childModel);
	
	protected abstract void onMethodDescription(MetaModel childModel);
	
	protected abstract void onArrayDescription(MetaModel childModel);
}
