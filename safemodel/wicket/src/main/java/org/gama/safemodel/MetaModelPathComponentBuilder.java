package org.gama.safemodel;

import java.util.Iterator;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Iterables;
import org.gama.safemodel.description.ContainerDescription;
import org.gama.safemodel.description.ContainerDescriptionPathIterator;

/**
 * A path builder to be used with the {@link org.apache.wicket.Component#get(String)} method. 
 * 
 * @author Guillaume Mary
 */
public class MetaModelPathComponentBuilder implements IMetaModelTransformer<String> {
	
	private StringAppender path;
	
	/**
	 * Concatenate {@link ComponentDescription#getName()} with colon from root MetaComponent to this passed as argument.
	 * 
	 * @param metaModel the leaf model
	 * @return a String representing the path of wicket component (wicketIds)
	 */
	@Override
	public String transform(MetaModel<? extends MetaModel, ? extends ContainerDescription> metaModel) {
		Iterator<ContainerDescription> modelPathIterator = Iterables.reverseIterator(
				Iterables.copy((Iterator<ContainerDescription>) new ContainerDescriptionPathIterator<>(metaModel)));
		path = new StringAppender(100);
		// we iterate over all elements, only in order to invoke dedicated onXXXX() iterator's methods
		while (modelPathIterator.hasNext()) {
			ContainerDescription containerDescription = modelPathIterator.next();
			path.cat(":", ((ComponentDescription) containerDescription).getName());
		}
		// final finish
		if (path.charAt(0) == ':') {
			path.cutHead(1);
		}
		return path.toString();
	}
	
}
