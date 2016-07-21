package org.gama.safemodel;

import java.util.Iterator;

import org.apache.wicket.Component;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Iterables;
import org.gama.safemodel.description.ContainerDescriptionPathIterator;

/**
 * A path builder to be used with the {@link org.apache.wicket.Component#get(String)} method. 
 * 
 * @author Guillaume Mary
 */
public class MetaModelPathComponentBuilder implements IMetaModelTransformer<String, MetaComponent> {
	
	/** Wicket path separator as String (default is Character) */
	public static final String PATH_SEPARATOR = "" + Component.PATH_SEPARATOR;
	
	private StringAppender path;
	
	/**
	 * Concatenate {@link ComponentDescription#getName()} with colon from root MetaComponent to this passed as argument.
	 * 
	 * @param metaModel the leaf model
	 * @return a String representing the path of wicket component (wicketIds)
	 */
	@Override
	public String transform(MetaComponent metaModel) {
		ContainerDescriptionPathIterator<ComponentDescription> containerDescriptionPathIterator = new ContainerDescriptionPathIterator<>(metaModel);
		Iterator<ComponentDescription> modelPathIterator = Iterables.reverseIterator(Iterables.copy(containerDescriptionPathIterator));
		path = new StringAppender(100);
		// we iterate over all elements, only in order to invoke dedicated onXXXX() iterator's methods
		while (modelPathIterator.hasNext()) {
			ComponentDescription containerDescription = modelPathIterator.next();
			path.cat(PATH_SEPARATOR, containerDescription.getName());
		}
		// final finish
		if (path.charAt(0) == Component.PATH_SEPARATOR) {
			path.cutHead(1);
		}
		return path.toString();
	}
	
}
