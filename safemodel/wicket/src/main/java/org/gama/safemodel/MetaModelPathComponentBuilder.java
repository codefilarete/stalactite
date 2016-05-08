package org.gama.safemodel;

import java.util.Iterator;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.ReadOnlyIterator;
import org.gama.safemodel.description.ContainerDescription;

/**
 * @author Guillaume Mary
 */
public class MetaModelPathComponentBuilder implements IMetaModelTransformer<String> {
	
	private StringAppender path;
	
	@Override
	public String transform(MetaModel<? extends MetaModel, ? extends ContainerDescription> metaModel) {
		Iterator<ContainerDescription> modelPathIterator = Iterables.reverseIterator(
				Iterables.copy((Iterator<ContainerDescription>) new DescriptionIterator<>(metaModel)));
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
	
	private static class DescriptionIterator<C extends ContainerDescription> extends ReadOnlyIterator<C> {
		
		private MetaModel<? extends MetaModel, C> x;
		
		public DescriptionIterator(MetaModel<? extends MetaModel, C> leafMetaModel) {
			x = leafMetaModel;
		}
		
		@Override
		public boolean hasNext() {
			return x != null && x.getDescription() != null;
		}
		
		@Override
		public C next() {
//			path.cat(":", ((ComponentDescription) x.getDescription()).getName());
			MetaModel next = x;
			x = x.getOwner();
			return (C) next.getDescription();
		}
	}
}
