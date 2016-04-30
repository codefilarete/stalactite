package org.gama.safemodel;

import org.gama.safemodel.description.ContainerDescription;

/**
 * @author Guillaume Mary
 */
public interface IMetaModelTransformer<R> {
	
	R transform(MetaModel<? extends MetaModel, ? extends ContainerDescription> metaModel);
}
