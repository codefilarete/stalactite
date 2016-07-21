package org.gama.safemodel;

/**
 * @author Guillaume Mary
 */
public interface IMetaModelTransformer<R, M extends MetaModel> {
	
	R transform(M metaModel);
}
