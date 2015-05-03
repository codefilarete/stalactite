package org.gama.safemodel;

/**
 * @author Guillaume Mary
 */
public interface IMetaModelTransformer<R> {
	
	R transform(MetaModel metaModel);
}
