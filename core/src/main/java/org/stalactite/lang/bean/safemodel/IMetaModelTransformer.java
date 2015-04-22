package org.stalactite.lang.bean.safemodel;

/**
 * @author Guillaume Mary
 */
public interface IMetaModelTransformer<R> {
	
	R transform(MetaModel metaModel);
}
