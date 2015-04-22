package org.stalactite.lang.bean.safemodel;

/**
 * @author Guillaume Mary
 */
public class MetaCity<O extends MetaModel> extends MetaModel<O> {
	
	public MetaCity() {
	}
	
	public MetaCity(FieldDescription accessor) {
		super(accessor);
	}
}
