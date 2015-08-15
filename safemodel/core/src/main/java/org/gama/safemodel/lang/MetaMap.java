package org.gama.safemodel.lang;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.MethodDescription;

import java.util.Map;

/**
 * @author Guillaume Mary
 */
public class MetaMap<K, V, O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	private MetaModel<MetaMap, MethodDescription<V>> get = new MetaModel<>((MethodDescription<V>) method(Map.class, "get", Object.class, Object.class));
	
	private MetaModel<MetaMap, MethodDescription<V>> put = new MetaModel<>((MethodDescription<V>) method(Map.class, "put", Object.class, Object.class, Object.class));
	
	public MetaMap(M description) {
		super(description);
		get.setOwner(this);
		put.setOwner(this);
	}
	
	public MetaModel<MetaMap, MethodDescription<V>> put() {
		return put;
	}
}
