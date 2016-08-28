package org.gama.safemodel.lang;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.ArrayDescription;
import org.gama.safemodel.description.MethodDescription;

import static org.gama.safemodel.description.MethodDescription.method;

/**
 * @author Guillaume Mary
 */
public class MetaString<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	private MetaModel<MetaString, MethodDescription<Integer>> length = new MetaModel<>(MethodDescription.method(String.class, "length", Integer.TYPE));
	
	public MetaString() {
	}
	
	public MetaString(M description) {
		super(description);
		length.setOwner(this);
	}
	
	public MetaString(M description, O owner) {
		super(description, owner);
		length.setOwner(this);
	}
	
	public MetaModel<MetaString, MethodDescription<Integer>> length() {
		return length;
	}
	
	public MetaModel<MetaString, MethodDescription<Character>> charAt(int index) {
		MetaModel<MetaString, MethodDescription<Character>> chartAt = new MetaModel<>(method(String.class, "charAt", Character.TYPE, Integer.TYPE));
		chartAt.setParameter(index);
		chartAt.setOwner(this);
		return chartAt;
	}
	
	public MetaModel<MetaModel, ArrayDescription> toCharArray(int index) {
		MetaModel<MetaString, MethodDescription<char[]>> toCharArray = new MetaModel<>(method(String.class, "toCharArray", char[].class));
		toCharArray.setOwner(this);
		MetaModel<MetaModel, ArrayDescription> toCharArrayAccessor = new MetaModel<>(new ArrayDescription(String.class));
		toCharArrayAccessor.setOwner(toCharArray);
		toCharArrayAccessor.setParameter(index);
		return toCharArrayAccessor;
	}
}
