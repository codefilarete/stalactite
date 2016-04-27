package org.gama.safemodel.metamodel;

import org.gama.safemodel.MetaMember;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.ArrayDescription;
import org.gama.safemodel.description.MethodDescription;

import static org.gama.safemodel.MetaMember.method;

/**
 * @author Guillaume Mary
 */
public class MetaString<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	public MetaString() {
	}
	
	public MetaString(M description) {
		super(description);
	}
	
	public MetaString(M description, O owner) {
		super(description, owner);
	}
	
	public MetaModel charAt(int index) {
		MetaModel<MetaString, MethodDescription<Character>> chartAt = new MetaModel<>(MetaMember.method(String.class, "charAt", Character.TYPE, Integer.TYPE));
		chartAt.setParameter(index);
		chartAt.setOwner(this);
		return chartAt;
	}
	
	public MetaModel toCharArray(int index) {
		MetaModel<MetaString, MethodDescription<Character[]>> toCharArray = (MetaModel<MetaString, MethodDescription<Character[]>>)
				new MetaModel(method(String.class, "toCharArray", char[].class));
		toCharArray.setOwner(this);
		MetaModel<MetaModel, ArrayDescription> toCharArrayAccessor = new MetaModel<>(new ArrayDescription(String.class));
		toCharArrayAccessor.setOwner(toCharArray);
		toCharArrayAccessor.setParameter(index);
		return toCharArrayAccessor;
	}
	
}
