package org.gama.safemodel;

import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.FieldDescription;
import org.gama.safemodel.description.MethodDescription;

/**
 * Parent for MetaModel that describes a member of a class
 * 
 * @author Guillaume Mary
 */
public class MetaMember<O extends MetaModel, D extends AbstractMemberDescription> extends MetaModel<O, D> {
	
	public static <R> FieldDescription<R> field(Class clazz, String name, Class<R> fieldType) {
		FieldDescription<R> fieldDescription = new FieldDescription<>(clazz, name);
		if (!fieldDescription.getFieldType().equals(fieldType)) {
			throw new IllegalArgumentException("Wrong field type given: declared "+fieldType.getName() + " but is " + fieldDescription.getFieldType().getName());
		}
		return fieldDescription;
	}
	
	public static <R> MethodDescription<R> method(Class clazz, String name, Class<R> returnType, Class... parameterTypes) {
		MethodDescription<R> methodDescription = new MethodDescription<>(clazz, name, parameterTypes);
		if (!methodDescription.getReturnType().equals(returnType)) {
			throw new IllegalArgumentException("Wrong return type given: declared "+returnType.getName() + " but is " + methodDescription.getReturnType().getName());
		}
		return methodDescription;
	}
	
	
	protected MetaMember() {
	}
	
	public MetaMember(D description) {
		super(description);
	}
	
	public MetaMember(D description, O owner) {
		
	}
}
