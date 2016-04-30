package org.gama.safemodel.description;

import java.lang.reflect.Field;

import org.gama.lang.Reflections;

/**
 * @author Guillaume Mary
 */
public class FieldDescription<R> extends AbstracNamedMemberDescription<Field> {
	
	/**
	 * Build a {@link FieldDescription} from the description of a field: name, owning class and type 
	 * 
	 * @throws IllegalArgumentException if the given field type is not the real one
	 * @throws Reflections.MemberNotFoundException if the field is not found in the hierarchy of the owning class
	 * @see FieldDescription#FieldDescription(Class, String)
	 */
	public static <R> FieldDescription<R> field(Class declaringClass, String name, Class<R> fieldType) {
		FieldDescription<R> fieldDescription = new FieldDescription<>(declaringClass, name);
		if (!fieldDescription.getFieldType().equals(fieldType)) {
			throw new IllegalArgumentException("Wrong field type given: declared "+fieldType.getName() + " but is " + fieldDescription.getFieldType().getName());
		}
		return fieldDescription;
	}
	
	public final Field field;
	
	/**
	 * @throws Reflections.MemberNotFoundException if the field is not found in the hierarchy of the owning class
	 * @see Reflections#findField(Class, String)
	 */
	public FieldDescription(Class declaringClass, String name) {
		this(Reflections.getField(declaringClass, name));
	}
	
	public FieldDescription(Field field) {
		super(field);
		this.field = field;
	}
	
	public Class<R> getFieldType() {
		return (Class<R>) field.getType();
	}
	
	public Field getField() {
		return field;
	}
}
