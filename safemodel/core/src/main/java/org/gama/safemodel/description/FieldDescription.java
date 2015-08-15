package org.gama.safemodel.description;

import org.gama.lang.Reflections;

import java.lang.reflect.Field;

/**
 * @author Guillaume Mary
 */
public class FieldDescription<R> extends AbstracNamedMemberDescription<Field> {
	
	public final Field field;
	
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
}
