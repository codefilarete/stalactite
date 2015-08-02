package org.gama.safemodel.description;

import org.gama.lang.Reflections;

import java.lang.reflect.Field;

/**
 * @author Guillaume Mary
 */
public class FieldDescription extends AbstracNamedMemberDescription {
	
	public final Field field;
	
	public FieldDescription(Class declaringClass, String name) {
		super(declaringClass, name);
		this.field = Reflections.getField(declaringClass, name);
	}
}
