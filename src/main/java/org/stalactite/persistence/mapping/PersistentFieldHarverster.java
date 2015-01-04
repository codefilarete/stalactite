package org.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.util.List;

import javax.annotation.Nonnull;

import org.stalactite.lang.Reflections.FieldIterator;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.Filter;

/**
 * @author mary
 */
public class PersistentFieldHarverster {
	
	public List<Field> getFields(Class clazz) {
		FieldFilter fieldVisitor = getFieldVisitor();
		return Iterables.filter(new FieldIterator(clazz), fieldVisitor);
	}
	
	/**
	 * Méthode à surcharger/modifier pour prendre en compte les Map par exemple
	 * @return
	 */
	@Nonnull
	protected FieldFilter getFieldVisitor() {
		return new FieldFilter();
	}
	
	public static class FieldFilter extends Filter<Field> {
		@Override
		public boolean accept(Field field) {
			return true;
		}
	}
}
