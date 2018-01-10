package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;

import org.gama.lang.Reflections;
import org.gama.reflection.AccessorByMember;
import org.gama.reflection.PropertyAccessor;

/**
 * @author Guillaume Mary
 */
public interface JoinColumnNamingStrategy {
	
	String DEFAULT_JOIN_COLUMN_SUFFIX = "Id";
	
	/**
	 * Expected to generate a name for the column that will join with the identifier of another one.
	 * @param accessor a {@link PropertyAccessor} made of member (method or field), not method reference
	 */
	String giveName(PropertyAccessor accessor);
	
	JoinColumnNamingStrategy DEFAULT = (accessor) -> {
		// At this point only AccessorByMember can be decrypted
		Member getter = ((AccessorByMember) accessor.getAccessor()).getGetter();
		String baseColumnName;
		if (getter instanceof Field) {
			baseColumnName = getter.getName();
		} else {
			baseColumnName = Reflections.JAVA_BEAN_ACCESSOR_PREFIX_REMOVER.apply((Method) getter);
		}
		return baseColumnName + DEFAULT_JOIN_COLUMN_SUFFIX;
	};
	
}
