package org.stalactite.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class ExceptionConverter<C> {
	
	protected void convertException(Throwable t, C c, AbstractReflector reflector, Object... args) {
		if (t instanceof NullPointerException) {
			throw new NullPointerException("Cannot call " + getReflectorDescription(reflector) + " on null instance");
		} else if (t instanceof InvocationTargetException || t instanceof IllegalAccessException) {
			Exceptions.throwAsRuntimeException(t.getCause());
		} else if (t instanceof IllegalArgumentException) {
			if ("wrong number of arguments".equals(t.getMessage())) {
				convertWrongNumberOfArguments(reflector, args);
			} else if("object is not an instance of declaring class".equals(t.getMessage())) {
				convertObjectIsNotAnInstanceOfDeclaringClass(c, reflector);
			} else if(reflector instanceof AccessorByMember
					&& !((AccessorByMember) reflector).getGetter().getDeclaringClass().isAssignableFrom(c.getClass())) {
				convertCannotSetFieldToObject(c, (AccessorByMember) reflector);
			} else {
				Exceptions.throwAsRuntimeException(t);
			}
		} else {
			Exceptions.throwAsRuntimeException(t);
		}
	}
	
	private void convertWrongNumberOfArguments(AbstractReflector reflector, Object... args) {
		String message = "wrong number of arguments for " + getReflectorDescription(reflector);
		if (reflector instanceof AccessorByMethod) {
			Class<?>[] parameterTypes = ((AccessorByMethod) reflector).getGetter().getParameterTypes();
			StringAppender parameterFormatter = new StringAppender(100);
			parameterFormatter.ccat(parameterTypes, ", ");
			message += ": expected " + parameterFormatter.toString()
					+ " but " + (Arrays.isEmpty(args) ? "none" : new StringAppender(100).ccat(args, ", ").wrap("(", ")"))
					+ " was given";
		}
		throw new IllegalArgumentException(message);
	}
	
	private void convertObjectIsNotAnInstanceOfDeclaringClass(C c, AbstractReflector reflector) {
		String message = "object is not an instance of declaring class";
		if (reflector instanceof AccessorByMember) {
			Class<?> declaringClass = ((AccessorByMember) reflector).getGetter().getDeclaringClass();
			message += ": expected " + declaringClass.getName() + " but was " + c.getClass().getName();
		}
		throw new IllegalArgumentException(message);
	}
	
	private void convertCannotSetFieldToObject(C c, AccessorByMember reflector) {
		// Modification du message par défaut qui n'est pas très clair "Can not set ... to ... "
		Field getter = (Field) reflector.getGetter();
		throw new IllegalArgumentException(c.getClass() + " doesn't have field " + getter.getName());
	}
	
	private String getReflectorDescription(AbstractReflector reflector) {
		if (reflector instanceof AbstractAccessor) {
			return ((AbstractAccessor) reflector).getGetterDescription();
		}
		if (reflector instanceof AbstractMutator) {
			return ((AbstractMutator) reflector).getSetterDescription();
		}
		throw new IllegalArgumentException("Unknown reflector " + reflector);
	}
	
}
