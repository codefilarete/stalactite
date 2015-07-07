package org.gama.lang.bean;

/**
 * @author Guillaume Mary
 */
public interface IDelegateWithReturnAndThrows<E> {
	
	E execute() throws Throwable;
}
