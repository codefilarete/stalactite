package org.gama.lang.bean;

/**
 * @author Guillaume Mary
 */
public interface IDelegate<R, T extends Throwable> {
	
	R execute() throws T;
}
