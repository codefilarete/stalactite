package org.stalactite.lang.bean;

/**
 * @author mary
 */
public interface IFactory<I, O> {

	O createInstance(I input);
}

