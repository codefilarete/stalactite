package org.gama.lang.bean;

/**
 * @author mary
 */
public interface IFactory<I, O> {

	O createInstance(I input);
}

