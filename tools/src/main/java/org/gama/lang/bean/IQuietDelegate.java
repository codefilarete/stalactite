package org.gama.lang.bean;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface IQuietDelegate<R> extends IDelegate<R, RuntimeException> {
}
