package org.codefilarete.stalactite.query.api;

import org.codefilarete.stalactite.query.model.Placeholder;
import org.codefilarete.stalactite.query.model.ValuedVariable;

/**
 * Value wrapper of an operator.
 * Made to have concrete values (String, Integer, etc.) and parameterized ones.
 * 
 * @param <V>
 * @see ValuedVariable
 * @see Placeholder
 * @author Guillaume Mary
 */
public interface Variable<V> {
}
