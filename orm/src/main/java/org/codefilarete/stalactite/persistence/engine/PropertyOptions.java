package org.codefilarete.stalactite.persistence.engine;

import java.util.function.Function;

/**
 * Options on a basic property
 * 
 * @author Guillaume Mary
 */
public interface PropertyOptions {
	
	/** Marks the property as mandatory. Note that using this method on an identifier one as no purpose because identifiers are already madatory. */
	PropertyOptions mandatory();
	
	/**
	 * Marks this property as set by constructor, meaning it won't be set by any associated setter (method or field access).
	 * Should be used in conjonction with {@link org.codefilarete.stalactite.persistence.engine.FluentEntityMappingBuilder.KeyOptions#usingConstructor(Function)}
	 * and other equivalent methods.
	 */
	PropertyOptions setByConstructor();
	
}
