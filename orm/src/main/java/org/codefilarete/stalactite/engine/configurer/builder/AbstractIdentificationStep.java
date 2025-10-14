package org.codefilarete.stalactite.engine.configurer.builder;

import java.lang.reflect.Method;

import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.key.FluentEntityMappingBuilderKeyOptions;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;

public class AbstractIdentificationStep<C, I> {
	
	private final MethodReferenceCapturer methodSpy;
	
	public AbstractIdentificationStep() {
		this.methodSpy = new MethodReferenceCapturer();
	}
	
	protected UnsupportedOperationException newMissingIdentificationException(Class<C> entityType) {
		SerializableTriFunction<FluentEntityMappingBuilder, SerializableBiConsumer<C, I>, IdentifierPolicy, FluentEntityMappingBuilderKeyOptions<C, I>>
				identifierMethodReference = FluentEntityMappingBuilder::mapKey;
		Method identifierSetter = this.methodSpy.findMethod(identifierMethodReference);
		return new UnsupportedOperationException("Identifier is not defined for " + Reflections.toString(entityType)
				+ ", please add one through " + Reflections.toString(identifierSetter));
	}
}
