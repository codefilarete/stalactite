package org.codefilarete.stalactite.engine.configurer;

import java.lang.reflect.Field;
import java.util.stream.Stream;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByField;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.MutatorByField;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.configurer.PropertyAccessorResolver.PropertyMapping;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.Reflections.MemberNotFoundException;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PropertyAccessorResolverTest {
	
	public static Stream<Arguments> resolve() {
		return Stream.of(
				// given getter is a method reference : getter is kept as it is and mutator is setter method
				arguments(
						new PropertyMappingSupport<>(Country::getName, null, null),
						new AccessorByMethodReference<>(Country::getName),
						new MutatorByMethod<>(Reflections.findMethod(Country.class, "setName", String.class))
				),
				// given setter is a method reference : setter is kept as it is and accessor is getter method
				arguments(
						new PropertyMappingSupport<>(null, Country::setName, null),
						new AccessorByMethod<>(Reflections.findMethod(Country.class, "getName")),
						new MutatorByMethodReference<>(Country::setName)
				),
				// given getter is a method reference and field is given : getter is kept as it is and mutator is field one
				arguments(
						new PropertyMappingSupport<>(Country::getName, null, Reflections.findField(Country.class, "name")),
						new AccessorByMethodReference<>(Country::getName),
						new MutatorByField<>(Reflections.findField(Country.class, "name"))
				),
				// given setter is a method reference : setter is kept as it is and accessor is getter method
				arguments(
						new PropertyMappingSupport<>(null, Country::setName, Reflections.findField(Country.class, "name")),
						new AccessorByField<>(Reflections.findField(Country.class, "name")),
						new MutatorByMethodReference<>(Country::setName)
				),
				// given getter but property doesn't meet Java Bean Naming Convention
				arguments(
						new PropertyMappingSupport<>(Country::hasNuclearPower, null, Reflections.findField(Country.class, "hasNuclearPower")),
						new AccessorByMethodReference<>(Country::hasNuclearPower),
						new MutatorByField<>(Reflections.findField(Country.class, "hasNuclearPower"))
				),
				// given setter but property doesn't meet Java Bean Naming Convention
				arguments(
						new PropertyMappingSupport<>(null, Country::nuclearPower, Reflections.findField(Country.class, "hasNuclearPower")),
						new AccessorByField<>(Reflections.findField(Country.class, "hasNuclearPower")),
						new MutatorByMethodReference<>(Country::nuclearPower)
				),
				// readonly property
				arguments(
						new PropertyMappingSupport<>(Country::hasNuclearPower, null, Reflections.findField(Country.class, "hasNuclearPower")),
						new AccessorByMethodReference<>(Country::hasNuclearPower),
						new MutatorByField<>(Reflections.findField(Country.class, "hasNuclearPower"))
				)
		);
	}
	
	@ParameterizedTest
	@MethodSource
	<C, O> void resolve(PropertyMapping<C, O> source, Accessor<C, O> expectedAccessor, Mutator<C, O> expectedMutator) {
		PropertyAccessorResolver<C, O> testInstance = new PropertyAccessorResolver<>(source);
		
		PropertyAccessor<C, O> actual = (PropertyAccessor<C, O>) testInstance.resolve();
		assertThat(actual.getAccessor()).isEqualTo(expectedAccessor);
		assertThat(actual.getMutator()).isEqualTo(expectedMutator);
	}
	
	@Test
	void resolve_propertyDoesntMeetJavaBeanNamingConvention_exceptionIsThrown() {
		PropertyAccessorResolver<Country, Boolean> testInstance;
		
		testInstance = new PropertyAccessorResolver<>(new PropertyMappingSupport<>(Country::hasNuclearPower, null, null));
		assertThatCode(testInstance::resolve)
				.isInstanceOf(MappingConfigurationException.class)
				.hasMessage("Can't find a property matching getter name hasNuclearPower, setter can't be deduced, provide a field name to fix it")
				.hasCauseInstanceOf(MemberNotFoundException.class)
				.hasRootCauseMessage("Field wrapper hasNuclearPower doesn't fit encapsulation naming convention");
		
		testInstance = new PropertyAccessorResolver<>(new PropertyMappingSupport<>(null, Country::nuclearPower, null));
		assertThatCode(testInstance::resolve)
				.isInstanceOf(MappingConfigurationException.class)
				.hasMessage("Can't find a property matching setter name nuclearPower, getter can't be deduced, provide a field name to fix it")
				.hasCauseInstanceOf(MemberNotFoundException.class)
				.hasRootCauseMessage("Field wrapper nuclearPower doesn't fit encapsulation naming convention");
	}
	
	private static class PropertyMappingSupport<C, O> implements PropertyMapping<C, O> {
		
		private final SerializableFunction<C, O> getter;
		private final SerializableBiConsumer<C, O> setter;
		private final Field field;
		
		PropertyMappingSupport(SerializableFunction<C, O> getter, SerializableBiConsumer<C, O> setter, Field field) {
			this.getter = getter;
			this.setter = setter;
			this.field = field;
		}
		
		@Override
		public SerializableFunction<C, O> getGetter() {
			return getter;
		}
		
		@Override
		public SerializableBiConsumer<C, O> getSetter() {
			return setter;
		}
		
		@Override
		public Field getField() {
			return field;
		}
	}
	
}