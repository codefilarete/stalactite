package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Small internal (package private) contract to help mutualizing usage of a {@link org.gama.reflection.MethodReferenceCapturer}.
 * Created to allow {@link org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.Inset} to be static and then being
 * overridable from outside.
 * 
 * @author Guillaume Mary
 */
public interface LambdaMethodUnsheller {
	
	Method captureLambdaMethod(SerializableFunction getter);
	
	Method captureLambdaMethod(SerializableBiConsumer setter);
}
