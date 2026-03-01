package org.codefilarete.stalactite.engine.configurer;

import java.lang.reflect.Method;

import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.SerializableAccessor;
import org.codefilarete.reflection.SerializableMutator;
import org.codefilarete.stalactite.engine.configurer.embeddable.Inset;

/**
 * Small internal (package private) contract to help mutualizing usage of a {@link MethodReferenceCapturer}.
 * Created to allow {@link Inset} to be static and then being
 * overridable from outside.
 * 
 * @author Guillaume Mary
 */
public interface LambdaMethodUnsheller {
	
	Method captureLambdaMethod(SerializableAccessor getter);
	
	Method captureLambdaMethod(SerializableMutator setter);
}
