package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;

import org.gama.lang.Reflections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class LambdaMethodCapturerTest {
	
	public static class Target {
		
		public String sayHello() {
			return "";
		}
		
		public void setHello(String message) {
		}
		
		public boolean isGood() {
			return false;
		}
		
		public void setGood(boolean b) {
		}
	}
	
	@Test
	public void testCapture_getter() {
		LambdaMethodCapturer<Target> testInstance = new LambdaMethodCapturer<>(Target.class);
		Method result = testInstance.capture(Target::sayHello);
		assertEquals(Reflections.findMethod(Target.class, "sayHello"), result);
		
		// primitive type test
		result = testInstance.capture(Target::isGood);
		assertEquals(Reflections.findMethod(Target.class, "isGood"), result);
	}
	
	@Test
	public void testCapture_setter() {
		LambdaMethodCapturer<Target> testInstance = new LambdaMethodCapturer<>(Target.class);
		Method result = testInstance.capture(Target::setHello);
		assertEquals(Reflections.findMethod(Target.class, "setHello", String.class), result);
		
		// primitive type test
		result = testInstance.capture(Target::setGood);
		assertEquals(Reflections.findMethod(Target.class, "setGood", boolean.class), result);
	}
	
}