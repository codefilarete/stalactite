package org.gama.stalactite.persistence.engine;

import org.gama.lang.trace.IncrementableInt;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class DecoratorTest {
	
	/** Basic use case, other use cases are not supported (too many combination error prone) */
	@Test
	public void testDecorate_methodsOfTargetAreCalled_methodsOnTargetAreInvoked() {
		Decorator<ExtensionInterface> testInstance = new Decorator<>(ExtensionInterface.class);
		IncrementableInt targetCallCount = new IncrementableInt();
		IncrementableInt interceptorCallCount = new IncrementableInt();
		ExtensionInterface interceptorSurrogate = new ExtensionInterface() {
			/** Overriden to check that methods interceptor are called */
			@Override
			public void sayBonjour() {
				interceptorCallCount.increment();
			}
		};
		DecorationInterface interceptor = testInstance.decorate(new IToBeDecorated() {
			/** Overriden to check that methods interceptor are called */
			@Override
			public void sayHello() {
				targetCallCount.increment();
			}
			
			@Override
			public void sayBuenosDias() {
				targetCallCount.increment();
			}
		}, DecorationInterface.class, interceptorSurrogate);
		
		// run tested code
		interceptor.sayHello();
		// check assertion
		assertEquals(1, targetCallCount.getValue());
		assertEquals(0, interceptorCallCount.getValue());
		
		// run tested code
		interceptor.sayBuenosDias();
		// check assertion
		assertEquals(2, targetCallCount.getValue());
		assertEquals(0, interceptorCallCount.getValue());
		
		// run tested code
		interceptor.sayBonjour();
		// check assertion
		assertEquals(1, interceptorCallCount.getValue());
	}
	
	@Test
	public void testDecorate_withInheritanceOnInterceptingInterface() {
		Decorator<ExtensionExtensionInterface> testInstance = new Decorator<>(ExtensionExtensionInterface.class);
		IncrementableInt targetCallCount = new IncrementableInt();
		IncrementableInt interceptorCallCount = new IncrementableInt();
		ExtensionExtensionInterface interceptorSurrogate = new ExtensionExtensionInterface() {
			/** Overriden to check that methods interceptor are called */
			@Override
			public void sayBonjour() {
				interceptorCallCount.increment();
			}
			
			@Override
			public void sayGuttenTag() {
				interceptorCallCount.increment();
			}
		};
		BigDecorationInterface interceptor = testInstance.decorate(new IToBeDecorated() {
			/** Overriden to check that methods interceptor are called */
			@Override
			public void sayHello() {
				targetCallCount.increment();
			}
			
			@Override
			public void sayBuenosDias() {
				targetCallCount.increment();
			}
		}, BigDecorationInterface.class, interceptorSurrogate);
		
		// run tested code
		interceptor.sayHello();
		// check assertion
		assertEquals(1, targetCallCount.getValue());
		assertEquals(0, interceptorCallCount.getValue());
		
		// run tested code
		interceptor.sayBuenosDias();
		// check assertion
		assertEquals(2, targetCallCount.getValue());
		assertEquals(0, interceptorCallCount.getValue());
		
		// run tested code
		interceptor.sayBonjour();
		// check assertion
		assertEquals(1, interceptorCallCount.getValue());
		
		// run tested code
		interceptor.sayGuttenTag();
		// check assertion
		assertEquals(2, interceptorCallCount.getValue());
	}
	
	private interface IToBeDecorated {
		
		void sayHello();
		
		void sayBuenosDias();
		
	}
	
	private interface ExtensionInterface {
		
		void sayBonjour();
		
	}
	
	private interface ExtensionExtensionInterface extends ExtensionInterface {
		
		void sayGuttenTag();
		
	}
	
	interface DecorationInterface extends IToBeDecorated, ExtensionInterface {
		
	}
	
	interface BigDecorationInterface extends IToBeDecorated, ExtensionExtensionInterface {
		
	}
	
}