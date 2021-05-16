package org.gama.stalactite.persistence.engine.configurer;

import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class PersisterBuilderContextTest {
	
	@Test
	void cycling() {
		PersisterBuilderContext testInstance = new PersisterBuilderContext();

		EntityMappingConfiguration entityMappingConfigurationMock = mock(EntityMappingConfiguration.class);
		when(entityMappingConfigurationMock.getEntityType()).thenReturn(Integer.class);
		ModifiableInt invokationSafeGuard = new ModifiableInt();
		testInstance.runInContext(entityMappingConfigurationMock, () -> {
			invokationSafeGuard.increment();
			assertTrue(testInstance.isCycling(entityMappingConfigurationMock));
			// testing by simulating recursive calls as it happens in production : we add a dummy EntityMappingConfiguration to the stack
			EntityMappingConfiguration dummyConfiguration = mock(EntityMappingConfiguration.class);
			when(dummyConfiguration.getEntityType()).thenReturn(String.class);
			testInstance.runInContext(dummyConfiguration, () -> {
				invokationSafeGuard.increment();
				assertTrue(testInstance.isCycling(entityMappingConfigurationMock));
				// even deeper
				testInstance.runInContext(dummyConfiguration, () -> {
					invokationSafeGuard.increment();
					assertTrue(testInstance.isCycling(entityMappingConfigurationMock));
					assertFalse(testInstance.isCycling(mock(EntityMappingConfiguration.class)));
				});
			});
			// testing sibling
			testInstance.runInContext(dummyConfiguration, () -> {
				invokationSafeGuard.increment();
				assertTrue(testInstance.isCycling(entityMappingConfigurationMock));
				assertFalse(testInstance.isCycling(mock(EntityMappingConfiguration.class)));
			});
		});
		assertEquals(invokationSafeGuard.getValue(), 4);
	}
	
}