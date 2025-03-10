package org.codefilarete.stalactite.engine.configurer;

import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.tool.trace.MutableInt;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class PersisterBuilderContextTest {
	
	@Test
	void cycling() {
		PersisterBuilderContext testInstance = new PersisterBuilderContext(mock(PersisterRegistry.class));

		EntityMappingConfiguration entityMappingConfigurationMock = mock(EntityMappingConfiguration.class);
		when(entityMappingConfigurationMock.getEntityType()).thenReturn(Integer.class);
		MutableInt invocationSafeGuard = new MutableInt();
		testInstance.runInContext(entityMappingConfigurationMock, () -> {
			invocationSafeGuard.increment();
			assertThat(testInstance.isCycling(entityMappingConfigurationMock)).isTrue();
			// testing by simulating recursive calls as it happens in production : we add a dummy EntityMappingConfiguration to the stack
			EntityMappingConfiguration dummyConfiguration = mock(EntityMappingConfiguration.class);
			when(dummyConfiguration.getEntityType()).thenReturn(String.class);
			testInstance.runInContext(dummyConfiguration, () -> {
				invocationSafeGuard.increment();
				assertThat(testInstance.isCycling(entityMappingConfigurationMock)).isTrue();
				// even deeper
				testInstance.runInContext(dummyConfiguration, () -> {
					invocationSafeGuard.increment();
					assertThat(testInstance.isCycling(entityMappingConfigurationMock)).isTrue();
					assertThat(testInstance.isCycling(mock(EntityMappingConfiguration.class))).isFalse();
				});
			});
			// testing sibling
			testInstance.runInContext(dummyConfiguration, () -> {
				invocationSafeGuard.increment();
				assertThat(testInstance.isCycling(entityMappingConfigurationMock)).isTrue();
				assertThat(testInstance.isCycling(mock(EntityMappingConfiguration.class))).isFalse();
			});
		});
		assertThat(4).isEqualTo(invocationSafeGuard.getValue());
	}
	
}