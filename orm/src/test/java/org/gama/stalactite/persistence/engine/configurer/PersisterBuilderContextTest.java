package org.gama.stalactite.persistence.engine.configurer;

import org.codefilarete.tool.trace.ModifiableInt;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
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
		PersisterBuilderContext testInstance = new PersisterBuilderContext();

		EntityMappingConfiguration entityMappingConfigurationMock = mock(EntityMappingConfiguration.class);
		when(entityMappingConfigurationMock.getEntityType()).thenReturn(Integer.class);
		ModifiableInt invokationSafeGuard = new ModifiableInt();
		testInstance.runInContext(entityMappingConfigurationMock, () -> {
			invokationSafeGuard.increment();
			assertThat(testInstance.isCycling(entityMappingConfigurationMock)).isTrue();
			// testing by simulating recursive calls as it happens in production : we add a dummy EntityMappingConfiguration to the stack
			EntityMappingConfiguration dummyConfiguration = mock(EntityMappingConfiguration.class);
			when(dummyConfiguration.getEntityType()).thenReturn(String.class);
			testInstance.runInContext(dummyConfiguration, () -> {
				invokationSafeGuard.increment();
				assertThat(testInstance.isCycling(entityMappingConfigurationMock)).isTrue();
				// even deeper
				testInstance.runInContext(dummyConfiguration, () -> {
					invokationSafeGuard.increment();
					assertThat(testInstance.isCycling(entityMappingConfigurationMock)).isTrue();
					assertThat(testInstance.isCycling(mock(EntityMappingConfiguration.class))).isFalse();
				});
			});
			// testing sibling
			testInstance.runInContext(dummyConfiguration, () -> {
				invokationSafeGuard.increment();
				assertThat(testInstance.isCycling(entityMappingConfigurationMock)).isTrue();
				assertThat(testInstance.isCycling(mock(EntityMappingConfiguration.class))).isFalse();
			});
		});
		assertThat(4).isEqualTo(invokationSafeGuard.getValue());
	}
	
}