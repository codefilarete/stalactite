package org.codefilarete.stalactite.engine.configurer.builder;

import org.codefilarete.reflection.AccessorByField;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IdentificationStepTest {
	
	@Test
	void assertCompositeKeyIdentifierOverridesEqualsHashcode() throws NoSuchFieldException {
		class DummyCompositeKeyIdentifier {
		
		}
		
		class DummyCompositeKeyIdentifierWithEqualsAndHashCode {
			
			@Override
			public boolean equals(Object obj) {
				// any kind of implementation is sufficient
				return super.equals(obj);
			}
			
			@Override
			public int hashCode() {
				// any kind of implementation is sufficient
				return super.hashCode();
			}
		}
		
		class DummyEntity {
			
			private DummyCompositeKeyIdentifier dummyCompositeKeyIdentifier;
			private DummyCompositeKeyIdentifierWithEqualsAndHashCode dummyCompositeKeyIdentifierWithEqualsAndHashCode;
		}
		
		EntityMappingConfiguration.CompositeKeyMapping<?, ?> compositeKeyMappingMock = mock(EntityMappingConfiguration.CompositeKeyMapping.class, RETURNS_MOCKS);
		when(compositeKeyMappingMock.getAccessor()).thenReturn(new AccessorByField<>(DummyEntity.class.getDeclaredField("dummyCompositeKeyIdentifier")));
		assertThatCode(() -> IdentificationStep.assertCompositeKeyIdentifierOverridesEqualsHashcode(compositeKeyMappingMock))
				.hasMessage("Composite key identifier class o.c.s.e.c.b.IdentificationStepTest$DummyCompositeKeyIdentifier" +
						" seems to have default implementation of equals() and hashcode() methods," +
						" which is not supported (identifiers must be distinguishable), please make it implement them");
		
		when(compositeKeyMappingMock.getAccessor()).thenReturn(new AccessorByField<>(DummyEntity.class.getDeclaredField("dummyCompositeKeyIdentifierWithEqualsAndHashCode")));
		assertThatCode(() -> IdentificationStep.assertCompositeKeyIdentifierOverridesEqualsHashcode(compositeKeyMappingMock))
				.doesNotThrowAnyException();
	}
	
}