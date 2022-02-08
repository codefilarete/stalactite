package org.gama.stalactite.persistence.engine.cascade;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class BeforeUpdateSupportTest {
	
	@Test
	void constructorArgsAreInvoked() {
		BiConsumer actionMock = mock(BiConsumer.class);
		Function targetProviderMock = mock(Function.class);
		when(targetProviderMock.apply(any())).thenAnswer(invocation -> ((String) invocation.getArgument(0)).toUpperCase());
		Predicate targetFilterMock = mock(Predicate.class);
		when(targetFilterMock.test(any())).thenAnswer(invocation -> !new Duo<>("B BIS", "B").equals(invocation.getArgument(0)));
		BeforeUpdateSupport testInstance = new BeforeUpdateSupport(actionMock, targetProviderMock, targetFilterMock);
		testInstance.beforeUpdate(Arrays.asList(
				new Duo<>("a bis", "a"),
				new Duo<>("b bis", "b"),
				new Duo<>("c bis", "c")),
				true);
		verify(actionMock).accept(eq(Arrays.asList(new Duo<>("A BIS", "A"), new Duo<>("C BIS", "C"))), eq(true));
		verify(targetProviderMock, times(6)).apply(any());
		verify(targetFilterMock, times(3)).test(any());
	}
	
	@Test
	void constructorWithDefaultFilter_acceptAll() {
		BiConsumer actionMock = mock(BiConsumer.class);
		Function targetProviderMock = mock(Function.class);
		when(targetProviderMock.apply(any())).thenAnswer(invocation -> ((String) invocation.getArgument(0)).toUpperCase());
		BeforeUpdateSupport testInstance = new BeforeUpdateSupport(actionMock, targetProviderMock);
		testInstance.beforeUpdate(Arrays.asList(
				new Duo<>("a bis", "a"),
				new Duo<>("b bis", "b"),
				new Duo<>("c bis", "c")),
				true);
		verify(actionMock).accept(eq(Arrays.asList(new Duo<>("A BIS", "A"), new Duo<>("B BIS", "B"), new Duo<>("C BIS", "C"))), eq(true));
		verify(targetProviderMock, times(6)).apply(any());
	}
}