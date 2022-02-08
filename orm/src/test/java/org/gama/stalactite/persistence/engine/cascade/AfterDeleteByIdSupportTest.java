package org.gama.stalactite.persistence.engine.cascade;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class AfterDeleteByIdSupportTest {
	
	@Test
	void constructorArgsAreInvoked() {
		Consumer actionMock = mock(Consumer.class);
		Function targetProviderMock = mock(Function.class);
		when(targetProviderMock.apply(any())).thenAnswer((Answer<String>) invocation -> ((String) invocation.getArgument(0)).toUpperCase());
		Predicate targetFilterMock = mock(Predicate.class);
		when(targetFilterMock.test(any())).thenAnswer(invocation -> !"B".equals(invocation.getArgument(0)));
		AfterDeleteByIdSupport testInstance = new AfterDeleteByIdSupport(actionMock, targetProviderMock, targetFilterMock);
		testInstance.afterDeleteById(Arrays.asList("a", "b", "c"));
		verify(actionMock).accept(eq(Arrays.asList("A", "C")));
		verify(targetProviderMock, times(3)).apply(any());
		verify(targetFilterMock, times(3)).test(any());
	}
	
	@Test
	void constructorWithDefaultFilter_acceptAll() {
		Consumer actionMock = mock(Consumer.class);
		Function targetProviderMock = mock(Function.class);
		when(targetProviderMock.apply(any())).thenAnswer((Answer<String>) invocation -> ((String) invocation.getArgument(0)).toUpperCase());
		AfterDeleteByIdSupport testInstance = new AfterDeleteByIdSupport(actionMock, targetProviderMock);
		testInstance.afterDeleteById(Arrays.asList("a", "b", "c"));
		verify(actionMock).accept(eq(Arrays.asList("A", "B", "C")));
		verify(targetProviderMock, times(3)).apply(any());
	}
}