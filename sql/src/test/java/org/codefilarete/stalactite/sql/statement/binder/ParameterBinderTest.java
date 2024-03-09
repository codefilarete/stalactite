package org.codefilarete.stalactite.sql.statement.binder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.ElementType;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.tool.io.IOs;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.sql.statement.binder.ParameterBinderTest.OnOff.Off;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ParameterBinderTest {
	
	@Test
	void wrap_convertASetToAString() throws SQLException {
		ParameterBinder<Set<ElementType>> testInstance = DefaultParameterBinders.STRING_BINDER.wrap(setAsString -> {
					String[] databaseValue = setAsString.substring(1, setAsString.length() - 1).split(", ");
					return EnumSet.copyOf(Stream.of(databaseValue).map(ElementType::valueOf).collect(Collectors.toSet()));
				},
				Set::toString);
		
		// write test
		PreparedStatement psMock = mock(PreparedStatement.class);
		
		// when
		testInstance.set(psMock, 42, EnumSet.of(ElementType.PACKAGE, ElementType.TYPE));
		// then
		verify(psMock).setString(42, "[TYPE, PACKAGE]");
		
		// read test
		ResultSet rsMock = mock(ResultSet.class);
		when(rsMock.getString(any())).thenReturn("[TYPE, PACKAGE]");
		
		// when
		Set<ElementType> actual = testInstance.get(rsMock, any());
		// then
		assertThat(actual).containsExactlyInAnyOrder(ElementType.PACKAGE, ElementType.TYPE)
				.isInstanceOf(EnumSet.class);
	}
	
	@Test
	void wrap_convertAStringToABinaryStream() throws SQLException {
		ParameterBinder<String> testInstance = DefaultParameterBinders.BINARYSTREAM_BINDER.wrap(binary -> {
					try {
						byte[] byteArray = IOs.toByteArray(binary);
						return new String(byteArray);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				},
				s -> new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
		
		// write test
		PreparedStatement psMock = mock(PreparedStatement.class);
		ArgumentCaptor<InputStream> psCaptor = ArgumentCaptor.forClass(InputStream.class);
		doNothing().when(psMock).setBinaryStream(anyInt(), psCaptor.capture());
		
		// when
		testInstance.set(psMock, 42, "Hello world !");
		// then
		assertThat(psCaptor.getValue()).hasBinaryContent("Hello world !".getBytes(StandardCharsets.UTF_8));
		
		// read test
		ResultSet rsMock = mock(ResultSet.class);
		when(rsMock.getBinaryStream(any())).thenReturn(new ByteArrayInputStream("Hello world !".getBytes(StandardCharsets.UTF_8)));
		
		// when
		String actual = testInstance.get(rsMock, any());
		// then
		assertThat(actual).isEqualTo("Hello world !");
	}
	
	@Test
	void wrap_convertAEnumToBoolean() throws SQLException {
		ParameterBinder<OnOff> testInstance = new LambdaParameterBinder<>(DefaultParameterBinders.BOOLEAN_BINDER,
				(Boolean b) -> b == Boolean.TRUE ? OnOff.On : Off,
				e -> e == OnOff.On);
		
		// write test
		PreparedStatement psMock = mock(PreparedStatement.class);
		
		// when
		testInstance.set(psMock, 42, OnOff.On);
		// then
		verify(psMock).setBoolean(42, true);
		
		// read test
		ResultSet rsMock = mock(ResultSet.class);
		when(rsMock.getBoolean(any())).thenReturn(false);
		
		// when
		OnOff actual = testInstance.get(rsMock, any());
		// then
		assertThat(actual).isEqualTo(Off);
	}
	
	enum OnOff {
		On, Off
	}
}