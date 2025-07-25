package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.SerializableThrowingBiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * @author Guillaume Mary
 */
class ResultSetReaderTest {
	
	@Test
	void get_exceptionHandling() throws SQLException {
		ResultSetReader<Integer> resultSetReader = new ResultSetReader<Integer>() {
			@Override
			public Class<Integer> getType() {
				return Integer.class;
			}
			
			@Override
			public Integer doGet(ResultSet resultSet, String columnName) {
				return (Integer) new Holder("A").get();
			}
		};
		ResultSet resultSetMock = Mockito.mock(ResultSet.class);
		Mockito.when(resultSetMock.getObject("XX")).thenReturn("my too long String value");
		Assertions.assertThatThrownBy(() -> resultSetReader.get(resultSetMock, "XX"))
				.hasMessage("Error while reading column 'XX' : trying to read 'my too long Str...' as java.lang.Integer but was java.lang.String");
	}
	
	@Test
	void thenApply_typeIsConverterOne() {
		ResultSetReader<Long> resultSetReader = new ResultSetReader<Long>() {
			@Override
			public Class<Long> getType() {
				return long.class;
			}
			
			@Override
			public Long doGet(ResultSet resultSet, String columnName) {
				return 1L;
			}
		};
		assertThat(resultSetReader.getType()).isEqualTo(long.class);
		
		// testing with a static converter
		ResultSetReader<Date> testInstance1 = resultSetReader.thenApply(Date::new);
		assertThat(testInstance1.getType()).isEqualTo(Date.class);
		
		// testing with an instance method reference
		ResultSetReader<Date> testInstance2 = resultSetReader.thenApply(ResultSetReaderTest::getTimeAsStatic);
		assertThat(testInstance2.getType()).isEqualTo(Date.class);
		
		// testing with an instance converter
		ResultSetReader<Date> testInstance3 = resultSetReader.thenApply(this::getTimeAsNonStatic);
		assertThat(testInstance3.getType()).isEqualTo(Date.class);
		
		// testing with an instance converter
		DecimalFormat df = new DecimalFormat("");
		ResultSetReader<String> testInstance4 = resultSetReader.thenApply(df::format);
		assertThat(testInstance4.getType()).isEqualTo(String.class);
	}
	
	static Stream<Arguments> ofMethodReference() {
		return Stream.of(
				arguments((SerializableThrowingBiFunction<ResultSet, String, ?, SQLException>) ResultSet::getLong, long.class),
				arguments((SerializableThrowingBiFunction<ResultSet, String, ?, SQLException>) ResultSet::getDate, java.sql.Date.class),
				arguments((SerializableThrowingBiFunction<ResultSet, String, ?, SQLException>) ResultSet::getDouble, double.class),
				arguments((SerializableThrowingBiFunction<ResultSet, String, ?, SQLException>) ResultSet::getFloat, float.class),
				arguments((SerializableThrowingBiFunction<ResultSet, String, ?, SQLException>) ResultSet::getString, String.class)
		);
	}
	
	@ParameterizedTest
	@MethodSource
	<O> void ofMethodReference(SerializableThrowingBiFunction<ResultSet, String, O, SQLException> resultSetGetter, Class<O> expectedType) {
		assertThat(ResultSetReader.ofMethodReference(resultSetGetter).getType()).isEqualTo(expectedType);
	}
	
	
	public static Date getTimeAsStatic(long date) {
		return new Date(date);
	}
	
	public Date getTimeAsNonStatic(long date) {
		return new Date(date);
	}
}