package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.stream.Stream;

import org.codefilarete.tool.function.SerializableThrowingBiFunction;
import org.codefilarete.tool.function.SerializableThrowingTriConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PreparedStatementWriterTest {
	
	@Test
	void preApply_typeIsConverterOne() {
		PreparedStatementWriter<Long> preparedStatementWriter = new PreparedStatementWriter<Long>() {
			@Override
			public Class<Long> getType() {
				return long.class;
			}
			
			@Override
			public void set(PreparedStatement preparedStatement, int valueIndex, Long value) {
				
			}
		};
		assertThat(preparedStatementWriter.getType()).isEqualTo(long.class);
		
		// testing with an instance method reference
		PreparedStatementWriter<Date> testInstance1 = preparedStatementWriter.preApply(Date::getTime);
		assertThat(testInstance1.getType()).isEqualTo(Date.class);
		
		// testing with an instance method reference
		PreparedStatementWriter<Date> testInstance2 = preparedStatementWriter.preApply(PreparedStatementWriterTest::getTimeAsStatic);
		assertThat(testInstance2.getType()).isEqualTo(Date.class);
		
		// testing with an instance converter
		PreparedStatementWriter<Date> testInstance3 = preparedStatementWriter.preApply(this::getTimeAsNonStatic);
		assertThat(testInstance3.getType()).isEqualTo(Date.class);
		
		PreparedStatementWriter<?> testInstance4 = preparedStatementWriter.preApply((Date date) -> {
			{
				// Do anything to ensure this piece of code is a real lambda and won't be optimized by JVM to make it
				// a method reference
				String s = "Hello world".toUpperCase();
				s = s.toLowerCase();
			}
			return date.getTime();
		});
		assertThat(testInstance4.getType()).isEqualTo(Date.class);
	}
	
	static Stream<Arguments> ofMethodReference() {
		return Stream.of(
				arguments((SerializableThrowingTriConsumer<PreparedStatement, Integer, Long, SQLException>) PreparedStatement::setLong, long.class),
				arguments((SerializableThrowingTriConsumer<PreparedStatement, Integer, java.sql.Date, SQLException>) PreparedStatement::setDate, java.sql.Date.class),
				arguments((SerializableThrowingTriConsumer<PreparedStatement, Integer, Double, SQLException>) PreparedStatement::setDouble, double.class),
				arguments((SerializableThrowingTriConsumer<PreparedStatement, Integer, Float, SQLException>) PreparedStatement::setFloat, float.class),
				arguments((SerializableThrowingTriConsumer<PreparedStatement, Integer, String, SQLException>) PreparedStatement::setString, String.class)
		);
	}
	
	@ParameterizedTest
	@MethodSource
	<O> void ofMethodReference(SerializableThrowingTriConsumer<PreparedStatement, Integer, O, SQLException> preparedStatementSetter, Class<O> expectedType) {
		assertThat(PreparedStatementWriter.ofMethodReference(preparedStatementSetter).getType()).isEqualTo(expectedType);
	}
	
	
	public static long getTimeAsStatic(Date date) {
		return date.getTime();
	}
	
	public long getTimeAsNonStatic(Date date) {
		return date.getTime();
	}
}