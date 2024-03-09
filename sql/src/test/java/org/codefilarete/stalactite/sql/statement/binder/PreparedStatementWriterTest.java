package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.util.Date;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
	
	public static long getTimeAsStatic(Date date) {
		return date.getTime();
	}
	
	public long getTimeAsNonStatic(Date date) {
		return date.getTime();
	}
}