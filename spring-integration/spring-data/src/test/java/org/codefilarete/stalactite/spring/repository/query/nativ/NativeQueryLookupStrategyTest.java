package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.Arrays;

import org.codefilarete.stalactite.spring.repository.query.NativeQueries;
import org.codefilarete.stalactite.spring.repository.query.NativeQuery;
import org.codefilarete.stalactite.sql.Dialect.DialectSupport;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;
import org.codefilarete.tool.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.tool.Nullable.nullable;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NativeQueryLookupStrategyTest {
	
	@Test
	void findSQL_withOneQueryOnMethod_withoutSignet() throws NoSuchMethodException {
		// whatever signet we have on the Dialect, the @NativeQuery sql will be always taken
		DatabaseSignet compatibility = new DatabaseSignet("Vendor 1", 2, 0);
		NativeQueryLookupStrategy<?> testInstance = new NativeQueryLookupStrategy<>(
				null,
				new DialectSupport(
						compatibility, null, null, null, null, null, null, null, null, null, 0, null, null, false),
				null);
		NativeQuery foundSQL = testInstance.findSQL(QueryAnnotationHolderClass.class.getDeclaredMethod("methodWithOneQuery_withoutSignet"));
		assertThat(foundSQL.value()).isEqualTo("any SQL");
	}
	
	static Iterable<Arguments> findSQL_withOneQueryOnMethod() {
		return Arrays.asList(
				// depending on signet we have on the Dialect, the @NativeQuery sql will be used
				arguments(new DatabaseSignet("Oracle", 9, 0), "an Oracle SQL"),
				arguments(new DatabaseSignet("Oracle", 10, 0), "an Oracle SQL"),
				arguments(new DatabaseSignet("Oracle", 11, 0), "an Oracle SQL"),
				arguments(new DatabaseSignet("H2", 2, 0), null)
		);
	}
	
	@ParameterizedTest
	@MethodSource("findSQL_withOneQueryOnMethod")
	void findSQL_withOneQueryOnMethod(DatabaseSignet compatibility, String expectedSQL) throws NoSuchMethodException {
		NativeQueryLookupStrategy<?> testInstance = new NativeQueryLookupStrategy<>(
				null,
				new DialectSupport(
						compatibility, null, null, null, null, null, null, null, null, null, 0, null, null, false),
				null);
		NativeQuery foundSQL = testInstance.findSQL(QueryAnnotationHolderClass.class.getDeclaredMethod("methodWithOneQuery_withSignet"));
		assertThat(nullable(foundSQL).map(NativeQuery::value)).extracting(Nullable::get).isEqualTo(expectedSQL);
	}
	
	static Iterable<Arguments> findSQL_withSeveralQueriesOnMethod() {
		return Arrays.asList(
				// depending on signet we have on the Dialect and the one on @NativeQuery, the right sql will be used
				arguments(new DatabaseSignet("Oracle", 2, 0), "an Oracle SQL"),
				arguments(new DatabaseSignet("MySQL", 2, 0), "a MySQL SQL"),
				arguments(new DatabaseSignet("Oracle", 9, 0), "an Oracle SQL"),
				arguments(new DatabaseSignet("Oracle", 10, 0), "an Oracle10 SQL"),
				arguments(new DatabaseSignet("Oracle", 11, 0), "an Oracle10 SQL"),
				// no SQL given for H2 => null is returned
				arguments(new DatabaseSignet("H2", 2, 0), null)
		);
	}
	
	@ParameterizedTest
	@MethodSource("findSQL_withSeveralQueriesOnMethod")
	void findSQL_withSeveralQueriesOnMethod(DatabaseSignet compatibility, String expectedSQL) throws NoSuchMethodException {
		NativeQueryLookupStrategy<?> testInstance = new NativeQueryLookupStrategy<>(
				null,
				new DialectSupport(
						compatibility, null, null, null, null, null, null, null, null, null, 0, null, null, false),
				null);
		NativeQuery foundSQL = testInstance.findSQL(QueryAnnotationHolderClass.class.getDeclaredMethod("methodWithSeveralQueries"));
		assertThat(nullable(foundSQL).map(NativeQuery::value)).extracting(Nullable::get).isEqualTo(expectedSQL);
	}
	
	static class QueryAnnotationHolderClass {
		
		@NativeQuery("any SQL")
		void methodWithOneQuery_withoutSignet() {
			
		}
		
		@NativeQuery(value = "an Oracle SQL", vendor = "Oracle")
		void methodWithOneQuery_withSignet() {
			
		}
		
		@NativeQueries({
				@NativeQuery(value = "an Oracle SQL", vendor = "Oracle"),
				@NativeQuery(value = "an Oracle10 SQL", vendor = "Oracle", major = 10),
				@NativeQuery(value = "a MySQL SQL", vendor = "MySQL"),
		})
		void methodWithSeveralQueries() {
			
		}
	}
}
