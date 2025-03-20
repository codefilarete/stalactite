package org.codefilarete.stalactite.spring.repository.query;

import java.util.Arrays;

import org.codefilarete.stalactite.sql.Dialect.DialectSupport;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class DeclaredQueryLookupStrategyTest {
	
	@Test
	void findSQL_withOneQueryOnMethod_withoutSignet() throws NoSuchMethodException {
		// whatever signet we have on the Dialect, the @Query sql will be always taken
		DatabaseSignet compatibility = new DatabaseSignet("Vendor 1", 2, 0);
		DeclaredQueryLookupStrategy<?> testInstance = new DeclaredQueryLookupStrategy<>(
				null,
				new DialectSupport(
						compatibility, null, null, null, null, null, null, null, null, null, 0, null, null, false),
				null);
		String foundSQL = testInstance.findSQL(QueryAnnotationHolderClass.class.getDeclaredMethod("methodWithOneQuery_withoutSignet"));
		assertThat(foundSQL).isEqualTo("any SQL");
	}
	
	static Iterable<Arguments> findSQL_withOneQueryOnMethod() {
		return Arrays.asList(
				// depending on signet we have on the Dialect, the @Query sql will be used
				arguments(new DatabaseSignet("Oracle", 9, 0), "an Oracle SQL"),
				arguments(new DatabaseSignet("Oracle", 10, 0), "an Oracle SQL"),
				arguments(new DatabaseSignet("Oracle", 11, 0), "an Oracle SQL"),
				arguments(new DatabaseSignet("H2", 2, 0), null)
		);
	}
	
	@ParameterizedTest
	@MethodSource("findSQL_withOneQueryOnMethod")
	void findSQL_withOneQueryOnMethod(DatabaseSignet compatibility, String expectedSQL) throws NoSuchMethodException {
		DeclaredQueryLookupStrategy<?> testInstance = new DeclaredQueryLookupStrategy<>(
				null,
				new DialectSupport(
						compatibility, null, null, null, null, null, null, null, null, null, 0, null, null, false),
				null);
		String foundSQL = testInstance.findSQL(QueryAnnotationHolderClass.class.getDeclaredMethod("methodWithOneQuery_withSignet"));
		assertThat(foundSQL).isEqualTo(expectedSQL);
	}
	
	static Iterable<Arguments> findSQL_withSeveralQueriesOnMethod() {
		return Arrays.asList(
				// depending on signet we have on the Dialect and the one on @Query, the right sql will be used
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
		DeclaredQueryLookupStrategy<?> testInstance = new DeclaredQueryLookupStrategy<>(
				null,
				new DialectSupport(
						compatibility, null, null, null, null, null, null, null, null, null, 0, null, null, false),
				null);
		String foundSQL = testInstance.findSQL(QueryAnnotationHolderClass.class.getDeclaredMethod("methodWithSeveralQueries"));
		assertThat(foundSQL).isEqualTo(expectedSQL);
	}
	
	static class QueryAnnotationHolderClass {
		
		@Query("any SQL")
		void methodWithOneQuery_withoutSignet() {
			
		}
		
		@Query(value = "an Oracle SQL", vendor = "Oracle")
		void methodWithOneQuery_withSignet() {
			
		}
		
		@Queries({
				@Query(value = "an Oracle SQL", vendor = "Oracle"),
				@Query(value = "an Oracle10 SQL", vendor = "Oracle", major = 10),
				@Query(value = "a MySQL SQL", vendor = "MySQL"),
		})
		void methodWithSeveralQueries() {
			
		}
	}
}
