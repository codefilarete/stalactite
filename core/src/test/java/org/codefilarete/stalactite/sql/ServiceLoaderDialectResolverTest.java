package org.codefilarete.stalactite.sql;

import java.util.Set;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.DialectResolver;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Guillaume Mary
 */
class ServiceLoaderDialectResolverTest {
	
	static Object[][] giveMatchingEntry() {
		DummyDialectEntry dialectA1 = new DummyDialectEntry(new DatabaseSignet("A", 1, 10), new Dialect());
		DummyDialectEntry dialectB110 = new DummyDialectEntry(new DatabaseSignet("B", 1, 10), new Dialect());
		DummyDialectEntry dialectB120 = new DummyDialectEntry(new DatabaseSignet("B", 1, 20), new Dialect());
		DummyDialectEntry dialectB210 = new DummyDialectEntry(new DatabaseSignet("B", 2, 10), new Dialect());
		DummyDialectEntry dialectB220 = new DummyDialectEntry(new DatabaseSignet("B", 2, 20), new Dialect());
		DummyDialectEntry dialectB310 = new DummyDialectEntry(new DatabaseSignet("B", 3, 10), new Dialect());
		DummyDialectEntry dialectC110 = new DummyDialectEntry(new DatabaseSignet("C", 1, 10), new Dialect());
		Set<DummyDialectEntry> availableDialects = Arrays.asSet(dialectA1, dialectB110, dialectB120, dialectB210, dialectB220, dialectB310, dialectC110);
		
		return new Object[][] {
				// Database has no matching dialect on product name, none is returned
				{ availableDialects,  new DatabaseSignet("X", 1, 10), null},
				// database is lower than any available dialects, none is returned
				{ availableDialects,  new DatabaseSignet("B", 0, 0), null},
				// we return the dialect below database version
				{ availableDialects,  new DatabaseSignet("B", 2, 15), dialectB210},
				// database is greater than any available dialects, the greatest available must be used
				{ availableDialects,  new DatabaseSignet("B", 4, 30), dialectB310},
		};
	}
	
	@ParameterizedTest
	@MethodSource
	void giveMatchingEntry(Set<DummyDialectEntry> dialectSet, DatabaseSignet databaseSignet, DummyDialectEntry expectedDialect) {
		ServiceLoaderDialectResolver testInstance = new ServiceLoaderDialectResolver();
		DialectResolver.DialectResolverEntry dialect = testInstance.giveMatchingEntry(dialectSet, databaseSignet);
		
		assertThat(dialect).isEqualTo(expectedDialect);
	}
	
	
	@Test
	void determineDialect_noCompatibleDialectFound_throwsException() {
		ServiceLoaderDialectResolver testInstance = new ServiceLoaderDialectResolver();
		assertThatThrownBy(() -> testInstance.determineDialect(Arrays.asSet(new DummyDialectEntry(new DatabaseSignet("A", 0, 0), new Dialect()),
															   new DummyDialectEntry(new DatabaseSignet("A", 0, 0), new Dialect())),
															   new DatabaseSignet("B", 2, 10)))
				// we only check main element of message : exception type and exact message is not so important 
				.hasMessageContaining("Unable to determine dialect")
				.hasMessageContaining("B 2.10");
	}
	
	@Test
	void determineDialect_multipleDialectsWithSameCompatibilityGiven_throwsException() {
		ServiceLoaderDialectResolver testInstance = new ServiceLoaderDialectResolver();
		assertThatThrownBy(() -> testInstance.giveMatchingEntry(Arrays.asSet(new DummyDialectEntry(new DatabaseSignet("B", 0, 0), null),
																			   new DummyDialectEntry(new DatabaseSignet("B", 0, 0), null)),
																  new DatabaseSignet("B", 2, 10)))
				.hasMessageContaining("Multiple dialects with same database compatibility found : B 0.0");
	}
	
	
	static class DummyDialectEntry implements DialectResolver.DialectResolverEntry {
		
		private final DatabaseSignet databaseSignet;
		
		private final Dialect dialect;
		
		DummyDialectEntry(DatabaseSignet databaseSignet, Dialect dialect) {
			this.databaseSignet = databaseSignet;
			this.dialect = dialect;
		}
		
		@Override
		public DatabaseSignet getCompatibility() {
			return databaseSignet;
		}
		
		@Override
		public Dialect getDialect() {
			return dialect;
		}
	}
}