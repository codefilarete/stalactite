package org.codefilarete.stalactite.sql.statement.binder;

import javax.sql.DataSource;

import java.math.BigDecimal;
import java.sql.SQLException;

import org.assertj.core.data.Offset;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.DerbyDatabaseHelper;
import org.codefilarete.stalactite.sql.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class DerbyParameterBindersTest extends AbstractParameterBindersITTest {
	
	@Override
	public DataSource giveDataSource() {
		return new DerbyInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new DerbyDatabaseHelper();
	}
	
	@Override
	@BeforeEach
	void createParameterBinderRegistry() {
		parameterBinderRegistry = new DerbyParameterBinderRegistry();
	}
	
	@Override
	@BeforeEach
	void createJavaTypeToSqlTypeMapping() {
		javaTypeToSqlTypeMapping = new DerbyTypeMapping();
	}
	
	@Test
	void bigDecimalBinder() throws SQLException {
		BigDecimal nullInsertion = insertAndSelect(BigDecimal.class, (BigDecimal) null);
		assertThat(nullInsertion).isNull();
		clearSchema();
		BigDecimal real = insertAndSelect(BigDecimal.class, BigDecimal.valueOf(42.66));
		assertThat(real).isCloseTo(BigDecimal.valueOf(42.66), Offset.offset(BigDecimal.valueOf(0.001)));
	}
}