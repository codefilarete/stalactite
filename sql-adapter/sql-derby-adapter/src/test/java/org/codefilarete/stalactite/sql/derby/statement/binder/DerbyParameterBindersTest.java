package org.codefilarete.stalactite.sql.derby.statement.binder;

import javax.sql.DataSource;

import java.math.BigDecimal;
import java.sql.SQLException;

import org.assertj.core.data.Offset;
import org.codefilarete.stalactite.sql.statement.binder.AbstractParameterBindersITTest;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.derby.test.DerbyDatabaseHelper;
import org.codefilarete.stalactite.sql.derby.test.DerbyInMemoryDataSource;
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
	protected void createParameterBinderRegistry() {
		parameterBinderRegistry = new DerbyParameterBinderRegistry();
	}
	
	@Override
	@BeforeEach
	protected void createJavaTypeToSqlTypeMapping() {
		javaTypeToSqlTypeMapping = new DerbyTypeMapping();
	}
	
}
