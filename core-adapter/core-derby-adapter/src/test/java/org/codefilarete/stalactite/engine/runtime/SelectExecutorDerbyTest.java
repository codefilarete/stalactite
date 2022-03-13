package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.junit.jupiter.api.Disabled;

/**
 * @author Guillaume Mary
 */
@Disabled // Derby can't be tested because it doesn't support "tupled in"
class SelectExecutorDerbyTest extends SelectExecutorITTest {
	
	@Override
	protected DataSource giveDataSource() {
		throw new UnsupportedOperationException("Derby doesn't support tupled in");
	}
}
