package org.codefilarete.stalactite.persistence.engine.runtime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

/**
 * @author Guillaume Mary
 */
@Disabled // Derby can't be tested because it doesn't support "tupled in"
class SelectExecutorDerbyTest extends SelectExecutorITTest {
	
	@Override
	@BeforeEach
	void createDataSource() {
		throw new UnsupportedOperationException("Derby doesn't support tupled in");
	}
}
