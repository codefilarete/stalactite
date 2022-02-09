package org.codefilarete.stalactite.sql.dml;

import java.util.Objects;
import java.util.function.Predicate;

import org.codefilarete.stalactite.sql.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SQLOperationDerbyTest extends SQLOperationITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new DerbyInMemoryDataSource();
    }

    @Override
    String giveLockStatement() {
        return "lock table Toto in EXCLUSIVE MODE";
    }

    @Override
    Predicate<Throwable> giveCancelOperationPredicate() {
        return Objects::isNull;
    }
	
	@BeforeEach
	protected void createReadOperationFactory() {
		this.readOperationFactory = DerbyReadOperation::new;
	}
}
