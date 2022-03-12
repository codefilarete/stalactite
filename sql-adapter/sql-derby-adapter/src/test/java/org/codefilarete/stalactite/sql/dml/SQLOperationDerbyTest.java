package org.codefilarete.stalactite.sql.dml;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.function.Predicate;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.DerbyDatabaseHelper;
import org.codefilarete.stalactite.sql.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SQLOperationDerbyTest extends SQLOperationITTest {
	
    @Override
	public DataSource giveDataSource() {
        return new DerbyInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new DerbyDatabaseHelper();
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
