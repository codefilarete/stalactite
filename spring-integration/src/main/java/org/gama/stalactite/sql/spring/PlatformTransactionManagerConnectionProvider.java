package org.gama.stalactite.sql.spring;

import javax.annotation.Nonnull;
import java.sql.Connection;

import com.google.common.annotations.VisibleForTesting;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @author Guillaume Mary
 */
public class PlatformTransactionManagerConnectionProvider implements SeparateTransactionExecutor {
	
	/**
	 * Creates adhoc {@link TransactionManagerDataSourceProvider} from given {@link PlatformTransactionManager} based on its concrete type.
	 * Only {@link HibernateTransactionManager}, {@link JpaTransactionManager} and {@link DataSourceTransactionManager} are supported. Exception
	 * will be thrown out of them.
	 * 
	 * @param platformTransactionManager any {@link PlatformTransactionManager} among supported ones
	 * @return a {@link TransactionManagerDataSourceProvider} taking its {@link javax.sql.DataSource} from given {@link PlatformTransactionManager}
	 */
	private static TransactionManagerDataSourceProvider dataSourceProvider(PlatformTransactionManager platformTransactionManager) {
		if (platformTransactionManager instanceof HibernateTransactionManager) {
			return ((HibernateTransactionManager) platformTransactionManager)::getDataSource;
		}
		if (platformTransactionManager instanceof JpaTransactionManager) {
			return ((JpaTransactionManager) platformTransactionManager)::getDataSource;
		}
		if (platformTransactionManager instanceof DataSourceTransactionManager) {
			return ((DataSourceTransactionManager) platformTransactionManager)::getDataSource;
		}
		throw new UnsupportedOperationException("Transaction manager " + platformTransactionManager.getClass() + " is not implemented");
	}
	
	/** {@link PlatformTransactionManager} given at construction time. Used for new transaction building. */
	private final PlatformTransactionManager transactionManager;
	
	/** {@link javax.sql.DataSource} extractor from the transaction manager */
	private final TransactionManagerDataSourceProvider dataSourceProvider;
	
	public PlatformTransactionManagerConnectionProvider(PlatformTransactionManager transactionManager) {
		this(transactionManager, dataSourceProvider(transactionManager));
	}
	
	@VisibleForTesting
	PlatformTransactionManagerConnectionProvider(PlatformTransactionManager transactionManager, TransactionManagerDataSourceProvider dataSourceProvider) {
		this.transactionManager = transactionManager;
		this.dataSourceProvider = dataSourceProvider;
	}
	
	@Nonnull
	@Override
	public Connection getCurrentConnection() {
		return DataSourceUtils.getConnection(dataSourceProvider.getDataSource());
	}
	
	@Override
	public void executeInNewTransaction(JdbcOperation jdbcOperation) {
		TransactionTemplate transactionTemplate = new TransactionTemplate(this.transactionManager);
		transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
		transactionTemplate.execute(new TransactionCallbackWithoutResult() {
			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				jdbcOperation.execute();
			}
		});
	}
}
