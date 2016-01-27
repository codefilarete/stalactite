package org.gama.stalactite.persistence.engine;

import org.gama.sql.SimpleConnectionProvider;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;

/**
 * CRUD Persistent features dedicated to an entity class. Kind of sliding door of {@link Persister} aimed at running
 * actions for it.
 * 
 * @author Guillaume Mary
 */
public abstract class DMLExecutor<T> {
	
	private final ClassMappingStrategy<T> mappingStrategy;
	private final TransactionManager transactionManager;
	private final DMLGenerator dmlGenerator;
	private final int inOperatorMaxSize;
	
	public DMLExecutor(ClassMappingStrategy<T> mappingStrategy, TransactionManager transactionManager,
					   DMLGenerator dmlGenerator, int inOperatorMaxSize) {
		this.mappingStrategy = mappingStrategy;
		this.transactionManager = transactionManager;
		this.dmlGenerator = dmlGenerator;
		this.inOperatorMaxSize = inOperatorMaxSize;
	}
	
	public ClassMappingStrategy<T> getMappingStrategy() {
		return mappingStrategy;
	}
	
	public TransactionManager getTransactionManager() {
		return transactionManager;
	}
	
	public DMLGenerator getDmlGenerator() {
		return dmlGenerator;
	}
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	/**
	 * Implementation that gives the TransactionManager.getCurrentConnection() of instanciation time
	 */
	protected class ConnectionProvider extends SimpleConnectionProvider {
		
		public ConnectionProvider() {
			super(transactionManager.getCurrentConnection());
		}
	}
}
