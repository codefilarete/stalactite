package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.SimpleConnectionProvider;

/**
 * CRUD Persistent features dedicated to an entity class. Kind of sliding door of {@link Persister} aimed at running
 * actions for it.
 * 
 * @author Guillaume Mary
 */
public abstract class DMLExecutor<C, I, T extends Table> {
	
	private final IEntityMappingStrategy<C, I, T> mappingStrategy;
	private final ConnectionProvider connectionProvider;
	private final DMLGenerator dmlGenerator;
	private final int inOperatorMaxSize;
	
	public DMLExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
					   DMLGenerator dmlGenerator, int inOperatorMaxSize) {
		this.mappingStrategy = mappingStrategy;
		this.connectionProvider = connectionProvider;
		this.dmlGenerator = dmlGenerator;
		this.inOperatorMaxSize = inOperatorMaxSize;
	}
	
	public IEntityMappingStrategy<C, I, T> getMappingStrategy() {
		return mappingStrategy;
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}
	
	public DMLGenerator getDmlGenerator() {
		return dmlGenerator;
	}
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	/**
	 * Implementation that gives the {@link ConnectionProvider#getCurrentConnection()} of instanciation time
	 */
	protected class CurrentConnectionProvider extends SimpleConnectionProvider {
		
		public CurrentConnectionProvider() {
			super(connectionProvider.getCurrentConnection());
		}
	}
}
