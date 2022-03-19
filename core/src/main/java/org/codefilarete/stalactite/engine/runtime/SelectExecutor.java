package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSelect;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.result.RowIterator;

/**
 * Class dedicated to select statement execution
 * 
 * @author Guillaume Mary
 */
public class SelectExecutor<C, I, T extends Table> extends DMLExecutor<C, I, T> implements org.codefilarete.stalactite.engine.SelectExecutor<C, I> {
	
	protected SQLOperationListener<Column<T, Object>> operationListener;
	
	public SelectExecutor(EntityMapping<C, I, T> mappingStrategy, ConnectionProvider connectionProvider, DMLGenerator dmlGenerator, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, inOperatorMaxSize);
	}
	
	public void setOperationListener(SQLOperationListener<Column<T, Object>> operationListener) {
		this.operationListener = operationListener;
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		int blockSize = getInOperatorMaxSize();
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		List<C> result = new ArrayList<>(50);
		if (!parcels.isEmpty()) {
			List<I> lastParcel = Iterables.last(parcels, java.util.Collections.emptyList());
			int lastBlockSize = lastParcel.size();
			if (lastBlockSize != blockSize) {
				parcels = Collections.cutTail(parcels);
			} else {
				lastParcel = java.util.Collections.emptyList();
			}
			// We ensure that the same Connection is used for all operations
			ConnectionProvider localConnectionProvider = new SimpleConnectionProvider(getConnectionProvider().giveConnection());
			// We distinguish the default case where packets are of the same size from the (last) case where it's different
			// So we can apply the same read operation to all the firsts packets
			T targetTable = getMapping().getTargetTable();
			Set<Column<T, Object>> columnsToRead = getMapping().getSelectableColumns();
			InternalExecutor executor = new InternalExecutor();
			if (!parcels.isEmpty()) {
				ReadOperation<Column<T, Object>> defaultReadOperation = newReadOperation(targetTable, columnsToRead, blockSize, localConnectionProvider);
				parcels.forEach(parcel -> result.addAll(executor.execute(defaultReadOperation, parcel)));
			}
			
			// last packet treatment (packet size may be different)
			if (!lastParcel.isEmpty()) {
				ReadOperation<Column<T, Object>> lastReadOperation = newReadOperation(targetTable, columnsToRead, lastBlockSize, localConnectionProvider);
				result.addAll(executor.execute(lastReadOperation, lastParcel));
			}
		}
		return result;
	}
	
	@SuppressWarnings("java:S2095")	// ReadOperation is close at execution time and is not used in this method
	private ReadOperation<Column<T, Object>> newReadOperation(T targetTable, Set<Column<T, Object>> columnsToRead, int blockSize,
												   ConnectionProvider connectionProvider) {
		PrimaryKey<T> primaryKey = targetTable.getPrimaryKey();
		ColumnParameterizedSelect<T> selectStatement = getDmlGenerator().buildSelectByKey(targetTable, columnsToRead, primaryKey.getColumns(), blockSize);
		ReadOperation<Column<T, Object>> readOperation = new ReadOperation<>(selectStatement, connectionProvider);
		readOperation.setListener(this.operationListener);
		return readOperation;
	}
	
	/**
	 * Small class that focuses on operation execution and entity loading.
	 * Kind of method group serving same purpose, made non static for simplicity.
	 */
	@VisibleForTesting
	class InternalExecutor {
		
		@VisibleForTesting
		List<C> execute(ReadOperation<Column<T, Object>> operation, List<I> ids) {
			Map<Column<T, Object>, Object> primaryKeyValues = getMapping().getIdMapping().getIdentifierAssembler().getColumnValues(ids);
			try (ReadOperation<Column<T, Object>> closeableOperation = operation) {
				closeableOperation.setValues(primaryKeyValues);
				return transform(closeableOperation, primaryKeyValues.size());
			} catch (RuntimeException e) {
				throw new SQLExecutionException(operation.getSqlStatement().getSQL(), e);
			}
		}
		
		protected List<C> transform(ReadOperation<Column<T, Object>> closeableOperation, int size) {
			ResultSet resultSet = closeableOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			RowIterator rowIterator = new RowIterator(resultSet, ((ColumnParameterizedSelect) closeableOperation.getSqlStatement()).getSelectParameterBinders());
			return transform(rowIterator, size);
		}
		
		protected List<C> transform(Iterator<Row> rowIterator, int resultSize) {
			return Iterables.collect(() -> rowIterator, getMapping()::transform, () -> new ArrayList<>(resultSize));
		}
	}
}