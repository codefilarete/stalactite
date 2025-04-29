package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSelect;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;

/**
 * Class dedicated to select statement execution
 * 
 * @author Guillaume Mary
 */
public class SelectExecutor<C, I, T extends Table<T>> extends DMLExecutor<C, I, T> implements org.codefilarete.stalactite.engine.SelectExecutor<C, I> {
	
	private final InternalExecutor<C, I, T> internalExecutor;

	private final ReadOperationFactory readOperationFactory;
	
	private final Integer fetchSize;

	protected SQLOperationListener<Column<T, ?>> operationListener;
	
	public SelectExecutor(EntityMapping<C, I, T> mappingStrategy,
						  ConnectionConfiguration connectionConfiguration,
						  DMLGenerator dmlGenerator,
						  ReadOperationFactory readOperationFactory,
						  int inOperatorMaxSize) {
		super(mappingStrategy, connectionConfiguration.getConnectionProvider(), dmlGenerator, inOperatorMaxSize);
		this.internalExecutor = new InternalExecutor<>(mappingStrategy);
		this.readOperationFactory = readOperationFactory;
		this.fetchSize = connectionConfiguration.getFetchSize();
	}
	
	public void setOperationListener(SQLOperationListener<Column<T, ?>> operationListener) {
		this.operationListener = operationListener;
	}
	
	@Override
	public Set<C> select(Iterable<I> ids) {
		int blockSize = getInOperatorMaxSize();
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		Set<C> result = new HashSet<>(50);
		if (!parcels.isEmpty()) {
			List<I> lastParcel = Iterables.last(parcels, java.util.Collections.emptyList());
			int lastBlockSize = lastParcel.size();
			if (lastBlockSize != blockSize) {
				parcels = Iterables.cutTail(parcels);
			} else {
				lastParcel = java.util.Collections.emptyList();
			}
			// We ensure that the same Connection is used for all operations
			ConnectionProvider localConnectionProvider = new SimpleConnectionProvider(getConnectionProvider().giveConnection());
			// We distinguish the default case where packets are of the same size from the (last) case where it's different
			// So we can apply the same read operation to all the firsts packets
			T targetTable = getMapping().getTargetTable();
			Set<Column<T, ?>> columnsToRead = getMapping().getSelectableColumns();
			if (!parcels.isEmpty()) {
				try (ReadOperation<Column<T, ?>> defaultReadOperation = newReadOperation(targetTable, columnsToRead, blockSize, localConnectionProvider)) {
					parcels.forEach(parcel -> result.addAll(internalExecutor.execute(defaultReadOperation, parcel)));
				}
			}
			
			// last packet treatment (packet size may be different)
			if (!lastParcel.isEmpty()) {
				try (ReadOperation<Column<T, ?>> lastReadOperation = newReadOperation(targetTable, columnsToRead, lastBlockSize, localConnectionProvider)) {
					result.addAll(internalExecutor.execute(lastReadOperation, lastParcel));
				}
			}
		}
		return result;
	}
	
	private ReadOperation<Column<T, ?>> newReadOperation(T targetTable,
														 Set<Column<T, ?>> columnsToRead,
														 int blockSize,
														 ConnectionProvider connectionProvider) {
		ColumnParameterizedSelect<T> selectStatement = getDmlGenerator().buildSelectByKey(targetTable, columnsToRead, targetTable.getPrimaryKey().getColumns(), blockSize);
		ReadOperation<Column<T, ?>> readOperation = readOperationFactory.createInstance(selectStatement, connectionProvider, fetchSize);
		readOperation.setListener(this.operationListener);
		return readOperation;
	}
	
	/**
	 * Small class that focuses on operation execution and entity creation from its result.
	 */
	@VisibleForTesting
	static class InternalExecutor<C, I, T extends Table<T>> {
		
		private final IdentifierAssembler<I, T> primaryKeyProvider;
		private final Function<Row, C> transformer;
		
		InternalExecutor(EntityMapping<C, I, T> mapping) {
			this(mapping.getIdMapping().getIdentifierAssembler(), mapping::transform);
		}
		
		/**
		 * @param primaryKeyProvider will provide entity ids necessary to set parameters of {@link ReadOperation} before its execution 
		 * @param transformer will transform result given by {@link ReadOperation} execution
		 */
		InternalExecutor(IdentifierAssembler<I, T> primaryKeyProvider, Function<Row, C> transformer) {
			this.primaryKeyProvider = primaryKeyProvider;
			this.transformer = transformer;
		}
		
		@VisibleForTesting
		Set<C> execute(ReadOperation<Column<T, ?>> operation, List<I> ids) {
			Map<Column<T, ?>, Object> primaryKeyValues = primaryKeyProvider.getColumnValues(ids);
			try (ReadOperation<Column<T, ?>> closeableOperation = operation) {
				closeableOperation.setValues(primaryKeyValues);
				return transform(closeableOperation, primaryKeyValues.size());
			} catch (RuntimeException e) {
				throw new SQLExecutionException(operation.getSqlStatement().getSQL(), e);
			}
		}
		
		protected Set<C> transform(ReadOperation<Column<T, ?>> closeableOperation, int size) {
			ResultSet resultSet = closeableOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			RowIterator rowIterator = new RowIterator(resultSet, ((ColumnParameterizedSelect) closeableOperation.getSqlStatement()).getSelectParameterBinders());
			return transform(rowIterator, size);
		}
		
		protected Set<C> transform(Iterator<Row> rowIterator, int resultSize) {
			return Iterables.collect(() -> rowIterator, transformer, () -> new HashSet<>(resultSize));
		}
	}
}
