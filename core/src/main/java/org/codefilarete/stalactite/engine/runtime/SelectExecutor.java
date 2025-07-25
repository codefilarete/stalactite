package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.Collection;
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
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.ColumnedRowIterator;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSelect;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.SQLOperation;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class dedicated to select statement execution
 * 
 * @author Guillaume Mary
 */
public class SelectExecutor<C, I, T extends Table<T>> extends DMLExecutor<C, I, T> implements org.codefilarete.stalactite.engine.SelectExecutor<C, I> {
	
	protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	
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
		Set<C> result = new HashSet<>(Iterables.size(ids, () -> 50));
		// We ensure that the same Connection is used for all operations
		ConnectionProvider localConnectionProvider = new SimpleConnectionProvider(getConnectionProvider().giveConnection());
		// We distinguish the default case where chunks are of the same size, from the (last) case where it's different
		// So we can apply the same read operation to all the first chunks
		Iterables.forEachChunk(
				ids,
				getInOperatorMaxSize(),
				chunks -> {
					LOGGER.debug("selecting entities in {} chunks", chunks.size());
				},
				chunkSize -> newReadOperation(getMapping().getTargetTable(), getMapping().getSelectableColumns(), chunkSize, localConnectionProvider),
				(readOperation, chunk) -> result.addAll(internalExecutor.execute(readOperation, chunk)),
				SQLOperation::close
		);
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
		private final Function<ColumnedRow, C> transformer;
		
		InternalExecutor(EntityMapping<C, I, T> mapping) {
			this(mapping.getIdMapping().getIdentifierAssembler(), mapping::transform);
		}
		
		/**
		 * @param primaryKeyProvider will provide entity ids necessary to set parameters of {@link ReadOperation} before its execution 
		 * @param transformer will transform result given by {@link ReadOperation} execution
		 */
		InternalExecutor(IdentifierAssembler<I, T> primaryKeyProvider, Function<ColumnedRow, C> transformer) {
			this.primaryKeyProvider = primaryKeyProvider;
			this.transformer = transformer;
		}
		
		@VisibleForTesting
		Set<C> execute(ReadOperation<Column<T, ?>> operation, Collection<I> ids) {
			Map<Column<T, ?>, ?> primaryKeyValues = primaryKeyProvider.getColumnValues(ids);
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
			ColumnParameterizedSelect<?> sqlStatement = (ColumnParameterizedSelect) closeableOperation.getSqlStatement();
			Iterator<? extends ColumnedRow> rowIterator = new ColumnedRowIterator(
					resultSet,
					sqlStatement.getSelectParameterBinders(),
					sqlStatement.getAliases());
			return transform(rowIterator, size);
		}
		
		protected Set<C> transform(Iterator<? extends ColumnedRow> rowIterator, int resultSize) {
			Iterable<? extends ColumnedRow> rows = () -> (Iterator<ColumnedRow>) rowIterator;
			return Iterables.collect(rows, transformer, () -> new HashSet<>(resultSize));
		}
	}
}
