package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.configurer.CompositeKeyAlreadyAssignedIdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSelect;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Predicates;

public class CompositeKeyPersister<C, I, T extends Table<T>> extends Persister<C, I, T> {
	
	private final SelectCompositeKeyExecutor<I, T> selectCompositeKeyExecutor;
	
	protected SQLOperationListener<Column<T, Object>> operationListener;
	
	private final CompositeKeyAlreadyAssignedIdentifierInsertionManager<C, I> compositeKeyInsertionManager;
	
	public CompositeKeyPersister(EntityMapping<C, I, T> mappingStrategy, CompositeKeyAlreadyAssignedIdentifierInsertionManager<C, I> compositeKeyInsertionManager, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		super(mappingStrategy, dialect, connectionConfiguration);
		IdentifierAssembler<I, T> identifierAssembler = mappingStrategy.getIdMapping().getIdentifierAssembler();
		this.selectCompositeKeyExecutor = new SelectCompositeKeyExecutor<>(identifierAssembler);
		this.compositeKeyInsertionManager = compositeKeyInsertionManager;
		addPersistListener(this.compositeKeyInsertionManager.getPersistListener());
	}
	
//	@Override
//	public void persist(Iterable<? extends C> entities) {
//		doPersist(excludePersistedEntities(entities));
//	}
	
	@Override
	protected void doPersist(Iterable<? extends C> entities) {
		entities = excludePersistedEntities(entities);
//		super.persist(entities);
		PersistExecutor.persist(entities, this::isNew, this,
				new org.codefilarete.stalactite.engine.UpdateExecutor<C>() {
					@Override
					public void updateById(Iterable<? extends C> entities) {
						CompositeKeyPersister.this.updateById(entities);
					}

					@Override
					public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
						CompositeKeyPersister.this.doUpdate(differencesIterable, allColumnsStatement);
					}
				},
				entities1 -> CompositeKeyPersister.this.doInsert(entities1), getMapping()::getId);
	}
	
	private Iterable<? extends C> excludePersistedEntities(Iterable<? extends C> entities) {
		Map<I, ? extends C> entitiesPerId = Iterables.map(entities, getMapping()::getId);
		List<I> existingEntities = selectIds(entitiesPerId.keySet());
		Predicate<Object> doesExist = Predicates.predicate(existingEntities::contains);
		entitiesPerId.keySet().removeIf(doesExist);
		return entitiesPerId.values();
	}
	
	@Override
	public boolean isNew(C c) {
		// we check for already persisted entities, registered in filterPersistedEntities(..) at persist(..) time
		return !this.compositeKeyInsertionManager.getIsPersistedFunction().apply(c);
	}
	
	private List<I> selectIds(Iterable<I> ids) {
		int blockSize = getInOperatorMaxSize();
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		List<I> result = new ArrayList<>(50);
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
			if (!parcels.isEmpty()) {
				ReadOperation<Column<T, Object>> defaultReadOperation = newReadOperation(targetTable, columnsToRead, blockSize, localConnectionProvider);
				parcels.forEach(parcel -> result.addAll(selectCompositeKeyExecutor.execute(defaultReadOperation, parcel)));
			}
			
			// last packet treatment (packet size may be different)
			if (!lastParcel.isEmpty()) {
				ReadOperation<Column<T, Object>> lastReadOperation = newReadOperation(targetTable, columnsToRead, lastBlockSize, localConnectionProvider);
				result.addAll(selectCompositeKeyExecutor.execute(lastReadOperation, lastParcel));
			}
		}
		return result;
	}
	
	@SuppressWarnings("java:S2095")	// ReadOperation is closed at execution time and is not used in this method
	private ReadOperation<Column<T, Object>> newReadOperation(T targetTable, Set<Column<T, Object>> columnsToRead, int blockSize,
															  ConnectionProvider connectionProvider) {
		PrimaryKey<T, ?> primaryKey = targetTable.getPrimaryKey();
		ColumnParameterizedSelect<T> selectStatement = getDmlGenerator().buildSelectByKey(targetTable, columnsToRead, primaryKey.getColumns(), blockSize);
		ReadOperation<Column<T, Object>> readOperation = new ReadOperation<>(selectStatement, connectionProvider);
		readOperation.setListener(this.operationListener);
		return readOperation;
	}
	
	/**
	 * Small class that focuses on operation execution and entity creation from its result.
	 */
	@VisibleForTesting
	static class SelectCompositeKeyExecutor<I, T extends Table<T>> {
		
		private final IdentifierAssembler<I, T> primaryKeyProvider;
		private final Function<Row, I> transformer;
		
		/**
		 * @param primaryKeyProvider will provide entity ids necessary to set parameters of {@link ReadOperation} before its execution 
		 */
		SelectCompositeKeyExecutor(IdentifierAssembler<I, T> primaryKeyProvider) {
			this.primaryKeyProvider = primaryKeyProvider;
			// we don't need a complex row aliaser since won't have alias in query because we only target one table without join
			ColumnedRow rowAliaser = new ColumnedRow();
			this.transformer = row -> {
				return primaryKeyProvider.assemble(row, rowAliaser);
			};
		}
		
		@VisibleForTesting
		List<I> execute(ReadOperation<Column<T, Object>> operation, List<I> ids) {
			Map<Column<T, Object>, Object> primaryKeyValues = primaryKeyProvider.getColumnValues(ids);
			try (ReadOperation<Column<T, Object>> closeableOperation = operation) {
				closeableOperation.setValues(primaryKeyValues);
				return transform(closeableOperation, primaryKeyValues.size());
			} catch (RuntimeException e) {
				throw new SQLExecutionException(operation.getSqlStatement().getSQL(), e);
			}
		}
		
		protected List<I> transform(ReadOperation<Column<T, Object>> closeableOperation, int size) {
			ResultSet resultSet = closeableOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			RowIterator rowIterator = new RowIterator(resultSet, ((ColumnParameterizedSelect) closeableOperation.getSqlStatement()).getSelectParameterBinders());
			return transform(rowIterator, size);
		}
		
		protected List<I> transform(Iterator<Row> rowIterator, int resultSize) {
			return Iterables.collect(() -> rowIterator, transformer, () -> new ArrayList<>(resultSize));
		}
	}
}
