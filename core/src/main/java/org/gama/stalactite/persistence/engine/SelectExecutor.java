package org.gama.stalactite.persistence.engine;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.result.RowIterator;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnParameterizedSelect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.PrimaryKey;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Class dedicated to select statement execution
 * 
 * @author Guillaume Mary
 */
public class SelectExecutor<C, I, T extends Table> extends DMLExecutor<C, I, T> {
	
	public SelectExecutor(ClassMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider, DMLGenerator dmlGenerator, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, inOperatorMaxSize);
	}
	
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
			ConnectionProvider localConnectionProvider = new SimpleConnectionProvider(getConnectionProvider().getCurrentConnection());
			// We distinguish the default case where packets are of the same size from the (last) case where it's different
			// So we can apply the same read operation to all the firsts packets
			T targetTable = getMappingStrategy().getTargetTable();
			Set<Column<T, Object>> columnsToRead = getMappingStrategy().getSelectableColumns();
			if (!parcels.isEmpty()) {
				ReadOperation<Column<T, Object>> defaultReadOperation = newReadOperation(targetTable, columnsToRead, blockSize, localConnectionProvider);
				parcels.forEach(parcel -> result.addAll(select(parcel, defaultReadOperation)));
			}
			
			// last packet treatment (packet size may be different)
			if (!lastParcel.isEmpty()) {
				ReadOperation<Column<T, Object>> lastReadOperation = newReadOperation(targetTable, columnsToRead, lastBlockSize, localConnectionProvider);
				result.addAll(select(lastParcel, lastReadOperation));
			}
		}
		return result;
	}
	
	private List<C> select(List<I> ids, ReadOperation<Column<T, Object>> readOperation) {
		Map<Column<T, Object>, Object> pkValues = getMappingStrategy().getIdMappingStrategy().getIdentifierAssembler().getColumnValues(ids);
		return execute(readOperation, pkValues);
	}
	
	private ReadOperation<Column<T, Object>> newReadOperation(T targetTable, Set<Column<T, Object>> columnsToRead, int blockSize,
												   ConnectionProvider connectionProvider) {
		PrimaryKey<T> primaryKey = targetTable.getPrimaryKey();
		ColumnParameterizedSelect<T> selectStatement = getDmlGenerator().buildSelectByKey(targetTable, columnsToRead, primaryKey.getColumns(), blockSize);
		return new ReadOperation<>(selectStatement, connectionProvider);
	}
	
	private List<C> execute(ReadOperation<Column<T, Object>> operation, Map<Column<T, Object>, Object> keyValues) {
		List<C> toReturn = new ArrayList<>(keyValues.size());
		try (ReadOperation<Column<T, Object>> closeableOperation = operation) {
			closeableOperation.setValues(keyValues);
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, ((ColumnParameterizedSelect) closeableOperation.getSqlStatement()).getSelectParameterBinders());
			while (rowIterator.hasNext()) {
				toReturn.add(getMappingStrategy().transform(rowIterator.next()));
			}
		} catch (Exception e) {
			throw Exceptions.asRuntimeException(e);
		}
		return toReturn;
	}
}
