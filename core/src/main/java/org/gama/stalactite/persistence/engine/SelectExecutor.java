package org.gama.stalactite.persistence.engine;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.result.RowIterator;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSelect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Column;
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
		CurrentConnectionProvider currentConnectionProvider = new CurrentConnectionProvider();
		List<C> toReturn = new ArrayList<>(50);
		int blockSize = getInOperatorMaxSize();
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		T targetTable = getMappingStrategy().getTargetTable();
		Set<Column<T, Object>> columnsToRead = targetTable.getColumns();
		Column<T, Object> primaryKey = targetTable.getPrimaryKey();
		
		// We distinguish the default case where packets are of the same size from the (last) case where it's different
		// So we can apply the same read operation to all the firsts packets
		ReadOperation<Column<T, Object>> defaultReadOperation = newReadOperation(targetTable, columnsToRead, blockSize, currentConnectionProvider);
		Collections.cutTail(parcels).forEach(parcel -> toReturn.addAll(execute(defaultReadOperation, primaryKey, parcel)));
		// last packet treatment (packet size may be different)
		List<I> lastParcel = Iterables.last(parcels);
		int lastBlockSize = lastParcel.size();
		ReadOperation<Column<T, Object>> lastReadOperation = lastBlockSize != blockSize
				? newReadOperation(targetTable, columnsToRead, lastBlockSize, currentConnectionProvider)
				: defaultReadOperation;
		toReturn.addAll(execute(lastReadOperation, primaryKey, lastParcel));
		return toReturn;
	}
	
	private ReadOperation<Column<T, Object>> newReadOperation(T targetTable, Set<Column<T, Object>> columnsToRead, int blockSize,
												   CurrentConnectionProvider currentConnectionProvider) {
		Column<T, Object> primaryKey = targetTable.getPrimaryKey();
		ColumnParamedSelect<T> selectStatement = getDmlGenerator().buildSelectByKey(targetTable, columnsToRead, primaryKey, blockSize);
		return (ReadOperation) new ReadOperation<>(selectStatement, currentConnectionProvider);
	}
	
	private List<C> execute(ReadOperation<Column<T, Object>> operation, Column<T, Object> primaryKey, List<I> values) {
		List<C> toReturn = new ArrayList<>(values.size());
		try (ReadOperation<Column<T, Object>> closeableOperation = operation) {
			// we must pass a single value when expected, else ExpandableStatement may be confused when applying them
			operation.setValue(primaryKey, values.size() == 1 ? values.get(0) : values);
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, ((ColumnParamedSelect) closeableOperation.getSqlStatement()).getSelectParameterBinders());
			while (rowIterator.hasNext()) {
				toReturn.add(getMappingStrategy().transform(rowIterator.next()));
			}
		} catch (Exception e) {
			throw Exceptions.asRuntimeException(e);
		}
		return toReturn;
	}
}
