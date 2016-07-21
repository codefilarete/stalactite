package org.gama.stalactite.persistence.engine;

import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.result.RowIterator;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSelect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Class dedicated to select statement execution
 * 
 * @author Guillaume Mary
 */
public class SelectExecutor<T> extends DMLExecutor<T> {
	
	public SelectExecutor(ClassMappingStrategy<T> mappingStrategy, org.gama.stalactite.persistence.engine.ConnectionProvider connectionProvider, DMLGenerator dmlGenerator, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, inOperatorMaxSize);
	}
	
	
	public List<T> select(Iterable<Serializable> ids) {
		ConnectionProvider connectionProvider = new ConnectionProvider();
		List<T> toReturn = new ArrayList<>(50);
		int blockSize = getInOperatorMaxSize();
		List<List<Serializable>> parcels = Collections.parcel(ids, blockSize);
		List<Serializable> lastBlock = Iterables.last(parcels);
		ColumnParamedSelect selectStatement;
		ReadOperation<Table.Column> readOperation;
		Table targetTable = getMappingStrategy().getTargetTable();
		Set<Table.Column> columnsToRead = targetTable.getColumns().asSet();
		if (parcels.size() > 1) {
			selectStatement = getDmlGenerator().buildMassiveSelect(targetTable, columnsToRead, getMappingStrategy().getSingleColumnKey(), blockSize);
			if (lastBlock.size() != blockSize) {
				parcels = parcels.subList(0, parcels.size() - 1);
			}
			readOperation = newReadOperation(selectStatement, connectionProvider);
			for (List<Serializable> parcel : parcels) {
				toReturn.addAll(execute(readOperation, getMappingStrategy().getSingleColumnKey(), parcel));
			}
		}
		if (lastBlock.size() > 0) {
			selectStatement = getDmlGenerator().buildMassiveSelect(targetTable, columnsToRead, getMappingStrategy().getSingleColumnKey(), lastBlock.size());
			readOperation = newReadOperation(selectStatement, connectionProvider);
			toReturn.addAll(execute(readOperation, getMappingStrategy().getSingleColumnKey(), lastBlock));
		}
		return toReturn;
	}
	
	private <C> ReadOperation<C> newReadOperation(SQLStatement<C> statement, ConnectionProvider connectionProvider) {
		return new ReadOperation<>(statement, connectionProvider);
	}
	
	protected List<T> execute(ReadOperation<Table.Column> operation, Table.Column column, Collection<Serializable> values) {
		List<T> toReturn = new ArrayList<>(values.size());
		try(ReadOperation<Table.Column> closeableOperation = operation) {
			operation.setValue(column, values);
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
