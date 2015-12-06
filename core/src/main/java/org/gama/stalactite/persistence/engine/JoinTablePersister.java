package org.gama.stalactite.persistence.engine;

import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.KeepOrderSet;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.result.Row;
import org.gama.sql.result.RowIterator;
import org.gama.stalactite.persistence.engine.listening.*;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSelect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SelectQueryBuilder;
import org.gama.stalactite.query.model.SelectQuery;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.*;

import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.*;

/**
 * Persister for entity with multiple joined tables by primary key.
 * A main table is defined by the {@link ClassMappingStrategy} passed to constructor. Complementary tables are defined
 * with {@link #setMappingStrategies(Collection)}.
 * Entity load is defined by a select that joins all tables, each {@link ClassMappingStrategy} is called to complete
 * entity loading.
 * 
 * @author Guillaume Mary
 */
public class JoinTablePersister<T> extends Persister<T> {
	
	/** Strategies and executors that must be applied, LinkedHashMap to keep order of {@link #setMappingStrategies(Collection)} parameter */
	private Map<ClassMappingStrategy<T>, PersisterExecutor<T>> executorPerStrategy = new LinkedHashMap<>();
	private final Dialect dialect;
	
	public JoinTablePersister(PersistenceContext persistenceContext, ClassMappingStrategy<T> mainMappingStrategy) {
		this(mainMappingStrategy, persistenceContext.getDialect(), persistenceContext.getTransactionManager(),
				persistenceContext.getJDBCBatchSize());
	}
	
	public JoinTablePersister(ClassMappingStrategy<T> mainMappingStrategy, Dialect dialect, TransactionManager transactionManager, int jdbcBatchSize) {
		super(mainMappingStrategy, transactionManager, configureIdentifierFixer(mainMappingStrategy), dialect.getDmlGenerator(),
				dialect.getWriteOperationRetryer(), jdbcBatchSize, dialect.getInOperatorMaxSize());
		this.dialect = dialect;
	}
	
	/**
	 * Set additionnal mapping strategies to apply for persistence. They will be called after the main strategy
	 * (passed in constructor), in order of the Collection, or in reverse order for delete actions to take into account
	 * potential foreign keys.
	 * 
	 * @param mappingStrategies
	 */
	public void setMappingStrategies(Collection<ClassMappingStrategy<T>> mappingStrategies) {
		this.executorPerStrategy.clear();
		for (ClassMappingStrategy<T> mappingStrategy : mappingStrategies) {
			// NB: we force a NoopIdentifierFixer instance because secondary tables must use same key as main table
			final PersisterExecutor<T> persisterExecutor = newPersisterExecutor(
					mappingStrategy,
					NoopIdentifierFixer.INSTANCE,
					getPersisterExecutor().getTransactionManager(),
					getPersisterExecutor().getDmlGenerator(),
					getPersisterExecutor().getWriteOperationRetryer(),
					getPersisterExecutor().getBatchSize(),
					getPersisterExecutor().getInOperatorMaxSize());
			this.executorPerStrategy.put(mappingStrategy, persisterExecutor);
			PersisterListener<T> persisterListener = getPersisterListener();
			persisterListener.addInsertListener(new NoopInsertListener<T>() {
				@Override
				public void afterInsert(Iterable<T> iterable) {
					persisterExecutor.insert(iterable);
				}
			});
			persisterListener.addUpdateListener(new NoopUpdateListener<T>() {
				@Override
				public void afterUpdate(Iterable<Map.Entry<T, T>> iterables, boolean allColumnsStatement) {
					persisterExecutor.update(iterables, allColumnsStatement);
				}
			});
			persisterListener.addUpdateRouglyListener(new NoopUpdateRouglyListener<T>() {
				@Override
				public void afterUpdateRoughly(Iterable<T> iterables) {
					persisterExecutor.updateRoughly(iterables);
				}
			});
			persisterListener.addDeleteListener(new NoopDeleteListener<T>() {
				@Override
				public void beforeDelete(Iterable<T> iterables) {
					persisterExecutor.delete(iterables);
				}
			});
		}
	}
	
	public Set<ClassMappingStrategy<T>> getMappingStrategies() {
		return executorPerStrategy.keySet();
	}
	
	public Set<Table> getTables() {
		Set<Table> tables  = new LinkedHashSet<>();
		for (ClassMappingStrategy<T> classMappingStrategy : getMappingStrategies()) {
			tables.add(classMappingStrategy.getTargetTable());
		}
		return tables;
	}
	
	private Collection<PersisterExecutor<T>> getPersisterExecutors() {
		return executorPerStrategy.values();
	}
	
	/**
	 * Overriden to implement a load by joining tables
	 * @param ids entity identifiers
	 * @return a List of loaded entities corresponding to identifiers passed as parameter
	 */
	@Override
	protected List<T> doSelect(Iterable<Serializable> ids) {
		SelectExecutor selectExecutor = new SelectExecutor();
		// creating query with join of all tables
		selectExecutor.addComplementaryTables();
		return selectExecutor.select(ids);
	}
	
	
	
	private class SelectExecutor {
		
		private final SelectQuery selectQuery = new SelectQuery();
		private final Map<String, ParameterBinder> selectParameterBinders = new HashMap<>();
		private final Map<Table.Column, ParameterBinder> parameterBinders = new HashMap<>();
		private final Map<Table.Column, int[]> inOperatorValueIndexes = new HashMap<>();
		private final int blockSize = getPersisterExecutor().getInOperatorMaxSize();
		private final Table.Column keyColumn = getTargetTable().getPrimaryKey();
		
		private List<T> result;
		
		public void addComplementaryTables() {
			Table previousTable = getTargetTable();
			addColumnsToSelect(getPersisterExecutor().getMappingStrategy().getTargetTable().getColumns(), selectQuery, selectParameterBinders);
			// looping on secondary tables by joining them and adding their columns to select
			SelectQuery.FluentFrom from = null;
			for (PersisterExecutor<T> complementaryExecutor : getPersisterExecutors()) {
				Table complementaryTable = complementaryExecutor.getMappingStrategy().getTargetTable();
				addColumnsToSelect(complementaryTable.getColumns(), selectQuery, selectParameterBinders);
				if (from == null) {
					// initialization of from with first encountered table
					from = selectQuery.from(previousTable.getPrimaryKey(), complementaryTable.getPrimaryKey());
				} else {
					from.innerJoin(previousTable.getPrimaryKey(), complementaryTable.getPrimaryKey());
				}
			}
		}
		
		private void bindInClause(int inSize) {
			int[] indexes = new int[inSize];
			for (int i = 0; i < inSize;) {
				indexes[i] = ++i;
			}
			inOperatorValueIndexes.put(keyColumn, indexes);
			parameterBinders.put(keyColumn, dialect.getColumnBinderRegistry().getBinder(keyColumn));
		}
		
		private void addColumnsToSelect(KeepOrderSet<Table.Column> selectableColumns, SelectQuery selectQuery, Map<String, ParameterBinder> selectParameterBinders) {
			for (Table.Column selectableColumn : selectableColumns) {
				selectQuery.select(selectableColumn, selectableColumn.getAlias());
				selectParameterBinders.put(selectableColumn.getName(), dialect.getColumnBinderRegistry().getBinder(selectableColumn));
			}
		}
		
		public List<T> select(Iterable<Serializable> ids) {
			// cutting ids into pieces, adjusting expected result size
			List<List<Serializable>> parcels = Collections.parcel(ids, blockSize);
			result = new ArrayList<>(parcels.size() * blockSize);
			
			// Use same Connection for all operations
			PersisterExecutor.ConnectionProvider connectionProvider = getPersisterExecutor().new ConnectionProvider();
			// Creation of the where clause: we use a dynamic "in" operator clause to avoid multiple SelectQuery instanciation
			DynamicInClause condition = new DynamicInClause();
			selectQuery.where(keyColumn, condition);
			SelectQueryBuilder queryBuilder = new SelectQueryBuilder(selectQuery);
			List<Serializable> lastBlock = Iterables.last(parcels);
			// keep only full blocks to run them on the fully filled "in" operator
			if (lastBlock.size() != blockSize) {
				parcels = Collections.cutTail(parcels);
			}
			if (!parcels.isEmpty()) {
				// change parameter mark count to adapt "in" operator values
				condition.setParamMarkCount(blockSize);
				// adding "in" identifiers to where clause
				bindInClause(blockSize);
				execute(connectionProvider, queryBuilder, parcels);
			}
			if (!lastBlock.isEmpty()) {
				// change parameter mark count to adapt "in" operator values
				condition.setParamMarkCount(lastBlock.size());
				bindInClause(lastBlock.size());
				execute(connectionProvider, queryBuilder, java.util.Collections.singleton(lastBlock));
			}
			return result;
		}
		
		private void execute(PersisterExecutor.ConnectionProvider connectionProvider,
								SelectQueryBuilder queryBuilder,
								Iterable<? extends Iterable<Serializable>> idsParcels) {
			ColumnParamedSelect preparedSelect = new ColumnParamedSelect(queryBuilder.toSQL(), inOperatorValueIndexes, parameterBinders, selectParameterBinders);
			ReadOperation<Table.Column> columnReadOperation = new ReadOperation<>(preparedSelect, connectionProvider);
			for (Iterable<Serializable> parcel : idsParcels) {
				execute(columnReadOperation, parcel);
			}
		}
		
		private void execute(ReadOperation<Table.Column> operation, Iterable<Serializable> ids) {
			try(ReadOperation<Table.Column> closeableOperation = operation) {
				operation.setValue(keyColumn, ids);
				ResultSet resultSet = closeableOperation.execute();
				RowIterator rowIterator = new RowIterator(resultSet, ((ColumnParamedSelect) closeableOperation.getSqlStatement()).getSelectParameterBinders());
				while (rowIterator.hasNext()) {
					result.add(transform(rowIterator));
				}
			} catch (Exception e) {
				Exceptions.throwAsRuntimeException(e);
			}
		}
		
		private T transform(RowIterator rowIterator) {
			Row row = rowIterator.next();
			// get entity from main mapping stategy
			T t = getPersisterExecutor().getMappingStrategy().transform(row);
			// complete entity load with complementary mapping strategy
			for (ClassMappingStrategy<T> classMappingStrategy : executorPerStrategy.keySet()) {
				classMappingStrategy.getRowTransformer().applyRowToBean(row, t);
			}
			return t;
		}
	}
	
	
	private static class DynamicInClause implements CharSequence {
		
		private String dynamicIn;
		
		public DynamicInClause setParamMarkCount(int markCount) {
			StringAppender result = new StringAppender(10 + markCount * SQL_PARAMETER_MARK_1.length());
			Strings.repeat(result.getAppender(), markCount, SQL_PARAMETER_MARK_1, SQL_PARAMETER_MARK_100, SQL_PARAMETER_MARK_10);
			result.cutTail(2);
			result.wrap("in (", ")");
			dynamicIn = result.toString();
			return this;
		}
		
		@Override
		public int length() {
			return dynamicIn.length();
		}
		
		@Override
		public char charAt(int index) {
			return dynamicIn.charAt(index);
		}
		
		@Override
		public CharSequence subSequence(int start, int end) {
			return dynamicIn.subSequence(start, end);
		}
	}
}
