package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;

import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.Exceptions;
import org.gama.reflection.PropertyAccessor;
import org.gama.spy.MethodReferenceCapturer;
import org.gama.sql.IConnectionProvider;
import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.result.Row;
import org.gama.sql.result.RowIterator;
import org.gama.stalactite.persistence.engine.JoinedStrategiesSelect.ParameterBinderProvider;
import org.gama.stalactite.persistence.engine.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.engine.JoinedStrategiesSelect.StrategyJoins.Join;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ToBeanRowTransformer;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSelect;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.builder.SelectQueryBuilder;
import org.gama.stalactite.query.model.SelectQuery;

import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_1;
import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_10;
import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_100;

/**
 * Class aimed at executing a SQL select statement from multiple joined {@link ClassMappingStrategy}
 * 
 * @author Guillaume Mary
 */
public class JoinedStrategiesSelectExecutor<T, I> {
	
	/** The surrogate for joining the strategies, will help to build the SQL */
	private final JoinedStrategiesSelect<T, I> joinedStrategiesSelect;
	private final ParameterBinderProvider parameterBinderProvider;
	private final Map<String, ParameterBinder> selectParameterBinders = new HashMap<>();
	private final Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
	private final Map<Column, int[]> inOperatorValueIndexes = new HashMap<>();
	private final int blockSize;
	private final ConnectionProvider connectionProvider;
	
	private final Column keyColumn;
	private List<T> result;
	
	JoinedStrategiesSelectExecutor(ClassMappingStrategy<T, I> classMappingStrategy, Dialect dialect, ConnectionProvider connectionProvider) {
		this.parameterBinderProvider = c -> dialect.getColumnBinderRegistry().getBinder(c);
		this.joinedStrategiesSelect = new JoinedStrategiesSelect<>(classMappingStrategy, this.parameterBinderProvider);
		this.connectionProvider = connectionProvider;
		// post-initialization
		this.blockSize = dialect.getInOperatorMaxSize();
		this.keyColumn = classMappingStrategy.getTargetTable().getPrimaryKey();
		
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}
	
	/** @deprecated use {@link #addComplementaryTables(String, ClassMappingStrategy, BiConsumer, Column, Column, Class)}  */
	@Deprecated
	public <U> String addComplementaryTables(String leftStrategyName, ClassMappingStrategy<U, ?> mappingStrategy, BiConsumer<T, Iterable<U>> setter,
											 Column leftJoinColumn, Column rightJoinColumn) {
		return joinedStrategiesSelect.add(leftStrategyName, mappingStrategy, leftJoinColumn, rightJoinColumn, setter);
	}
	
	public <U> String addComplementaryTables(String leftStrategyName, ClassMappingStrategy<U, ?> mappingStrategy, BiConsumer<T, Iterable<U>> setter,
											 Column leftJoinColumn, Column rightJoinColumn, Class<? extends Collection> oneToManyType) {
		// we outer join nullable columns
		boolean isOuterJoin = rightJoinColumn.isNullable();
		return joinedStrategiesSelect.add(leftStrategyName, mappingStrategy, leftJoinColumn, rightJoinColumn, isOuterJoin, setter, oneToManyType);
	}
	
	public List<T> select(Iterable<I> ids) {
		// cutting ids into pieces, adjusting expected result size
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		result = new ArrayList<>(parcels.size() * blockSize);
		
		SelectQuery selectQuery = joinedStrategiesSelect.buildSelectQuery();
		SelectQueryBuilder queryBuilder = new SelectQueryBuilder(selectQuery);
		queryBuilder.toSQL();
		
		// Use same Connection for all operations
		IConnectionProvider connectionProvider = new SimpleConnectionProvider(getConnectionProvider().getCurrentConnection());
		// Creation of the where clause: we use a dynamic "in" operator clause to avoid multiple SelectQueryBuilder instanciation
		DynamicInClause condition = new DynamicInClause();
		selectQuery.where(keyColumn, condition);
		List<I> lastBlock = Iterables.last(parcels);
		// keep only full blocks to run them on the fully filled "in" operator
		int lastBlockSize = lastBlock.size();
		if (lastBlockSize != blockSize) {
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
			condition.setParamMarkCount(lastBlockSize);
			bindInClause(lastBlockSize);
			execute(connectionProvider, queryBuilder, java.util.Collections.singleton(lastBlock));
		}
		return result;
	}
	
	private void bindInClause(int inSize) {
		int[] indexes = new int[inSize];
		for (int i = 0; i < inSize; ) {
			indexes[i] = ++i;
		}
		inOperatorValueIndexes.put(keyColumn, indexes);
		parameterBinders.put(keyColumn, this.parameterBinderProvider.getBinder(keyColumn));
	}
	
	private void execute(IConnectionProvider connectionProvider,
						 SelectQueryBuilder queryBuilder,
						 Iterable<? extends Iterable<I>> idsParcels) {
		ColumnParamedSelect preparedSelect = new ColumnParamedSelect(queryBuilder.toSQL(), inOperatorValueIndexes, parameterBinders, selectParameterBinders);
		ReadOperation<Column> columnReadOperation = new ReadOperation<>(preparedSelect, connectionProvider);
		for (Iterable<I> parcel : idsParcels) {
			execute(columnReadOperation, parcel);
		}
	}
	
	private void execute(ReadOperation<Column> operation, Iterable<I> ids) {
		try (ReadOperation<Column> closeableOperation = operation) {
			operation.setValue(keyColumn, ids);
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, ((ColumnParamedSelect) closeableOperation.getSqlStatement()).getSelectParameterBinders());
			while (rowIterator.hasNext()) {
				result.add(transform(rowIterator));
			}
		} catch (Exception e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
	
	T transform(Iterator<Row> rowIterator) {
		List<T> result = transform(rowIterator, joinedStrategiesSelect.getStrategyJoins(JoinedStrategiesSelect.FIRST_STRATEGY_NAME));
		return Iterables.first(result);
	}
	
	static <T> List<T> transform(Iterator<Row> rowIterator, StrategyJoins<T> rootStrategyJoins) {
		List<T> result = new ArrayList<>();
		Map<Class, Map<Object, Object>> entityCache = new HashMap<>();
		for (Row row : (Iterable<Row>) () -> rowIterator) {
			Queue<StrategyJoins> stack = new ArrayDeque<>();
			stack.add(rootStrategyJoins);
			while (!stack.isEmpty()) {
				StrategyJoins<?> strategyJoins = stack.poll();
				ToBeanRowTransformer mainRowTransformer = strategyJoins.getStrategy().getRowTransformer();
				Object primaryKeyValue = row.get(strategyJoins.getTable().getPrimaryKey().getAlias());
				Object rowInstance = entityCache.computeIfAbsent(strategyJoins.getStrategy().getClassToPersist(), k -> new HashMap<>())
						.get(primaryKeyValue);
				if (rowInstance == null) {
					rowInstance = mainRowTransformer.newRowInstance();
					entityCache.computeIfAbsent(strategyJoins.getStrategy().getClassToPersist(), k -> new HashMap<>())
							.put(primaryKeyValue, rowInstance);
					if (strategyJoins == rootStrategyJoins) {
						result.add((T) rowInstance);
					}
					mainRowTransformer.applyRowToBean(row, rowInstance);
				}
				for (Join join : strategyJoins.getJoins()) {
					Object primaryKeyValue2 = row.get(join.getStrategy().getTable().getPrimaryKey().getAlias());
					Object rowInstance2 = entityCache.computeIfAbsent(join.getStrategy().getStrategy().getClassToPersist(), k -> new HashMap<>())
							.get(primaryKeyValue2);
					
					ToBeanRowTransformer rowTransformer = join.getStrategy().getStrategy().getRowTransformer();
					if (rowInstance2 == null) {
						rowInstance2 = rowTransformer.newRowInstance();
						entityCache.computeIfAbsent(join.getStrategy().getStrategy().getClassToPersist(), k -> new HashMap<>())
								.put(primaryKeyValue2, rowInstance2);
						rowTransformer.applyRowToBean(row, rowInstance2);
					}
					Method capture = new MethodReferenceCapturer(rowInstance.getClass()).capture(join.getSetter());
					PropertyAccessor<Object, Object> accessor = PropertyAccessor.of(capture);
					// TODO: generic: la signature en Iterable du join.getSetter est fausse en OneToOne, je me demande comment ça marche ! il faudra sans doute mettre "Object" au lieu de Iterable
					// TODO: fournir également le getter (pour remplacer capture et supprimer spy du pom.xml) car c'est obligatoire pour avoir les "value bean" comme les Collection ou Embeddable
					// TODO: supprimer méthode deprecated JoinedStrategiesSelect#add
					// TODO: sortir cet algorithme dans une classe à part
					// TODO: créer une méthode qui récupère/crée l'entité du cache
					// TODO: construire la Queue 1 fois et la réutiliser avec un Iterator ?
					// TODO: implémenter l'héritage d'entité
					// TODO: implémenter ManyToMany ?
					// TODO: supprimer la dépendance du projet vers spy (mis juste pour MethodReferenceCapturer)
					if (join.getOneToManyType() != null) {
						Collection<Object> oneToManyTarget = (Collection) accessor.get(rowInstance);
						if (oneToManyTarget == null) {
							oneToManyTarget = Reflections.newInstance((Class<Collection>) join.getOneToManyType());
						}
						oneToManyTarget.add(rowInstance2);
						join.getSetter().accept(rowInstance, oneToManyTarget);
					} else {
						join.getSetter().accept(rowInstance, rowInstance2);
					}
					stack.add(join.getStrategy());
				}
			}
		}
		return result;
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
		
		@Override
		public String toString() {
			return dynamicIn;
		}
	}
}
