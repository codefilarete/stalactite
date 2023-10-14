package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.EntitySelectExecutor;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.Duo;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismEntitySelectExecutor<C, I, T extends Table<T>, DTYPE> implements EntitySelectExecutor<C> {
	
	private static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	
	private final IdentifierAssembler<I, T> identifierAssembler;
	private final Map<Class<C>, ConfiguredRelationalPersister<C, I>> persisterPerSubclass;
	private final Column discriminatorColumn;
	private final SingleTablePolymorphism polymorphismPolicy;
	private final EntityJoinTree<C, I> entityJoinTree;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public SingleTablePolymorphismEntitySelectExecutor(IdentifierAssembler<I, T> identifierAssembler,
													   Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
													   Column<T, DTYPE> discriminatorColumn,
													   SingleTablePolymorphism polymorphismPolicy,
													   EntityJoinTree<C, I> mainEntityJoinTree,
													   ConnectionProvider connectionProvider,
													   Dialect dialect) {
		this.identifierAssembler = identifierAssembler;
		this.persisterPerSubclass = (Map<Class<C>, ConfiguredRelationalPersister<C, I>>) persisterPerSubclass;
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = polymorphismPolicy;
		this.entityJoinTree = mainEntityJoinTree;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
	}
	
	@Override
	public Set<C> loadGraph(CriteriaChain where) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		
		QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(query, dialect, where, entityTreeQuery.getColumnClones());
		
		// selecting ids and their discriminator
		PrimaryKey<T, I> pk = ((T) entityJoinTree.getRoot().getTable()).getPrimaryKey();
		pk.getColumns().forEach(column -> query.select(column, column.getAlias()));
		query.select(discriminatorColumn, DISCRIMINATOR_ALIAS);
		
		// selecting ids and their entity type
		Map<String, ResultSetReader> columnReaders = new HashMap<>();
		Map<Selectable<?>, String> aliases = query.getAliases();
		aliases.forEach((selectable, s) -> columnReaders.put(s, dialect.getColumnBinderRegistry().getBinder((Column) selectable)));
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		
		List<Duo<I, DTYPE>> ids = readIds(sqlQueryBuilder, columnReaders, columnedRow);
		
		Map<Class, Set<I>> idsPerSubclass = new HashMap<>();
		ids.forEach(id -> idsPerSubclass.computeIfAbsent(polymorphismPolicy.getClass(id.getRight()), k -> new HashSet<>()).add(id.getLeft()));
		
		Set<C> result = new HashSet<>();
		
		idsPerSubclass.forEach((k, v) -> result.addAll(persisterPerSubclass.get(k).select(v)));
		return result;
	}
	
	private List<Duo<I, DTYPE>> readIds(QuerySQLBuilder sqlQueryBuilder, Map<String, ResultSetReader> columnReaders, ColumnedRow columnedRow) {
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, columnReaders);
			List<Duo<I, DTYPE>> result = new ArrayList<>();
			rowIterator.forEachRemaining(row -> result.add(new Duo<>(identifierAssembler.assemble(row, columnedRow), (DTYPE) row.get(DISCRIMINATOR_ALIAS))));
			return result;
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
}
