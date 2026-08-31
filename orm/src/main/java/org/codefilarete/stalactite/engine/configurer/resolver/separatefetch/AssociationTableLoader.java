package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.IdentityLinkedMap;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.query.api.QualifiedSelectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;

/**
 * {@link SelectExecutor} that loads the records of an association table from the identifiers of the entities that
 * own the relation (the "left" or "source" side of the association).
 * <p>
 * It owns its own {@link EntityJoinTree} whose root is the association table, which makes it the entry point of any
 * second-phase (separate fetch) loading : the aggregate main query doesn't have to join the association table
 * anymore, which prevents it from returning a cartesian product of all the relations of the aggregate.
 * <p>
 * Note that records are not selected through the association table persister because its select is made from the
 * record identifier (which is the whole association row), whereas on a separate loading we only have the source
 * entity identifiers at hand.
 *
 * @param <C> record type (association row bean)
 * @param <ID> record identifier type
 * @param <SRC> type of the entity owning the relation
 * @param <SRCID> identifier type of the entity owning the relation
 * @param <LEFTTABLE> table of the entity owning the relation
 * @param <ASSOCIATIONTABLE> association table type
 * @author Guillaume Mary
 */
public class AssociationTableLoader<C, ID, SRC, SRCID, LEFTTABLE extends Table<LEFTTABLE>, ASSOCIATIONTABLE extends Table<ASSOCIATIONTABLE>> implements SelectExecutor<C, SRCID> {
	
	private final EntityJoinTree<C, ID> entityJoinTree;
	private final IdMapping<SRC, SRCID> sourceIdMapping;
	private final Map<QualifiedSelectable<LEFTTABLE, ?>, QualifiedSelectable<ASSOCIATIONTABLE, ?>> reverseForeignKey;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	/**
	 * @param sourceIdMapping identifier mapping of the entity owning the relation, used to build the where clause of the select
	 * @param associationRecordMapping mapping of the association table records, used as the root of the internal {@link EntityJoinTree}
	 * @param reverseForeignKey mapping between the columns of the source entity table and those of the association table
	 * @param dialect the dialect to build and execute the query
	 * @param connectionProvider the provider of the connection the query will be executed on
	 */
	public AssociationTableLoader(IdMapping<SRC, SRCID> sourceIdMapping,
	                              EntityMapping<C, ID, ASSOCIATIONTABLE> associationRecordMapping,
	                              Map<QualifiedSelectable<LEFTTABLE, ?>, QualifiedSelectable<ASSOCIATIONTABLE, ?>> reverseForeignKey,
	                              Dialect dialect,
	                              ConnectionProvider connectionProvider) {
		this.sourceIdMapping = sourceIdMapping;
		this.reverseForeignKey = reverseForeignKey;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		this.entityJoinTree = new EntityJoinTree<>(associationRecordMapping);
	}
	
	/**
	 * Gives the join tree of this loader : its root is the association table, hence any relation that must be loaded
	 * along with the association records has to be grafted on it.
	 *
	 * @return the join tree owned by this loader
	 */
	public EntityJoinTree<C, ID> getEntityJoinTree() {
		return entityJoinTree;
	}
	
	@Override
	public Set<C> select(Iterable<SRCID> ids) {
		int estimatedResultSize = Iterables.size(ids);
		// we avoid relying on Entity equals/Hashcode by using a Map based on System.identityHashCode(..)
		Set<C> result = Collections.newSetFromMap(new IdentityLinkedMap<>(estimatedResultSize));
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		SeparateFetchQueryExecutor<C, SRCID, LEFTTABLE, ASSOCIATIONTABLE> queryExecutor =
				new SeparateFetchQueryExecutor<>(entityTreeQuery, sourceIdMapping, reverseForeignKey, dialect, connectionProvider);
		Iterables.forEachChunk(
				ids,
				dialect.getInOperatorMaxSize(),
				chunks -> {},
				chunkSize -> null,    // no particular initialization to do
				(context, chunk) -> {
					result.addAll(queryExecutor.select(chunk));
				},
				context -> {}
		);
		return result;
	}
}
