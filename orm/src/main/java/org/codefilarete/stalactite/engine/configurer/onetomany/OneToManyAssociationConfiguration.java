package org.codefilarete.stalactite.engine.configurer.onetomany;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

/**
 * Class that stores elements necessary to one-to-many association configuration
 */
class OneToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>> {
	
	private final OneToManyRelation<SRC, TRGT, TRGTID, C> oneToManyRelation;
	private final ConfiguredRelationalPersister<SRC, SRCID> srcPersister;
	private final PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	private final ReversibleAccessor<SRC, C> collectionGetter;
	private final String columnName;
	private final Mutator<SRC, C> setter;
	private final boolean orphanRemoval;
	private final boolean writeAuthorized;
	
	OneToManyAssociationConfiguration(OneToManyRelation<SRC, TRGT, TRGTID, C> oneToManyRelation,
									  ConfiguredRelationalPersister<SRC, SRCID> srcPersister,
									  PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey,
									  ForeignKeyNamingStrategy foreignKeyNamingStrategy,
									  JoinColumnNamingStrategy joinColumnNamingStrategy,
									  ColumnNamingStrategy indexColumnNamingStrategy,
									  String columnName,
									  boolean orphanRemoval,
									  boolean writeAuthorized) {
		this.oneToManyRelation = oneToManyRelation;
		this.srcPersister = srcPersister;
		this.leftPrimaryKey = leftPrimaryKey;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
		this.columnName = columnName;
		this.collectionGetter = oneToManyRelation.getCollectionProvider();
		this.setter = collectionGetter.toMutator();
		// we don't use AccessorDefinition.giveMemberDefinition(..) because it gives a cross-member definition, loosing get/set for example,
		// whereas we need this information to build better association table name
		this.orphanRemoval = orphanRemoval;
		this.writeAuthorized = writeAuthorized;
	}
	
	public OneToManyRelation<SRC, TRGT, TRGTID, C> getOneToManyRelation() {
		return oneToManyRelation;
	}
	
	public ConfiguredRelationalPersister<SRC, SRCID> getSrcPersister() {
		return srcPersister;
	}
	
	public PrimaryKey<LEFTTABLE, SRCID> getLeftPrimaryKey() {
		return leftPrimaryKey;
	}
	
	public ForeignKeyNamingStrategy getForeignKeyNamingStrategy() {
		return foreignKeyNamingStrategy;
	}
	
	public JoinColumnNamingStrategy getJoinColumnNamingStrategy() {
		return joinColumnNamingStrategy;
	}
	
	public ColumnNamingStrategy getIndexColumnNamingStrategy() {
		return indexColumnNamingStrategy;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public ReversibleAccessor<SRC, C> getCollectionGetter() {
		return collectionGetter;
	}
	
	public Mutator<SRC, C> getSetter() {
		return setter;
	}
	
	public boolean isOrphanRemoval() {
		return orphanRemoval;
	}
	
	public boolean isWriteAuthorized() {
		return writeAuthorized;
	}
	
	/**
	 * Gives the collection factory used to instantiate relation field.
	 *
	 * @return the one given by {@link OneToManyRelation#getCollectionFactory()} or one deduced from member signature
	 */
	protected Supplier<C> giveCollectionFactory() {
		Supplier<C> collectionFactory = oneToManyRelation.getCollectionFactory();
		if (collectionFactory == null) {
			collectionFactory = BeanRelationFixer.giveCollectionFactory((Class<C>) oneToManyRelation.getMethodReference().getPropertyType());
		}
		return collectionFactory;
	}
}
