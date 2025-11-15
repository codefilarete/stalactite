package org.codefilarete.stalactite.engine.configurer.onetomany;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
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
	private final AccessorDefinition accessorDefinition;
	private final Supplier<C> collectionFactory;
	
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
		this.orphanRemoval = orphanRemoval;
		this.writeAuthorized = writeAuthorized;
		this.accessorDefinition = buildAccessorDefinition();
		this.collectionFactory = buildCollectionFactory();
	}
	
	private AccessorDefinition buildAccessorDefinition() {
		// we don't use AccessorDefinition.giveMemberDefinition(..) because it gives a cross-member definition, loosing get/set for example,
		// whereas we need this information to build a better association table name
		AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(this.oneToManyRelation.getCollectionProvider());
		return new AccessorDefinition(
				accessorDefinition.getDeclaringClass(),
				accessorDefinition.getName(),
				// we prefer the target persister type to method reference member type because the latter only gets the collection type which is not
				// valuable information for table / column naming
				this.oneToManyRelation.getTargetMappingConfiguration().getEntityType());
	}
	
	private Supplier<C> buildCollectionFactory() {
		Supplier<C> result = oneToManyRelation.getCollectionFactory();
		if (result == null) {
			result = BeanRelationFixer.giveCollectionFactory((Class<C>) oneToManyRelation.getMethodReference().getPropertyType());
		}
		return result;
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
	 * Equivalent to {@link org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation#getMethodReference()} but used for table and colum naming only.
	 * Collection access will be done through {@link OneToManyAssociationConfiguration#getCollectionGetter()} and {@link OneToManyAssociationConfiguration#getCollectionFactory()}
	 */
	public AccessorDefinition getAccessorDefinition() {
		return accessorDefinition;
	}
	
	/**
	 * Gives the collection factory used to instantiate relation field.
	 *
	 * @return the one given by {@link OneToManyRelation#getCollectionFactory()} or one deduced from member signature
	 */
	public Supplier<C> getCollectionFactory() {
		return collectionFactory;
	}
}
