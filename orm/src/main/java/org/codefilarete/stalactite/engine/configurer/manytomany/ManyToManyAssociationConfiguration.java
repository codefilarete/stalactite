package org.codefilarete.stalactite.engine.configurer.manytomany;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.DefaultReadWritePropertyAccessPoint;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation.MappedByConfiguration;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Reflections;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Class that stores elements necessary to many-to-many association configuration
 * @author Guillaume Mary
 */
class ManyToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C1 extends Collection<TRGT>, C2 extends Collection<SRC>,
		LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> {
	
	private final ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation;
	private final ConfiguredRelationalPersister<SRC, SRCID> srcPersister;
	private final PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	private final ReadWritePropertyAccessPoint<SRC, C1> collectionGetter;
	private final Mutator<SRC, C1> setter;
	private final boolean orphanRemoval;
	private final boolean writeAuthorized;
	private final AccessorDefinition accessorDefinition;
	private final Supplier<C1> collectionFactory;
	
	ManyToManyAssociationConfiguration(ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> manyToManyRelation,
									   ConfiguredRelationalPersister<SRC, SRCID> srcPersister,
									   PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey,
									   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
									   ColumnNamingStrategy indexColumnNamingStrategy,
									   boolean orphanRemoval,
									   boolean writeAuthorized) {
		this.manyToManyRelation = manyToManyRelation;
		this.srcPersister = srcPersister;
		this.leftPrimaryKey = leftPrimaryKey;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.collectionGetter = manyToManyRelation.getCollectionAccessor();
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
		this.setter = collectionGetter;
		// we don't use AccessorDefinition.giveMemberDefinition(..) because it gives a cross-member definition, loosing get/set for example,
		// whereas we need this information to build better association table name
		this.orphanRemoval = orphanRemoval;
		this.writeAuthorized = writeAuthorized;
		this.accessorDefinition = AccessorDefinition.giveDefinition(this.manyToManyRelation.getCollectionAccessor()); //buildAccessorDefinition();
		this.collectionFactory = buildCollectionFactory();
	}
	
	private Supplier<C1> buildCollectionFactory() {
		Supplier<C1> result = manyToManyRelation.getCollectionFactory();
		if (result == null) {
			result = Reflections.giveCollectionFactory((Class<C1>) this.accessorDefinition.getMemberType());
		}
		return result;
	}
	
	public ManyToManyRelation<SRC, TRGT, TRGTID, C1, C2> getManyToManyRelation() {
		return manyToManyRelation;
	}
	
	public ColumnNamingStrategy getIndexColumnNamingStrategy() {
		return indexColumnNamingStrategy;
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
	
	public ReadWritePropertyAccessPoint<SRC, C1> getCollectionGetter() {
		return collectionGetter;
	}
	
	public Mutator<SRC, C1> getSetter() {
		return setter;
	}
	
	public boolean isOrphanRemoval() {
		return orphanRemoval;
	}
	
	public boolean isWriteAuthorized() {
		return writeAuthorized;
	}
	
	/**
	 * Equivalent to {@link ManyToManyRelation#getCollectionAccessor()} but used for table and colum naming.
	 */
	public AccessorDefinition getAccessorDefinition() {
		return accessorDefinition;
	}
	
	/**
	 * Gives the collection factory used to instantiate relation field.
	 *
	 * @return the one given by {@link ManyToManyRelation#getCollectionFactory()} or one deduced from member signature
	 */
	protected Supplier<C1> getCollectionFactory() {
		return collectionFactory;
	}
	
	/**
	 * Build the accessor for the reverse property, made of configured getter and setter. If one of them is unavailable, it's deduced from the
	 * present one. If both are absent, null is returned. 
	 * 
	 * @return null if no getter nor setter were defined
	 */
	@javax.annotation.Nullable
	ReadWritePropertyAccessPoint<TRGT, C2> buildReversePropertyAccessor() {
		MappedByConfiguration<SRC, TRGT, C2> mappedByConfiguration = manyToManyRelation.getMappedByConfiguration();
		Nullable<SerializablePropertyAccessor<TRGT, C2>> getterReference = nullable(mappedByConfiguration.getReverseCollectionAccessor());
		Nullable<SerializablePropertyMutator<TRGT, C2>> setterReference = nullable(mappedByConfiguration.getReverseCollectionMutator());
		if (getterReference.isAbsent() && setterReference.isAbsent()) {
			return null;
		} else if (getterReference.isPresent() && setterReference.isPresent()) {
			// we keep close to user demand : we keep its method references
			return new DefaultReadWritePropertyAccessPoint<>(getterReference.get(), setterReference.get());
		} else if (getterReference.isPresent() && setterReference.isAbsent()) {
			// we keep close to user demand : we keep its method reference ...
			// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
			return Accessors.readWriteAccessPoint(getterReference.get());
		} else {
			// we keep close to user demand : we keep its method reference ...
			// ... but we can't do it for getter, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
			return Accessors.readWriteAccessPoint(setterReference.get());
		}
	}
}
