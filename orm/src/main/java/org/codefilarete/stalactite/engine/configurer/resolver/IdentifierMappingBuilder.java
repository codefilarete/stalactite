package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.CompositeKeyMapping;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.KeyMapping;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.SingleKeyMapping;
import org.codefilarete.stalactite.dsl.idpolicy.AlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.BeforeInsertIdentifierPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.BeforeInsertIdentifierPolicySupport;
import org.codefilarete.stalactite.dsl.idpolicy.DatabaseSequenceIdentifierPolicySupport;
import org.codefilarete.stalactite.dsl.idpolicy.GeneratedKeysPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.PooledHiLoSequenceIdentifierPolicySupport;
import org.codefilarete.stalactite.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.model.IdentifierMapping;
import org.codefilarete.stalactite.engine.configurer.resolver.InheritanceMappingResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.BeforeInsertIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.CompositeKeyAlreadyAssignedIdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.manager.JDBCGeneratedKeysIdentifierManager;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequence;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequenceOptions;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequencePersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.function.Sequence;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.first;

public class IdentifierMappingBuilder<C, I> {
	
	private final EntityMappingConfiguration<C, I> keyDefiner;
	private final ResolvedConfiguration<?, I> resolvedConfiguration;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public IdentifierMappingBuilder(EntityMappingConfiguration<C, I> keyDefiner,
	                                ResolvedConfiguration<?, I> resolvedConfiguration,
	                                Dialect dialect,
	                                ConnectionConfiguration connectionConfiguration) {
		this.keyDefiner = keyDefiner;
		this.resolvedConfiguration = resolvedConfiguration;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	public IdentifierMapping<C, I> build() {
		KeyMapping<?, I> foundKeyMapping = keyDefiner.getKeyMapping();
		AccessorDefinition idDefinition = AccessorDefinition.giveDefinition(foundKeyMapping.getAccessor());
		Class<I> identifierType = idDefinition.getMemberType();
		if (foundKeyMapping instanceof SingleKeyMapping) {
			SingleKeyMapping<C, I> singleKeyMapping = (SingleKeyMapping<C, I>) foundKeyMapping;
			return new SingleIdentifierMapping<>((ReadWritePropertyAccessPoint<C, I>) foundKeyMapping.getAccessor(), resolveInsertionManager(foundKeyMapping.getAccessor(), identifierType, singleKeyMapping.getIdentifierPolicy()));
		} else if (foundKeyMapping instanceof CompositeKeyMapping) {
			CompositeKeyMapping<C, I> compositeKeyMapping = (CompositeKeyMapping<C, I>) foundKeyMapping;
			assertCompositeKeyIdentifierOverridesEqualsHashcode(compositeKeyMapping);
			CompositeKeyAlreadyAssignedIdentifierInsertionManager<C, I> identifierManager = new CompositeKeyAlreadyAssignedIdentifierInsertionManager<>(identifierType, compositeKeyMapping.getMarkAsPersistedFunction(), compositeKeyMapping.getIsPersistedFunction());
			EmbeddableMappingBuilder<I, ?> compositeKeyBuilder = new EmbeddableMappingBuilder<>(
					compositeKeyMapping.getCompositeKeyMappingBuilder().getConfiguration(),
					resolvedConfiguration.getTable(),
					dialect.getColumnBinderRegistry(),
					resolvedConfiguration.getNamingConfiguration().getColumnNamingStrategy(),
					resolvedConfiguration.getMappingConfiguration().getUniqueConstraintNamingStrategy());
			
			return new CompositeIdentifierMapping<>(compositeKeyMapping.getAccessor(), identifierManager, compositeKeyBuilder.build());
		} else {
			// should not happen
			throw new MappingConfigurationException("Unknown key mapping : " + foundKeyMapping);
		}
	}
	
	@VisibleForTesting
	static void assertCompositeKeyIdentifierOverridesEqualsHashcode(CompositeKeyMapping<?, ?> compositeKeyIdentification) {
		Class<?> compositeKeyType = AccessorDefinition.giveDefinition(compositeKeyIdentification.getAccessor()).getMemberType();
		try {
			compositeKeyType.getDeclaredMethod("equals", Object.class);
			compositeKeyType.getDeclaredMethod("hashCode");
		} catch (NoSuchMethodException e) {
			throw new MappingConfigurationException("Composite key identifier class " + Reflections.toString(compositeKeyType) + " seems to have default implementation of equals() and hashcode() methods,"
					+ " which is not supported (identifiers must be distinguishable), please make it implement them");
		}
	}
	
	IdentifierInsertionManager<C, I> resolveInsertionManager(ReadWritePropertyAccessPoint<?, I> idAccessor, Class<I> identifierType, IdentifierPolicy<I> identifierPolicy) {
		IdentifierInsertionManager<?, I> identifierInsertionManager = null;
		if (identifierPolicy instanceof GeneratedKeysPolicy) {
			identifierInsertionManager = buildGeneratedKeyInsertionManager(idAccessor);
		} else if (identifierPolicy instanceof BeforeInsertIdentifierPolicy) {
			Sequence<I> sequence;
			if (identifierPolicy instanceof PooledHiLoSequenceIdentifierPolicySupport) {
				sequence = (Sequence<I>) generatePooledHiLoSequence((PooledHiLoSequenceIdentifierPolicySupport) identifierPolicy);
			} else if (identifierPolicy instanceof DatabaseSequenceIdentifierPolicySupport) {
				sequence = (Sequence<I>) generateDatabaseSequence((DatabaseSequenceIdentifierPolicySupport) identifierPolicy);
			} else if (identifierPolicy instanceof BeforeInsertIdentifierPolicySupport) {
				sequence = ((BeforeInsertIdentifierPolicySupport<I>) identifierPolicy).getSequence();
			} else {
				throw new MappingConfigurationException("Before-insert identifier policy " + Reflections.toString(identifierPolicy.getClass()) + " is not supported");
			}
			identifierInsertionManager = new BeforeInsertIdentifierManager<>(new AccessorWrapperIdAccessor<>(idAccessor), sequence, identifierType);
		} else if (identifierPolicy instanceof AlreadyAssignedIdentifierPolicy) {
			identifierInsertionManager = generateAlreadyAssignedIdentifierManager(identifierType, (AlreadyAssignedIdentifierPolicy<C, I>) identifierPolicy);
		}
		return (IdentifierInsertionManager<C, I>) identifierInsertionManager;
	}
	
	private IdentifierInsertionManager<?, I> buildGeneratedKeyInsertionManager(ReadWritePropertyAccessPoint<?, I> idAccessor) {
		IdentifierInsertionManager<?, I> identifierInsertionManager;
		// with identifier set by database generated key, identifier must be retrieved as soon as possible which means by the very first
		// persister, which is current one, which is the first in order of mappings
		if (resolvedConfiguration.getTable().getPrimaryKey().isComposed()) {
			throw new UnsupportedOperationException("Composite primary key is not compatible with database-generated column");
		}
		Column<?, I> primaryKey = (Column<?, I>) first(resolvedConfiguration.getTable().getPrimaryKey().getColumns());
		identifierInsertionManager = new JDBCGeneratedKeysIdentifierManager<>(
				new AccessorWrapperIdAccessor<>(idAccessor),
				dialect.buildGeneratedKeysReader(primaryKey.getName(), primaryKey.getJavaType()),
				primaryKey.getJavaType()
		);
		return identifierInsertionManager;
	}
	
	private Sequence<Long> generatePooledHiLoSequence(PooledHiLoSequenceIdentifierPolicySupport identifierPolicy) {
		Class<C> entityType = keyDefiner.getEntityType();
		PooledHiLoSequenceOptions options = new PooledHiLoSequenceOptions(50, entityType.getSimpleName());
		ConnectionProvider connectionProvider = connectionConfiguration.getConnectionProvider();
		if (!(connectionProvider instanceof SeparateTransactionExecutor)) {
			throw new MappingConfigurationException("Before-insert identifier policy configured with connection that doesn't support separate transaction,"
					+ " please provide a " + Reflections.toString(SeparateTransactionExecutor.class) + " as connection provider or change identifier policy");
		}
		return new PooledHiLoSequence(options,
				new PooledHiLoSequencePersister(identifierPolicy.getStorageOptions(), dialect, (SeparateTransactionExecutor) connectionProvider, connectionConfiguration.getBatchSize()));
	}
	
	private Sequence<Long> generateDatabaseSequence(DatabaseSequenceIdentifierPolicySupport identifierPolicy) {
		Class<C> entityType = keyDefiner.getEntityType();
		DatabaseSequenceIdentifierPolicySupport databaseSequenceSupport = identifierPolicy;
		String sequenceName = databaseSequenceSupport.getDatabaseSequenceNamingStrategy().giveName(entityType);
		Database database = new Database();
		Database.Schema sequenceSchema = nullable(databaseSequenceSupport.getDatabaseSequenceSettings().getSchemaName())
				.map(s -> database.new Schema(s))
				.elseSet(resolvedConfiguration.getTable()::getSchema)
				.get();
		org.codefilarete.stalactite.sql.ddl.structure.Sequence databaseSequence
				= new org.codefilarete.stalactite.sql.ddl.structure.Sequence(sequenceSchema, sequenceName)
				.withBatchSize(databaseSequenceSupport.getDatabaseSequenceSettings().getBatchSize())
				.withInitialValue(databaseSequenceSupport.getDatabaseSequenceSettings().getInitialValue());
		return dialect.getDatabaseSequenceSelectorFactory().create(databaseSequence, connectionConfiguration.getConnectionProvider());
	}
	
	private IdentifierInsertionManager<C, I> generateAlreadyAssignedIdentifierManager(Class<I> identifierType, AlreadyAssignedIdentifierPolicy<C, I> identifierPolicy) {
		IdentifierInsertionManager<C, I> identifierInsertionManager;
		AlreadyAssignedIdentifierPolicy<C, I> alreadyAssignedPolicy = identifierPolicy;
		identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(
				identifierType,
				alreadyAssignedPolicy.getMarkAsPersistedFunction(),
				alreadyAssignedPolicy.getIsPersistedFunction());
		return identifierInsertionManager;
	}
}
