package org.codefilarete.stalactite.engine.configurer.builder;

import java.util.function.Function;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.idpolicy.AlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.BeforeInsertIdentifierPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.BeforeInsertIdentifierPolicySupport;
import org.codefilarete.stalactite.dsl.idpolicy.DatabaseSequenceIdentifierPolicySupport;
import org.codefilarete.stalactite.dsl.idpolicy.GeneratedKeysPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.PooledHiLoSequenceIdentifierPolicySupport;
import org.codefilarete.stalactite.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification.CompositeKeyIdentification;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification.SingleColumnIdentification;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.Mapping;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.MappingPerTable;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.BeforeInsertIdentifierManager;
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
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.function.Sequence;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.Reflections.PRIMITIVE_DEFAULT_VALUES;
import static org.codefilarete.tool.collection.Iterables.first;

public class IdentifierManagerStep<C, I> extends AbstractIdentificationStep<C, I> {
	
	void applyIdentifierManager(AbstractIdentification<C, I> identification,
								MappingPerTable<C> inheritanceMappingPerTable,
								ReversibleAccessor<C, I> idAccessor,
								Dialect dialect,
								ConnectionConfiguration connectionConfiguration) {
		determineIdentifierManager(identification, inheritanceMappingPerTable, idAccessor, dialect, connectionConfiguration);
	}
	
	/**
	 * Determines {@link IdentifierInsertionManager} for current configuration as well as its whole inheritance configuration.
	 * The result is set in given {@link SingleColumnIdentification}. Could have been done on a separate object but it would have complicated some method
	 * signature, and {@link SingleColumnIdentification} is a good place for it.
	 *
	 * @param identification given to know the expected policy and to set the result in it
	 * @param mappingPerTable necessary to get table and primary key to be read in the after-insert policy
	 * @param idAccessor id accessor to get and set identifier on entity (except for already-assigned strategy)
	 * @param dialect dialect to compute elements of identifier policy
	 */
	private void determineIdentifierManager(AbstractIdentification<C, I> identification,
											MappingPerTable<C> mappingPerTable,
											ReversibleAccessor<C, I> idAccessor,
											Dialect dialect,
											ConnectionConfiguration connectionConfiguration) {
		AccessorDefinition idDefinition = AccessorDefinition.giveDefinition(idAccessor);
		Class<I> identifierType = idDefinition.getMemberType();
		IdentifierInsertionManager<C, I> identifierInsertionManager = null;
		if (identification instanceof AbstractIdentification.CompositeKeyIdentification) {
			identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(
					identifierType,
					((CompositeKeyIdentification<C, I>) identification).getMarkAsPersistedFunction(),
					((CompositeKeyIdentification<C, I>) identification).getIsPersistedFunction());
		} else {
			IdentifierPolicy<I> identifierPolicy = ((SingleColumnIdentification<C, I>) identification).getIdentifierPolicy();
			if (identifierPolicy instanceof GeneratedKeysPolicy) {
				// with identifier set by database generated key, identifier must be retrieved as soon as possible which means by the very first
				// persister, which is current one, which is the first in order of mappings
				Table<?> targetTable = first(mappingPerTable.getMappings()).getTargetTable();
				if (targetTable.getPrimaryKey().isComposed()) {
					throw new UnsupportedOperationException("Composite primary key is not compatible with database-generated column");
				}
				Column<?, I> primaryKey = (Column<?, I>) first(targetTable.getPrimaryKey().getColumns());
				identifierInsertionManager = new JDBCGeneratedKeysIdentifierManager<>(
						new AccessorWrapperIdAccessor<>(idAccessor),
						dialect.buildGeneratedKeysReader(primaryKey.getName(), primaryKey.getJavaType()),
						primaryKey.getJavaType()
				);
			} else if (identifierPolicy instanceof BeforeInsertIdentifierPolicy) {
				Sequence<I> sequence;
				if (identifierPolicy instanceof PooledHiLoSequenceIdentifierPolicySupport) {
					Class<C> entityType = identification.getIdentificationDefiner().getEntityType();
					PooledHiLoSequenceOptions options = new PooledHiLoSequenceOptions(50, entityType.getSimpleName());
					ConnectionProvider connectionProvider = connectionConfiguration.getConnectionProvider();
					if (!(connectionProvider instanceof SeparateTransactionExecutor)) {
						throw new MappingConfigurationException("Before-insert identifier policy configured with connection that doesn't support separate transaction,"
								+ " please provide a " + Reflections.toString(SeparateTransactionExecutor.class) + " as connection provider or change identifier policy");
					}
					sequence = (Sequence<I>) new PooledHiLoSequence(options,
							new PooledHiLoSequencePersister(((PooledHiLoSequenceIdentifierPolicySupport) identifierPolicy).getStorageOptions(), dialect, (SeparateTransactionExecutor) connectionProvider, connectionConfiguration.getBatchSize()));
				} else if (identifierPolicy instanceof DatabaseSequenceIdentifierPolicySupport) {
					Class<C> entityType = identification.getIdentificationDefiner().getEntityType();
					DatabaseSequenceIdentifierPolicySupport databaseSequenceSupport = (DatabaseSequenceIdentifierPolicySupport) identifierPolicy;
					String sequenceName = databaseSequenceSupport.getDatabaseSequenceNamingStrategy().giveName(entityType);
					Database database = new Database();
					Schema sequenceSchema = nullable(databaseSequenceSupport.getDatabaseSequenceSettings().getSchemaName())
							.map(s -> database.new Schema(s))
							.elseSet(() -> first(mappingPerTable.getMappings()).getTargetTable().getSchema())
							.get();
					org.codefilarete.stalactite.sql.ddl.structure.Sequence databaseSequence
							= new org.codefilarete.stalactite.sql.ddl.structure.Sequence(sequenceSchema, sequenceName)
							.withBatchSize(databaseSequenceSupport.getDatabaseSequenceSettings().getBatchSize())
							.withInitialValue(databaseSequenceSupport.getDatabaseSequenceSettings().getInitialValue());
					sequence = (Sequence<I>) dialect.getDatabaseSequenceSelectorFactory().create(databaseSequence, connectionConfiguration.getConnectionProvider());
				} else if (identifierPolicy instanceof BeforeInsertIdentifierPolicySupport) {
					sequence = ((BeforeInsertIdentifierPolicySupport<I>) identifierPolicy).getSequence();
				} else {
					throw new MappingConfigurationException("Before-insert identifier policy " + Reflections.toString(identifierPolicy.getClass()) + " is not supported");
				}
				identifierInsertionManager = new BeforeInsertIdentifierManager<>(new AccessorWrapperIdAccessor<>(idAccessor), sequence, identifierType);
			} else if (identifierPolicy instanceof AlreadyAssignedIdentifierPolicy) {
				AlreadyAssignedIdentifierPolicy<C, I> alreadyAssignedPolicy = (AlreadyAssignedIdentifierPolicy<C, I>) identifierPolicy;
				identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(
						identifierType,
						alreadyAssignedPolicy.getMarkAsPersistedFunction(),
						alreadyAssignedPolicy.getIsPersistedFunction());
			}
		}
		
		// Treating configurations that are not the identifying one (for child-class) : they get an already-assigned identifier manager
		AlreadyAssignedIdentifierManager<C, I> fallbackMappingIdentifierManager = determineFallbackIdentifierManager(idAccessor, identifierInsertionManager, identifierType);
		identification
				.setInsertionManager(identifierInsertionManager)
				.setFallbackInsertionManager(fallbackMappingIdentifierManager);
	}
	
	private <E> AlreadyAssignedIdentifierManager<E, I> determineFallbackIdentifierManager(ReversibleAccessor<E, I> idAccessor,
																						  IdentifierInsertionManager<E, I> identifierInsertionManager,
																						  Class<I> identifierType) {
		AlreadyAssignedIdentifierManager<E, I> fallbackMappingIdentifierManager;
		if (identifierInsertionManager instanceof AlreadyAssignedIdentifierManager) {
			fallbackMappingIdentifierManager = new AlreadyAssignedIdentifierManager<>(identifierType,
					((AlreadyAssignedIdentifierManager<E, I>) identifierInsertionManager).getMarkAsPersistedFunction(),
					((AlreadyAssignedIdentifierManager<E, I>) identifierInsertionManager).getIsPersistedFunction());
		} else {
			// auto-increment, sequence, etc : non-identifying classes get an already-assigned identifier manager based on their default value
			Function<E, Boolean> isPersistedFunction = identifierType.isPrimitive()
					? c -> PRIMITIVE_DEFAULT_VALUES.get(identifierType) == idAccessor.get(c)
					: c -> idAccessor.get(c) != null;
			fallbackMappingIdentifierManager = new AlreadyAssignedIdentifierManager<>(identifierType, c -> {
			}, isPersistedFunction);
		}
		return fallbackMappingIdentifierManager;
	}
}
