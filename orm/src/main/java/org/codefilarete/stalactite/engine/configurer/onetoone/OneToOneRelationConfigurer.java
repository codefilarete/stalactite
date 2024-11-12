package org.codefilarete.stalactite.engine.configurer.onetoone;

import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <SRCID> identifier type of source entities
 * @param <TRGTID>> identifier type of target entities
 * @author Guillaume Mary
 */
public class OneToOneRelationConfigurer<SRC, TRGT, SRCID, TRGTID> {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private final OneToOneRelation<SRC, TRGT, TRGTID> oneToOneRelation;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final OneToOneConfigurerTemplate configurer;
	
	public OneToOneRelationConfigurer(OneToOneRelation<SRC, TRGT, TRGTID> oneToOneRelation,
									  ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									  Dialect dialect,
									  ConnectionConfiguration connectionConfiguration,
									  ForeignKeyNamingStrategy foreignKeyNamingStrategy,
									  JoinColumnNamingStrategy joinColumnNamingStrategy) {
		this.oneToOneRelation = oneToOneRelation;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		if (oneToOneRelation.isRelationOwnedByTarget()) {
			this.configurer = new OneToOneOwnedByTargetConfigurer<>(sourcePersister, oneToOneRelation, joinColumnNamingStrategy, foreignKeyNamingStrategy, dialect, connectionConfiguration);
		} else {
			this.configurer = new OneToOneOwnedBySourceConfigurer<>(sourcePersister, oneToOneRelation, joinColumnNamingStrategy, foreignKeyNamingStrategy);
		}
	}
	
	public ForeignKeyNamingStrategy getForeignKeyNamingStrategy() {
		return foreignKeyNamingStrategy;
	}
	
	public JoinColumnNamingStrategy getJoinColumnNamingStrategy() {
		return joinColumnNamingStrategy;
	}
	
	public void configure(String tableAlias,
						  PersisterBuilderImpl<TRGT, TRGTID> targetPersisterBuilder,
						  boolean loadSeparately) {
		ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = targetPersisterBuilder
				// please note that even if no table is found in configuration, build(..) will create one
				.build(dialect, connectionConfiguration,
						nullable(oneToOneRelation.getTargetTable()).getOr(nullable(oneToOneRelation.getReverseColumn()).map(Column::getTable).get()));
		this.configurer.configure(tableAlias, targetPersister, loadSeparately);
	}
	
	public CascadeConfigurationResult<SRC, TRGT> configureWithSelectIn2Phases(
			String tableAlias,
			ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
			FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		return this.configurer.configureWithSelectIn2Phases(tableAlias, targetPersister, firstPhaseCycleLoadListener);
	}
	
	/**
	 * Object invoked on row read
	 * @param <SRC>
	 * @param <TRGTID>
	 */
	@FunctionalInterface
	public interface FirstPhaseCycleLoadListener<SRC, TRGTID> {
		
		void onFirstPhaseRowRead(SRC src, TRGTID targetId);
		
	}
}
