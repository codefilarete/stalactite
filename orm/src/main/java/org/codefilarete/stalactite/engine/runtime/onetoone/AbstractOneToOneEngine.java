package org.codefilarete.stalactite.engine.runtime.onetoone;

import java.util.Map;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.engine.configurer.onetoone.OrphanRemovalOnUpdate;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class AbstractOneToOneEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> {
	
	protected final ConfiguredPersister<SRC, SRCID> sourcePersister;
	
	protected final ConfiguredPersister<TRGT, TRGTID> targetPersister;
	
	protected final Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> keyColumnsMapping;
	
	protected final Accessor<SRC, TRGT> targetAccessor;
	
	public AbstractOneToOneEngine(ConfiguredPersister<SRC, SRCID> sourcePersister,
								  ConfiguredPersister<TRGT, TRGTID> targetPersister,
								  Accessor<SRC, TRGT> targetAccessor,
								  Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> keyColumnsMapping) {
		this.sourcePersister = sourcePersister;
		this.targetPersister = targetPersister;
		this.keyColumnsMapping = keyColumnsMapping;
		this.targetAccessor = targetAccessor;
	}
	
	public void addInsertCascade() {
	}
	
	public void addUpdateCascade(boolean orphanRemoval) {
		if (orphanRemoval) {
			sourcePersister.addUpdateListener(new OrphanRemovalOnUpdate<>(targetPersister, targetAccessor));
		}
	}
	
	public void addDeleteCascade(boolean orphanRemoval) {
	}
}
