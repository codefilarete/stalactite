package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.Map;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.configurer.resolver.OrphanRemovalOnUpdate;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class AbstractOneToOneEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> {
	
	protected final EntityWriteExecutor<SRC, SRCID> sourcePersister;
	
	protected final EntityReadWriteExecutor<TRGT, TRGTID> targetPersister;
	
	protected final Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> keyColumnsMapping;
	
	protected final Accessor<SRC, TRGT> targetAccessor;
	
	public AbstractOneToOneEngine(EntityWriteExecutor<SRC, SRCID> sourcePersister,
	                              EntityReadWriteExecutor<TRGT, TRGTID> targetPersister,
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
