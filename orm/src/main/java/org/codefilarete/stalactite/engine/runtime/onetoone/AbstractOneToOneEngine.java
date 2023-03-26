package org.codefilarete.stalactite.engine.runtime.onetoone;

import java.util.Map;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.engine.configurer.onetoone.OrphanRemovalOnUpdate;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class AbstractOneToOneEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> {
	
	protected final EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister;
	
	protected final EntityConfiguredPersister<TRGT, TRGTID> targetPersister;
	
	protected final Map<Column<LEFTTABLE, Object>, Column<RIGHTTABLE, Object>> keyColumnsMapping;
	
	protected final Accessor<SRC, TRGT> targetAccessor;
	
	public AbstractOneToOneEngine(EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister,
								  EntityConfiguredPersister<TRGT, TRGTID> targetPersister,
								  Accessor<SRC, TRGT> targetAccessor,
								  Map<Column<LEFTTABLE, Object>, Column<RIGHTTABLE, Object>> keyColumnsMapping) {
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
