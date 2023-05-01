package org.codefilarete.stalactite.engine.configurer.onetoone;

import java.util.ArrayList;
import java.util.List;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.tool.Duo;

public class OrphanRemovalOnUpdate<SRC, TRGT> implements UpdateListener<SRC> {
	
	private final ConfiguredPersister<TRGT, ?> targetPersister;
	private final Accessor<SRC, TRGT> targetAccessor;
	private final Accessor<TRGT, ?> targetIdAccessor;
	
	public OrphanRemovalOnUpdate(ConfiguredPersister<TRGT, ?> targetPersister, Accessor<SRC, TRGT> targetAccessor) {
		this.targetPersister = targetPersister;
		this.targetAccessor = targetAccessor;
		this.targetIdAccessor = targetPersister.getMapping().getIdMapping().getIdAccessor()::getId;
	}
	
	@Override
	public void afterUpdate(Iterable<? extends Duo<? extends SRC, ? extends SRC>> payloads, boolean allColumnsStatement) {
		List<TRGT> targetsToDeleteUpdate = new ArrayList<>();
		payloads.forEach(duo -> {
			TRGT newTarget = getTarget(duo.getLeft());
			TRGT oldTarget = getTarget(duo.getRight());
			// nullified relations and changed ones must be removed (orphan removal)
			// TODO: one day we'll have to cover case of reused instance in same graph : one of the relation must handle it, not both,
			//  "else a marked instance" system must be implemented
			if (newTarget == null || (oldTarget != null && !targetIdAccessor.get(newTarget).equals(targetIdAccessor.get(oldTarget)))) {
				targetsToDeleteUpdate.add(oldTarget);
			}
		});
		targetPersister.delete(targetsToDeleteUpdate);
	}
	
	private TRGT getTarget(SRC src) {
		return src == null ? null : targetAccessor.get(src);
	}
}
