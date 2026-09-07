package org.codefilarete.stalactite.query.api;

import javax.annotation.Nullable;

import org.codefilarete.stalactite.query.model.From;
import org.codefilarete.stalactite.sql.ddl.Size;

/**
 * A {@link Selectable} qualified by the table/query it belongs to ({@link #getOwner()})
 * Thus, it can be used in a join clause, because, as a difference with {@link Selectable}, knowing its owner is
 * required to build a join clause (see {@link JoinChain}), while {@link #getJavaType()} enforces type compatibility
 * between joined elements.
 * 
 * @param <T> owner type, expected to appear in a {@link From}
 * @param <O> Java type of the joined element, used to check joined elements compatibility
 * @author Guillaume Mary
 */
public interface QualifiedSelectable<T extends Fromable, O> extends Selectable<O> {
	
	T getOwner();
	
	default String getName() {
		return getExpression();
	}
	
	/** Optional information, used only for schema generation */
	@Nullable
	Size getSize();
	
}
