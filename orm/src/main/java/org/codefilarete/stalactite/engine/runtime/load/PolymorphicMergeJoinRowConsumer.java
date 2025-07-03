package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Reflections;

/**
 * 
 * @param <D>
 * @param <I>
 * @author Guillaume Mary
 */
public class PolymorphicMergeJoinRowConsumer<D, I> extends MergeJoinRowConsumer<D> {

	private final Function<ColumnedRow, I> identifierProvider;
	
	public PolymorphicMergeJoinRowConsumer(MergeJoinNode<D, ?, ?, ?> node, EntityMapping<D, I, ?> entityInflater) {
		super(node, entityInflater.getRowTransformer());
		this.identifierProvider = row -> entityInflater.getIdMapping().getIdentifierAssembler().assemble(row);
	}
	
	@Nullable
	public I giveIdentifier(ColumnedRow row) {
		return identifierProvider.apply(row);
	}
	
	public D transform(ColumnedRow row) {
		return super.merger.transform(row);
	}
	
	/**
	 * Implemented for debug. DO NOT RELY ON IT for anything else.
	 */
	@Override
	public String toString() {
		return Reflections.toString(this.getClass());
	}
}
