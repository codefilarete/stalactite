package org.codefilarete.stalactite.engine.model.book;

import org.codefilarete.stalactite.engine.model.device.Location;

public class ImprintPublisher extends Publisher {
	
	private Location printingWorkLocation;
	
	public Location getPrintingWorkLocation() {
		return printingWorkLocation;
	}
	
	public void setPrintingWorkLocation(Location printingWorkLocation) {
		this.printingWorkLocation = printingWorkLocation;
	}
}
