/*
 * Copyright (c) 2006-, IPD Boehm, Universitaet Karlsruhe (TH) / KIT, by Guido Sautter
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Universität Karlsruhe (TH) nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY UNIVERSITÄT KARLSRUHE (TH) / KIT AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package de.uka.ipd.idaho.goldenGateServer.srs;

import java.io.File;

import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentHost;

/**
 * Generic abstract subperclass for for the plugin classes, namely
 * StorageFilter, DocumentSplitter, and Indexer. This class provides the
 * plugin's data path and host. The default implementations of init() and exit()
 * do nothing, sub classes are welcome to overwrite tham as needed.
 * 
 * @author sautter
 */
public abstract class AbstractGoldenGateSrsPlugin implements GoldenGateSrsPlugin {
	
	/** the plugin's host, providing access to the shared database */
	protected GoldenGateServerComponentHost host;
	
	/** the plugin's data path, the folder the plugin's data is located in */
	protected File dataPath;
	
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.GoldenGateSrsPlugin#setDataPath(java.io.File)
	 */
	public void setDataPath(File dataPath) {
		this.dataPath = dataPath;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsPlugin#setHost(de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentHost)
	 */
	public void setHost(GoldenGateServerComponentHost host) {
		this.host = host;
	}

	/* (non-Javadoc)
	 * @see de.goldenGateSrs.GoldenGateSrsPlugin#init()
	 */
	public void init() {}

	/* (non-Javadoc)
	 * @see de.goldenGateSrs.GoldenGateSrsPlugin#exit()
	 */
	public void exit() {}
}
