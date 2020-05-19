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
 *     * Neither the name of the Universitaet Karlsruhe (TH) nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY UNIVERSITAET KARLSRUHE (TH) / KIT AND CONTRIBUTORS 
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
package de.uka.ipd.idaho.goldenGateServer.srs.filters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.gPath.GPath;
import de.uka.ipd.idaho.gamta.util.gPath.exceptions.GPathException;
import de.uka.ipd.idaho.goldenGateServer.srs.AbstractStorageFilter;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * The XPathFilter filters documents or parts of them using XPath expressions.
 * These expressions can specify both required conditions and prohibited
 * conditions. The behavior of an XPath filter can be controlled using two
 * parameters (to be specified in the 'config.cnfg' file in the filters data
 * path) and a list of XPath expressions (to be specified in the file
 * 'useXPath.cnfg' in the filters data path, as a list of the names of the files
 * containing the actual XPath expressions to use, see example below). -
 * filterDocuments: If set to any non-null value, the filter will validate
 * documents as a whole using the filter(QueriableAnnotation) method. This is
 * recommended when no DocumentSplitter is in use. - filterDocumentParts:
 * If set to any non-null value, the filter will validate document parts after
 * splitting using the filter(QueriableAnnotation[], QueriableAnnotation)
 * method. This is recommended when one or more DocumentSplitters are in use.
 * 
 * --- 'useXPath.cnfg' ---
 * ERROR: someXPath.xPath.txt &lt;-- the XPath expression in
 * 'someXPath.xPath.txt' identifies erroneous markup. A document passes the
 * filter only if this expression does not return any result nodes.
 * REQUIRED: anotherXPath.xPath.txt &lt;-- the XPath expression in
 * 'anotherXPath.xPath.txt' identifies essential parts of the markup. A document
 * passes the filter only if this expression returns at least one result node.
 * ...
 * ERROR: ...
 * REQUIRED: ...
 * ...
 * REQUIRED: ...
 * ...
 * --- EOF 'useXPath.cnfg' ---
 * This is more complex than just providing a list of XPath expressions in one
 * file, but on the other hand makes it easier to put individual expressions in
 * and out of use without deleting them. It's just adding or removing the
 * respective file name from useXPath.cnfg.
 * 
 * @author sautter
 */
public class XPathFilter extends AbstractStorageFilter {

	private boolean filterDocuments = false;
	private boolean filterDocumentParts = false;
	
	private String[] errorPaths = new String[0];
	private String[] requiredPaths = new String[0];
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.AbstractStorageFilter#init()
	 */
	public void init() {
		Settings set = Settings.loadSettings(new File(this.dataPath, "config.cnfg"));
		
		//	get behavioral configuration
		this.filterDocuments = set.containsKey("filterDocuments");
		this.filterDocumentParts = set.containsKey("filterDocumentParts");
		
		//	load XPath names
		try {
			StringVector useXPaths = StringVector.loadList(new File(this.dataPath, "useXPath.cnfg"));
			StringVector errorPaths = new StringVector();
			StringVector requiredPaths = new StringVector();
			for (int p = 0; p < useXPaths.size(); p++) {
				String useXPath = useXPaths.get(p);
				if (useXPath.indexOf(':') != -1) {
					String use = useXPath.substring(0, useXPath.indexOf(':')).trim();
					String xPathFileName = useXPath.substring(useXPath.indexOf(':') + 1).trim();
					if ("ERROR".equals(use) || "REQUIRED".equals(use)) {
						try {
							File xPathFile = new File(this.dataPath, xPathFileName);
							String xPath = EasyIO.readFile(xPathFile);
							if ("ERROR".equals(use))
								errorPaths.addElementIgnoreDuplicates(xPath);
							if ("REQUIRED".equals(use))
								requiredPaths.addElementIgnoreDuplicates(xPath);
						}
						catch (IOException ioe) {
							System.out.println("Could not load XPath expression from '" + xPathFileName + "':" + ioe.getMessage());
							ioe.printStackTrace(System.out);
						}
					}
				}
			}
			this.errorPaths = errorPaths.toStringArray();
			this.requiredPaths = requiredPaths.toStringArray();
		}
		catch (IOException ioe) {
			System.out.println("Could not load XPath list:" + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.StorageFilter#filter(de.uka.ipd.idaho.gamta.QueriableAnnotation)
	 */
	public String filter(QueriableAnnotation doc) {
		return (this.filterDocuments ? this.validate(doc) : null);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.StorageFilter#filter(de.uka.ipd.idaho.gamta.QueriableAnnotation[], de.uka.ipd.idaho.gamta.QueriableAnnotation)
	 */
	public QueriableAnnotation[] filter(QueriableAnnotation[] parts, QueriableAnnotation doc) {
		if (this.filterDocumentParts) {
			ArrayList partList = new ArrayList();
			for (int p = 0; p < parts.length; p++) {
				String error = this.validate(parts[p]);
				if (error == null)
					partList.add(parts[p]);
			}
			return ((QueriableAnnotation[]) partList.toArray(new QueriableAnnotation[partList.size()]));
		}
		else return parts;
	}
	
	private String validate(QueriableAnnotation doc) {
		
		for (int e = 0; e < this.errorPaths.length; e++) {
			GPath path = this.getPath(this.errorPaths[e]);
			try {
				Annotation[] pathResult = GPath.evaluatePath(doc, path, null);
				if (pathResult.length != 0)
					return ("Error found: " + this.errorPaths[e]);
			}
			catch (GPathException gpe) {
				return gpe.getMessage();
			}
		}
		
		for (int r = 0; r < this.requiredPaths.length; r++) {
			GPath path = this.getPath(this.requiredPaths[r]);
			try {
				Annotation[] pathResult = GPath.evaluatePath(doc, path, null);
				if (pathResult.length == 0)
					return ("Requirement not met: " + this.requiredPaths[r]);
			}
			catch (GPathException gpe) {
				return gpe.getMessage();
			}
		}
		
		return null;
	}
	
	private HashMap paths = new HashMap();
	private GPath getPath(String path) {
		GPath gPath = ((GPath) this.paths.get(path));
		if (gPath == null) {
			gPath = new GPath(path);
			this.paths.put(path, gPath);
		}
		return gPath;
	}
}
