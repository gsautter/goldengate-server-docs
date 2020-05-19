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
package de.uka.ipd.idaho.goldenGateServer.ats;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.TokenSequenceUtils;
import de.uka.ipd.idaho.gamta.util.AttributeMapAnnotation;
import de.uka.ipd.idaho.gamta.util.gPath.GPath;
import de.uka.ipd.idaho.gamta.util.gPath.GPathExpression;
import de.uka.ipd.idaho.gamta.util.gPath.GPathParser;
import de.uka.ipd.idaho.goldenGateServer.dio.connectors.GoldenGateDioEXP;
import de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringRelation;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * The GoldenGATE Annotation Thesaurus Server (ATS) provides specific
 * annotations of documents in a collection hosted by a GoldenGATE DIO in the
 * fashion of lists. Nested annotations are not stored, only attributes and
 * value, plus the source document ID, i.e., the ID of the document an
 * annotation was extracted from. The ATS offers annotations by their type,
 * optionally filtered trough a GPath predicate.<br/>
 * This functionality can (theoretically) serve as sort of an indexing service
 * for searching documents, but this is not the purpose of this component, which
 * is not this strongly optimized for retrieval speed. The major purpose to use
 * ATS is providing gazetteers and training data for creating new extraction
 * components.<br/>
 * The favorite way of accessing the data in an ATS is using a
 * GoldenGateAtsClient.<br/>
 * For convenience of implementation, and to relive users from waiting on upload
 * results, this component is an exporter, even though exports go to the local
 * file system.
 * 
 * @author sautter
 */
public class GoldenGateATS extends GoldenGateEXP implements GoldenGateAtsConstants {
	
	/* list of hosted annotation types */
	private StringVector annotationTypes = new StringVector();
	
	/*
	 * list for preventing storing structural annotations (too large, would
	 * replicate complete documents several times if respective annotations form
	 * an overlay or are close to doing so)
	 */
	private StringVector ignoreAnnotationTypes = new StringVector(); 
	
	/** Constructor passing 'ATS' as the letter code to super constructor
	 */
	public GoldenGateATS() {
		super("ATS", "AnnoThesaurus");
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateScf.AbstractServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	load existing annotation types
		File[] typeFolders = this.dataPath.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return file.isDirectory();
			}
		});
		for (int f = 0; f < typeFolders.length; f++)
			this.annotationTypes.addElement(typeFolders[f].getName().replaceAll("\\+", ":"));
		
		//	load annotation types to ignore
		try {
			StringVector iats = StringVector.loadList(new File(this.dataPath, "ignoreTypes.cnfg"));
			for (int t = 0; t < iats.size(); t++) {
				String iat = iats.get(t).trim();
				if ((iat.length() == 0) || iat.startsWith("//"))
					continue;
				this.ignoreAnnotationTypes.addElementIgnoreDuplicates(iat);
			}
		}
		catch (IOException ioe) {
			System.out.println("GoldenGateATS: error loading ignorable annotation types: " + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}
		
		//	initialize super class
		super.initComponent();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#getBinding()
	 */
	protected GoldenGateExpBinding getBinding() {
		return new GoldenGateDioEXP(this);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doUpdate(de.uka.ipd.idaho.gamta.QueriableAnnotation, java.util.Properties)
	 */
	protected void doUpdate(QueriableAnnotation doc, Properties docAttributes) throws IOException {
		
		//	get document ID
		String docId = ((String) doc.getAttribute(DOCUMENT_ID_ATTRIBUTE));
		if (docId == null)
			return;
		
		//	get annotation types
		String[] types = doc.getAnnotationTypes();
		for (int t = 0; t < types.length; t++) {
			
			//	this one's not ours
			if (this.ignoreAnnotationTypes.containsIgnoreCase(types[t]))
				continue;
			
			//	remember type
			this.annotationTypes.addElementIgnoreDuplicates(types[t]);
			
			//	load existing file
			File typeDocFile = this.getFile(types[t], docId, true);
			Reader in = new BufferedReader(new InputStreamReader(new FileInputStream(typeDocFile), "UTF-8"));
			StringRelation typeDocData = StringRelation.readCsvData(in, CSV_DELIMITER, true, null);
			in.close();
			
			//	index what we got
			HashMap removeTupels = new HashMap();
			for (int d = 0; d < typeDocData.size(); d++) {
				if (docId.equals(typeDocData.get(d).getValue(SOURCE_DOCUMENT_ID)))
					removeTupels.put(typeDocData.get(d), new Integer(d));
			}
			
			//	add new annotation
			Annotation[] annotations = doc.getAnnotations(types[t]);
			HashSet addTupels = new HashSet();
			for (int d = 0; d < annotations.length; d++) {
				StringTupel typeDocDataTupel = new StringTupel();
				typeDocDataTupel.setValue(SOURCE_DOCUMENT_ID, docId);
				typeDocDataTupel.setValue(Annotation.ANNOTATION_VALUE_ATTRIBUTE, TokenSequenceUtils.concatTokens(annotations[d], true, true));
				String[] ans = annotations[d].getAttributeNames();
				for (int a = 0; a < ans.length; a++) {
					Object value = annotations[d].getAttribute(ans[a]);
					if ((value != null) && (value instanceof CharSequence))
						typeDocDataTupel.setValue(ans[a], value.toString());
				}
				
				//	we've seen this one before, simply retain it
				if (removeTupels.containsKey(typeDocDataTupel))
					removeTupels.remove(typeDocDataTupel);
				
				//	this one's new
				else addTupels.add(typeDocDataTupel);
			}
			
			//	nothing changed, we're done
			if ((removeTupels.size() + addTupels.size()) == 0)
				continue;
			
			//	remove what we have to
			if (removeTupels.size() != 0) {
				ArrayList removeIndices = new ArrayList(removeTupels.values());
				Collections.sort(removeIndices);
				Collections.reverse(removeIndices);
				for (Iterator it = removeIndices.iterator(); it.hasNext();)
					typeDocData.remove(((Integer) it.next()).intValue());
			}
			
			//	add what's new
			for (Iterator it = addTupels.iterator(); it.hasNext();)
				typeDocData.addElement((StringTupel) it.next());
			
			//	clean up empty file
			if (typeDocData.size() == 0)
				typeDocFile.delete();
			
			//	store what we got
			else {
				Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(typeDocFile, true), "UTF-8"));
				StringRelation.writeCsvData(out, typeDocData, CSV_DELIMITER, true);
				out.flush();
				out.close();
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doDelete(java.lang.String, java.util.Properties)
	 */
	protected void doDelete(String docId, Properties docAttributes) throws IOException {
		String[] types = this.annotationTypes.toStringArray();
		for (int t = 0; t < types.length; t++) {
			File typeDocFile = this.getFile(types[t], docId, false);
			if (typeDocFile == null)
				continue;
			
			//	load existing file
			Reader in = new BufferedReader(new InputStreamReader(new FileInputStream(typeDocFile), "UTF-8"));
			StringRelation typeDocData = StringRelation.readCsvData(in, CSV_DELIMITER, true, null);
			in.close();
			
			//	check if anything changed
			boolean unmodified = true;
			for (int d = 0; d < typeDocData.size(); d++)
				if (docId.equals(typeDocData.get(d).getValue(SOURCE_DOCUMENT_ID))) {
					typeDocData.remove(d--);
					unmodified = false;
				}
			
			//	nothing changed, we're done
			if (unmodified)
				continue;
			
			//	clean up empty file
			if (typeDocData.size() == 0)
				typeDocFile.delete();
			
			//	store what we got
			else {
				Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(typeDocFile, true), "UTF-8"));
				StringRelation.writeCsvData(out, typeDocData, CSV_DELIMITER, true);
				out.flush();
				out.close();
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(super.getActions()));
		ComponentAction ca;
		
		//	request for annotation types
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_ANNOTATION_TYPES;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	indicate types coming
				output.write(GET_ANNOTATION_TYPES);
				output.newLine();
				
				//	write types
				String[] types = annotationTypes.toStringArray();
				for (int t = 0; t < types.length; t++) {
					output.write(types[t]);
					output.newLine();
				}
			}
		};
		cal.add(ca);
		
		//	request for annotations
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_ANNOTATIONS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	read parameters
				String type = input.readLine();
				String predicate = input.readLine().trim();
				
				//	parse predicate (if any)
				GPathExpression gpe = null;
				if (predicate.length() != 0) try {
					gpe = GPathParser.parseExpression(predicate);
				}
				catch (Exception e) {
					output.write(e.getMessage());
					output.newLine();
					return;
				}
				
				//	get relevant files, and prepare to filter out duplicates
				File[] typeDataFiles = getFiles(type);
				HashSet alreadySeen = new HashSet();
				
				//	indicate annotations coming
				output.write(GET_ANNOTATIONS);
				output.newLine();
				
				//	filter and send files one by one (we can work line wise, as we don't have to expect line broken content)
				for (int f = 0; f < typeDataFiles.length; f++) {
					
					//	load file
					Reader in = new BufferedReader(new InputStreamReader(new FileInputStream(typeDataFiles[f]), "UTF-8"));
					StringRelation typeDocData = StringRelation.readCsvData(in, CSV_DELIMITER, true, null);
					in.close();
					
					//	get keys for current file
					StringVector keyList = typeDocData.getKeys();
					keyList.remove(SOURCE_DOCUMENT_ID);
					keyList.remove(Annotation.ANNOTATION_VALUE_ATTRIBUTE);
					String[] keys = keyList.toStringArray();
					
					//	filter out duplicates
					for (int d = 0; d < typeDocData.size(); d++) {
						
						//	wrap current tupel
						StringTupel tdd = typeDocData.get(d);
						QueriableAnnotation dat = new AttributeMapAnnotation(type, tdd, keys, tdd.getValue(Annotation.ANNOTATION_VALUE_ATTRIBUTE), null);
						
						//	filter out duplicates
						if (!alreadySeen.add(dat.toXML()))
							typeDocData.remove(d--);
						
						//	apply filter predicate if given
						else if ((gpe != null) && !GPath.evaluateExpression(gpe, dat, null).asBoolean().value)
							typeDocData.remove(d--);
					}
					
					//	anything left?
					if (typeDocData.size() == 0)
						continue;
					
					//	send data
					StringRelation.writeCsvData(output, typeDocData, CSV_DELIMITER, true);
					
					//	add line break to indicate to client the end of the current file, and potentially a new file coming with new keys
					output.newLine();
				}
			}
		};
		cal.add(ca);
		
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private File getFile(String type, String docId, boolean create) throws IOException {
		
		//	get or create annotation type folder
		File typeFolder = new File(this.dataPath, type.replaceAll("\\:", "+"));
		if (!typeFolder.exists()) {
			if (create)
				typeFolder.mkdir();
			else return null;
		}
		
		//	get or create data file for current document (3 chars / 12 bits of random should be enough for now)
		File docDataFile = new File(typeFolder, ("at-" + docId.substring(0, 3) + ".csv"));
		try {
			if (!docDataFile.exists()) {
				if (create)
					docDataFile.createNewFile();
				else return null;
			}
		}
		catch (IOException ioe) {
			this.logError("GoldenGateATS: error creating file '" + docDataFile.getAbsolutePath() + "': " + ioe.getMessage());
			this.logError(ioe);
			throw ioe;
		}
		
		//	return data file
		return docDataFile;
	}
	
	private File[] getFiles(String type) {
		
		//	get or create annotation type folder
		File typeFolder = new File(this.dataPath, type.replaceAll("\\:", "+"));
		
		//	list data files
		if (typeFolder.exists())
			return typeFolder.listFiles(new FileFilter() {
				public boolean accept(File file) {
					return (file.isFile() && file.getName().startsWith("at-") && file.getName().endsWith(".csv"));
				}
			});
		
		//	nothing we have
		else return new File[0];
	}
}