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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.TokenSequenceUtils;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDIO;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent.DioDocumentEventListener;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringRelation;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * The GoldenGATE Annotation Thesaurus Server (ATS) provides specific
 * annotations of document in a collection hosted by a GoldenGATE DIO in the
 * fashion of lists. Nested annotations are not stored, only attributes and
 * value, plus the source document ID, i.e., the ID of the document an
 * annotation was extracted from. The ATS offers annotations by their type,
 * optionally filtered trough a GPath predicate.
 * This functionality can (theoretically) serve as sort of an indexing service
 * for searching documents, but this is not the purpose of this component, which
 * is not this strongly optimized for retrieval speed. The major purpose of ATS
 * is providing gazetteers and training data for creating new extraction
 * components.
 * The favorite way of accessing the data in an ATS is using a
 * GoldenGateAtsClient.
 * 
 * @author sautter
 */
public class GoldenGateATS extends AbstractGoldenGateServerComponent implements GoldenGateAtsConstants {
	
	private GoldenGateDIO dio = null;
	
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
		super("ATS");
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
			this.ignoreAnnotationTypes = StringVector.loadList(new File(this.dataPath, "ignoreTypes.txt"));
		}
		catch (IOException ioe) {
			System.out.println("GoldenGateATS: error loading ignorable annotation types: " + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		
		//	get document IO server
		this.dio = ((GoldenGateDIO) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateDIO.class.getName()));
		
		//	check success
		if (this.dio == null) throw new RuntimeException(GoldenGateDIO.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		
		//	listen to changes
		this.dio.addDocumentEventListener(new DioDocumentEventListener() {
			
			/* (non-Javadoc)
			 * @see de.uka.ipd.idaho.goldenGateServer.dio.DioDocumentStorageListener#documentCheckedOut(de.uka.ipd.idaho.goldenGateServer.dst.DocumentStorageEvent)
			 */
			public void documentCheckedOut(DioDocumentEvent dse) {}
			
			/* (non-Javadoc)
			 * @see de.goldenGateScf.dss.notification.DocumentStorageListener#documentDeleted(de.goldenGateScf.dss.notification.DocumentStorageEvent)
			 */
			public void documentDeleted(DioDocumentEvent dse) {
				try {
					deleteDocument(dse.documentId);
				}
				catch (IOException ioe) {
					System.out.println("GoldenGateATS: error deleting document '" + dse.documentId + "' - " + ioe.getMessage());
					ioe.printStackTrace(System.out);
				}
			}
			
			/* (non-Javadoc)
			 * @see de.goldenGateScf.dss.notification.DocumentStorageListener#documentUpdated(de.goldenGateScf.dss.notification.DocumentStorageEvent)
			 */
			public void documentUpdated(final DioDocumentEvent dse) {
				try {
					storeDocument(dse.documentId, dse.document);
				}
				catch (IOException ioe) {
					System.out.println("GoldenGateATS: error storing document '" + dse.documentId + "' - " + ioe.getMessage());
					ioe.printStackTrace(System.out);
				}
			}
			
			/* (non-Javadoc)
			 * @see de.uka.ipd.idaho.goldenGateServer.dio.DioDocumentStorageListener#documentReleased(de.uka.ipd.idaho.goldenGateServer.dst.DocumentStorageEvent)
			 */
			public void documentReleased(DioDocumentEvent dse) {}
		});
	}

	/* (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList();
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
//				String predicate = input.readLine(); // TODO: enable predicate filtering
//				
//				//	parse predicate (if any)
//				GPathExpression gpe = null;
//				if (predicate.length() != 0)
//					try {
//						gpe = GPathParser.parseExpression(predicate);
//					}
//					catch (Exception e) {
//						output.write(e.getMessage());
//						output.newLine();
//						return;
//					}
				
				
				File[] typeDataFiles = getFiles(type);
				StringRelation typeData = new StringRelation();
				for (int f = 0; f < typeDataFiles.length; f++) {
					Reader in = new BufferedReader(new InputStreamReader(new FileInputStream(typeDataFiles[f]), "UTF-8"));
					StringRelation typeDocData = StringRelation.readCsvData(in, CSV_DELIMITER, true, null);
					in.close();
					typeData = typeData.union(typeDocData);
				}
				
				//	indicate annotations coming
				output.write(GET_ANNOTATIONS);
				output.newLine();
				
				//	write annotations
				StringRelation.writeCsvData(output, typeData, CSV_DELIMITER, true);
			}
		};
		cal.add(ca);
		
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	public void storeDocument(String docId, QueriableAnnotation doc) throws IOException {
		String[] types = doc.getAnnotationTypes();
		for (int t = 0; t < types.length; t++) {
			if (!this.ignoreAnnotationTypes.contains(types[t])) {
				this.annotationTypes.addElementIgnoreDuplicates(types[t]);
				File typeDocFile = this.getFile(types[t], docId, true);
				Reader in = new BufferedReader(new InputStreamReader(new FileInputStream(typeDocFile), "UTF-8"));
				StringRelation typeDocData = StringRelation.readCsvData(in, CSV_DELIMITER, true, null);
				in.close();
				
				HashMap removeTupels = new HashMap();
				for (int d = 0; d < typeDocData.size(); d++) {
					if (docId.equals(typeDocData.get(d).getValue(SOURCE_DOCUMENT_ID)))
						removeTupels.put(typeDocData.get(d), new Integer(d));
				}
				
				Annotation[] annotations = doc.getAnnotations(types[t]);
				HashSet addTupels = new HashSet();
				for (int d = 0; d < annotations.length; d++) {
					StringTupel typeDocDataTupel = new StringTupel();
					typeDocDataTupel.setValue(SOURCE_DOCUMENT_ID, docId);
					typeDocDataTupel.setValue(Annotation.START_INDEX_ATTRIBUTE, ("" + annotations[d].getStartIndex()));
					typeDocDataTupel.setValue(Annotation.SIZE_ATTRIBUTE, ("" + annotations[d].size()));
					typeDocDataTupel.setValue(Annotation.ANNOTATION_VALUE_ATTRIBUTE, TokenSequenceUtils.concatTokens(annotations[d], true, true));
					String[] ans = annotations[d].getAttributeNames();
					for (int a = 0; a < ans.length; a++) {
						Object value = annotations[d].getAttribute(ans[a]);
						if ((value != null) && (value instanceof CharSequence))
							typeDocDataTupel.setValue(ans[a], value.toString());
					}
					
					if (removeTupels.containsKey(typeDocDataTupel)) removeTupels.remove(typeDocDataTupel); // data tupel present, dequeue it from remove list
					else addTupels.add(typeDocDataTupel); // add tupel to insert queue otherwise
				}
				if ((removeTupels.size() + addTupels.size()) != 0) {
					ArrayList removeIndices = new ArrayList(removeTupels.values());
					Collections.sort(removeIndices);
					Collections.reverse(removeIndices);
					Iterator it = removeIndices.iterator();
					while (it.hasNext())
						typeDocData.remove(((Integer) it.next()).intValue());
					
					it = addTupels.iterator();
					while (it.hasNext())
						typeDocData.addElement((StringTupel) it.next());
					
					typeDocFile.delete();
					if (typeDocData.size() != 0) {
						typeDocFile = this.getFile(types[t], docId, true);
						Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(typeDocFile, true), "UTF-8"));
						StringRelation.writeCsvData(out, typeDocData, CSV_DELIMITER, true);
						out.flush();
						out.close();
					}
				}
			}
		}
	}
	
	public void deleteDocument(String docId) throws IOException {
		String[] types = this.annotationTypes.toStringArray();
		for (int t = 0; t < types.length; t++) {
			File typeDocFile = this.getFile(types[t], docId, false);
			if (typeDocFile != null) {
				Reader in = new BufferedReader(new InputStreamReader(new FileInputStream(typeDocFile), "UTF-8"));
				StringRelation typeDocData = StringRelation.readCsvData(in, CSV_DELIMITER, true, null);
				in.close();
				boolean modified = false;
				for (int d = 0; d < typeDocData.size(); d++) {
					if (docId.equals(typeDocData.get(d).getValue(SOURCE_DOCUMENT_ID))) {
						typeDocData.remove(d--);
						modified = true;
					}
				}
				if (modified) {
					typeDocFile.delete();
					if (typeDocData.size() != 0) {
						typeDocFile = this.getFile(types[t], docId, true);
						Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(typeDocFile, true), "UTF-8"));
						StringRelation.writeCsvData(out, typeDocData, CSV_DELIMITER, true);
						out.flush();
						out.close();
					}
				}
			}
		}
	}
	
	private File getFile(String type, String docId, boolean create) throws IOException {
		File typeFolder = new File(this.dataPath, type.replaceAll("\\:", "+"));
		if (!typeFolder.exists()) {
			if (create) typeFolder.mkdir();
			else return null;
		}
		File docIdFile = new File(typeFolder, ("at-" + docId.substring(0, 3) + ".csv"));
		try {
			if (!docIdFile.exists()) {
				if (create) docIdFile.createNewFile();
				else return null;
			}
		}
		catch (IOException ioe) {
			System.out.println("GoldenGateATS: error creating file '" + docIdFile.getAbsolutePath() + "': " + ioe.getMessage());
			ioe.printStackTrace(System.out);
			throw ioe;
		}
		return docIdFile;
	}
	
	private File[] getFiles(String type) {
		File typeFolder = new File(this.dataPath, type.replaceAll("\\:", "+"));
		if (typeFolder.exists()) return typeFolder.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return (file.isFile() && file.getName().startsWith("at-") && file.getName().endsWith(".csv"));
			}
		});
		else return new File[0];
	}
}
