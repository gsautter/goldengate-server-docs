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
package de.uka.ipd.idaho.goldenGateServer.ats.client;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;

import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.TokenSequence;
import de.uka.ipd.idaho.goldenGateServer.ats.GoldenGateAtsConstants;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringRelation;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * Client for accessing the annotation lists stored in a GoldenGATE ATS.
 * 
 * @author sautter
 */
public class GoldenGateAtsClient implements GoldenGateAtsConstants {
	
	private ServerConnection serverConnection;
	
	/**
	 * Constructor
	 * @param serverConnection the ServerConnection to use for communication
	 *            with the backing ATS
	 */
	public GoldenGateAtsClient(ServerConnection serverConnection) {
		this.serverConnection = serverConnection;
	}
	
	/**
	 * Retrieve a list of the annotation types the backing ATS provides lists of
	 * @return an array holding the annotation types provided by the backing ATS
	 */
	public String[] getAnnotationTypes() throws IOException {
		Connection con = null;
		try {
			con = this.serverConnection.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_ANNOTATION_TYPES);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_ANNOTATION_TYPES.equals(error)) {
				StringVector types = new StringVector();
				String type;
				while ((type = br.readLine()) != null)
					types.addElement(type);
				return types.toStringArray();
			}
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Retrieve the annotations of a specific type from the backing ATS
	 * @param type the type of the annotation to retrieve
	 * @return an array holding all annotations of the specified type
	 */
	public Annotation[] getAnnotations(String type) throws IOException {
		return this.getAnnotations(type, null);
	}
	
	/**
	 * Retrieve the annotations of a specific type from the backing ATS
	 * @param type the type of the annotation to retrieve
	 * @param predicate a GPath predicate for filtering the annotations of the specified type (specifying null will ignore the filter predicate, but specifying an invalid predicate expression will result in an IOException being thrown)
	 * @return an array holding all annotations of the specified type
	 */
	public Annotation[] getAnnotations(String type, String predicate) throws IOException {
		Connection con = null;
		try {
			con = this.serverConnection.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_ANNOTATIONS);
			bw.newLine();
			bw.write(type);
			bw.newLine();
//			bw.write((predicate == null) ? "" : predicate);
//			bw.newLine(); // TODO: enable predicate filtering
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_ANNOTATIONS.equals(error)) {
				
				//	TODO: read data and create annotations in same pass (stream data)
				
				StringRelation data = StringRelation.readCsvData(br, CSV_DELIMITER, true, null);
				StringVector dataKeys = data.getKeys();
				dataKeys.removeAll(Annotation.ANNOTATION_VALUE_ATTRIBUTE);
				dataKeys.removeAll(Annotation.START_INDEX_ATTRIBUTE);
				dataKeys.removeAll(Annotation.SIZE_ATTRIBUTE);
				dataKeys.removeAll(Annotation.END_INDEX_ATTRIBUTE);
				String[] keys = dataKeys.toStringArray();
				
				ArrayList annotations = new ArrayList();
				for (int d = 0; d < data.size(); d++) {
					StringTupel dataTupel = data.get(d);
					String value = dataTupel.getValue(Annotation.ANNOTATION_VALUE_ATTRIBUTE);
					if (value != null) {
						TokenSequence tokens = Gamta.newTokenSequence(value, null);
						Annotation annotation = Gamta.newAnnotation(tokens, type, 0, tokens.size());
						for (int k = 0; k < keys.length; k++) {
							value = dataTupel.getValue(keys[k]);
							if (value != null)
								annotation.setAttribute(keys[k], value);
						}
						annotations.add(annotation);
					}
				}
				return ((Annotation[]) annotations.toArray(new Annotation[annotations.size()]));
			}
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
}
