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
package de.uka.ipd.idaho.goldenGateServer.scs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.gamta.util.gPath.types.GPathObject;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerActivityLogger;
import de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP;
import de.uka.ipd.idaho.goldenGateServer.srs.connectors.GoldenGateSrsEXP;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * GoldenGATE SRS Collection Statistics Server (SCS) is an add-on for GoldenGATE
 * Search and Retrieval Server (SRS). It listens for updates in SRS and thereby
 * gathers statistics data on SRS's document collection. Which statistics SCS
 * gathers for each document is customizable. SCS reads the definition of the
 * statistics fields from the file 'Fields.xml', which it expects to find in its
 * data path. SCS can be triggered to re-read this file at runtime through
 * GoldenGATE Server's console interface.<br>
 * For technical reasons, SCS is modeled as an exporter service, even though the
 * export destination is its local statistics table. In particular, this class
 * exploits the asynchronous update handling of the generic export service so
 * statistics updates do not slow down document updates.
 * 
 * @author sautter
 */
public class GoldenGateSCS extends GoldenGateEXP implements GoldenGateScsConstants, LiteratureConstants {
	
	private IoProvider io;
	
	private FieldSet[] fieldSets = new FieldSet[0];
	private HashMap fieldsByName = new HashMap();
	private Field getField(String name) {
		return ((Field) this.fieldsByName.get(name));
	}
	
	private static final String DATA_TABLE_NAME = "GgScsData";
	
	/** Constructor passing 'SCS' as the letter code to super constructor
	 */
	public GoldenGateSCS() {
		super("SCS", "SrsCollectionStatistics");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.wcs.GoldenGateWCS#initComponent(de.uka.ipd.idaho.easyIO.settings.Settings)
	 */
	protected void initComponent() {
		super.initComponent();
		
		//	get database connection
		this.io = this.host.getIoProvider();
		if (!this.io.isJdbcAvailable())
			throw new RuntimeException("GoldenGATE SCS cannot work without database access.");
		
		//	load field definitions
		this.loadFields(this);
	}
	
	private synchronized void loadFields(GoldenGateServerActivityLogger log) {
		log.logResult("GoldenGateSCS: Loading field definitions ...");
		FieldSet[] fieldSets;
		
		try {
			Reader fin = new InputStreamReader(new FileInputStream(new File(this.dataPath, "Fields.xml")));
			fieldSets = FieldSet.readFieldSets(fin);
			fin.close();
			log.logResult(" - field definitions read");
		}
		catch (IOException ioe) {
			log.logError(" - could not read field definitions: " + ioe.getMessage());
			log.logError(ioe);
			return;
		}
		
		//	ensure data table
		TableDefinition td = new TableDefinition(DATA_TABLE_NAME);
		td.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		for (int fs = 0; fs < fieldSets.length; fs++) {
			Field[] fields = fieldSets[fs].getFields();
			for (int f = 0; f < fields.length; f++) {
				if (Field.STRING_TYPE.equals(fields[f].type))
					td.addColumn(fields[f].fullName, TableDefinition.VARCHAR_DATATYPE, fields[f].length);
				else if (Field.NUMBER_TYPE.equals(fields[f].type))
					td.addColumn(fields[f].fullName, TableDefinition.REAL_DATATYPE, 0);
				else if (Field.BOOLEAN_TYPE.equals(fields[f].type))
					td.addColumn(fields[f].fullName, TableDefinition.CHAR_DATATYPE, 1);
			}
		}
		if (this.io.ensureTable(td, true)) {
			log.logResult(" - database table adjusted");
			this.fieldSets = fieldSets;
			this.fieldsByName.clear();
			for (int fs = 0; fs < this.fieldSets.length; fs++) {
				Field[] fields = this.fieldSets[fs].getFields();
				for (int f = 0; f < fields.length; f++)
					this.fieldsByName.put(fields[f].fullName, fields[f]);
			}
			log.logResult(" - registers updated");
			
			//	index data table
			this.io.indexColumn(DATA_TABLE_NAME, DOCUMENT_ID_ATTRIBUTE);
			for (Iterator fit = this.fieldsByName.keySet().iterator(); fit.hasNext();)
				this.io.indexColumn(DATA_TABLE_NAME, ((String) fit.next()));
			log.logResult(" - data indexed");
		}
		else {
			log.logError(" - database table could not be adjusted");
			log.logError("   ==> keeping old field definitions");
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#exitComponent()
	 */
	protected void exitComponent() {
		//	disconnect from database
		this.io.close();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#getBinding()
	 */
	protected GoldenGateExpBinding getBinding() {
		return new GoldenGateSrsEXP(this);
	}
	
	private static final String UPDATE_FIELDS_COMMAND = "updateFields";
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.wcs.GoldenGateWCS#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(super.getActions()));
		ComponentAction ca;
		
		//	update field definitions
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return UPDATE_FIELDS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						UPDATE_FIELDS_COMMAND,
						"Reload the statistics field definitions."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					loadFields(this);
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no argument.");
			}
		};
		cal.add(ca);
		
		//	retrieve field definitions
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_FIELDS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				output.write(GET_FIELDS);
				output.newLine();
				for (int f = 0; f < fieldSets.length; f++)
					fieldSets[f].writeXml(output);
				output.flush();
			}
		};
		cal.add(ca);
		
		//	compile and retrieve statistics
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_STATISTICS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				StringBuffer fieldData = new StringBuffer();
				String line;
				do {
					input.mark(1024);
					line = input.readLine();
					if (line == null)
						break;
					if (line.startsWith("<"))
						fieldData.append(line);
					else break;
				} while (true);
				
				QueryField[] fields = QueryField.readQueryFields(new StringReader(fieldData.toString()));
				Statistics stat = getStatistics(fields);
				
				output.write(GET_STATISTICS);
				output.newLine();
				stat.writeData(output);
				output.flush();
			}
		};
		cal.add(ca);
		
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doUpdate(de.uka.ipd.idaho.gamta.QueriableAnnotation, java.util.Properties)
	 */
	protected void doUpdate(QueriableAnnotation doc, Properties docAttributes) throws IOException {
		
		//	get document ID
		String docId = ((String) doc.getAttribute(DOCUMENT_ID_ATTRIBUTE));
		
		//	collect field names
		StringVector fieldNames = new StringVector();
		fieldNames.addElementIgnoreDuplicates(DOCUMENT_ID_ATTRIBUTE);
		for (int fs = 0; fs < this.fieldSets.length; fs++) {
			Field[] fields = this.fieldSets[fs].getFields();
			for (int f = 0; f < fields.length; f++)
				fieldNames.addElementIgnoreDuplicates(fields[f].fullName);
		}
		
		//	check if document in data table
		String checkQuery = "SELECT " + fieldNames.concatStrings(", ") +
				" FROM " + DATA_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				";";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(checkQuery);
			
			//	document already in database
			if (sqr.next()) {
				
				//	collect stored field values (might save database update)
				Properties dbValues = new Properties();
				for (int f = 0; f < fieldNames.size(); f++)
					dbValues.setProperty(fieldNames.get(f), sqr.getString(f));
				StringVector updates = new StringVector();
				
				//	collect new field values
				for (int fs = 0; fs < this.fieldSets.length; fs++) {
					Field[] fields = this.fieldSets[fs].getFields();
					for (int f = 0; f < fields.length; f++) {
						Predicate[] predicates = fields[f].getPredicates();
						for (int p = 0; p < predicates.length; p++)
							if (predicates[p].isApplicable(doc)) {
								GPathObject valueObject = predicates[p].extractData(doc);
								String value = null;
								if (Field.STRING_TYPE.equals(fields[f].type)) {
									value = valueObject.asString().value;
									if (value.length() > fields[f].length)
										value = value.substring(0, fields[f].length);
									if (value.equals(dbValues.getProperty(fields[f].fullName)))
										value = null;
									else value = ("'" + EasyIO.sqlEscape(value) + "'");
								}
								else if (Field.NUMBER_TYPE.equals(fields[f].type)) {
									value = ("" + valueObject.asNumber().value);
									if (value.equals(dbValues.getProperty(fields[f].fullName)))
										value = null;
								}
								else if (Field.BOOLEAN_TYPE.equals(fields[f].type)) {
									value = (valueObject.asBoolean().value ? "T" : "F");
									if (value.equals(dbValues.getProperty(fields[f].fullName)))
										value = null;
									else value = ("'" + value + "'");
								}
								
								if (value != null)
									updates.addElementIgnoreDuplicates(fields[f].fullName + " = " + value);
								
								p = predicates.length;
							}
					}
				}
				
				//	no updates, we're done
				if (updates.isEmpty())
					return;
				
				//	updates to store in database
				else {
					String updateQuery = "UPDATE " + DATA_TABLE_NAME +
							" SET " + updates.concatStrings(", ") +
							" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
							";";
					try {
						this.io.executeUpdateQuery(updateQuery);
					}
					catch (SQLException sqle) {
						this.logError("GoldenGateSCS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating table entry.");
						this.logError("  query was " + updateQuery);
					}
				}
			}
			
			//	document not yet in database, insert it
			else {
				
				//	collect field values
				Properties values = new Properties();
				for (int fs = 0; fs < this.fieldSets.length; fs++) {
					Field[] fields = this.fieldSets[fs].getFields();
					for (int f = 0; f < fields.length; f++) {
						String value = null;
						Predicate[] predicates = fields[f].getPredicates();
						for (int p = 0; p < predicates.length; p++)
							if (predicates[p].isApplicable(doc)) {
								GPathObject valueObject = predicates[p].extractData(doc);
								if (Field.STRING_TYPE.equals(fields[f].type)) {
									value = valueObject.asString().value;
									if (value.length() > fields[f].length)
										value = value.substring(0, fields[f].length);
									value = ("'" + EasyIO.sqlEscape(value) + "'");
								}
								else if (Field.NUMBER_TYPE.equals(fields[f].type)) {
									value = ("" + valueObject.asNumber().value);
								}
								else if (Field.BOOLEAN_TYPE.equals(fields[f].type)) {
									value = (valueObject.asBoolean().value ? "T" : "F");
									value = ("'" + value + "'");
								}
								p = predicates.length;
							}
						if (value == null) {
							if (Field.STRING_TYPE.equals(fields[f].type))
								value = "''";
							else if (Field.NUMBER_TYPE.equals(fields[f].type))
								value = "0";
							else if (Field.BOOLEAN_TYPE.equals(fields[f].type))
								value = "F";
						}
						if (value != null)
							values.setProperty(fields[f].fullName, value);
					}
				}
				
				//	assemble query
				StringVector fieldValues = new StringVector();
				fieldValues.addElement("'" + docId + "'");
				for (int f = 1; f < fieldNames.size(); f++) {
					String value = values.getProperty(fieldNames.get(f));
					if (value == null)
						fieldNames.remove(f--);
					else fieldValues.addElement(value);
				}
				String insertQuery = "INSERT INTO " + DATA_TABLE_NAME +
						" (" + fieldNames.concatStrings(", ") + ")" +
						" VALUES" +
						" (" + fieldValues.concatStrings(", ") + ")" +
						";";
				
				//	execute query
				try {
					this.io.executeUpdateQuery(insertQuery);
				}
				catch (SQLException sqle) {
					this.logError("GoldenGateSCS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while creating table entry.");
					this.logError("  query was " + insertQuery);
				}
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSCS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while checking table entry.");
			this.logError("  query was " + checkQuery);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doDelete(java.lang.String, java.util.Properties)
	 */
	protected void doDelete(String docId, Properties docAttributes) throws IOException {
		String deleteQuery = "DELETE FROM " + DATA_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				";";
		try {
			io.executeUpdateQuery(deleteQuery);
		}
		catch (SQLException sqle) {
			this.logError("PlaziWCS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting table entry.");
			this.logError("  query was " + deleteQuery);
		}
	}
	
	private Statistics getStatistics(QueryField[] queryFields) throws IOException {
		ArrayList resQueryFields = new ArrayList();
		
		//	collect fields, aggregates, and filter predicates
		StringVector selectFields = new StringVector();
		StringVector resultFields = new StringVector();
		StringVector groupFields = new StringVector();
		StringVector predicates = new StringVector();
		TreeMap sortOrderFields = new TreeMap();
		for (int f = 0; f < queryFields.length; f++) {
			if ("count".equals(queryFields[f].name)) {
				selectFields.addElement("count(" + DOCUMENT_ID_ATTRIBUTE + ") AS count");
				resultFields.addElement("count");
				if (queryFields[f].sortPriority > -1)
					sortOrderFields.put(new Integer(queryFields[f].sortPriority), "count");
				resQueryFields.add(new QueryField(Field.COUNT_FIELD, Aggregate.COUNT_TYPE, null, queryFields[f].sortPriority));
			}
			
			Field field = this.getField(queryFields[f].name);
			if (field == null)
				continue;
			
			if (QueryField.GROUP_OPERATION.equals(queryFields[f].operation)) {
				selectFields.addElement(queryFields[f].name);
				resultFields.addElement(queryFields[f].name);
				if (queryFields[f].sortPriority > -1)
					sortOrderFields.put(new Integer(queryFields[f].sortPriority), queryFields[f].name);
				groupFields.addElementIgnoreDuplicates(queryFields[f].name);
				resQueryFields.add(queryFields[f]);
			}
			else if (!Aggregate.IGNORE_TYPE.equals(queryFields[f].operation)) {
				selectFields.addElement(queryFields[f].operation + "(" + queryFields[f].name + ") AS " + queryFields[f].name);
				resultFields.addElement(queryFields[f].name);
				if (queryFields[f].sortPriority > -1)
					sortOrderFields.put(new Integer(queryFields[f].sortPriority), queryFields[f].name);
				resQueryFields.add(queryFields[f]);
			}
			
			if (queryFields[f].filter != null) {
				if (Field.STRING_TYPE.equals(field.type))
					predicates.addElementIgnoreDuplicates(field.fullName + " LIKE '" + EasyIO.prepareForLIKE(queryFields[f].filter) + "%'");
				
				else if (Field.BOOLEAN_TYPE.equals(field.type))
					predicates.addElementIgnoreDuplicates(field.fullName + " = '" + (((queryFields[f].filter.length() == 0) || "false".equalsIgnoreCase(queryFields[f].filter) || "f".equalsIgnoreCase(queryFields[f].filter) || "0".equals(queryFields[f].filter)) ? "F" : "T") + "'");
				
				else if (Field.NUMBER_TYPE.equals(field.type)) try {
					Double.parseDouble(queryFields[f].filter); // make sure it's a number (prevents SQL injection)
					predicates.addElementIgnoreDuplicates(field.fullName + " = " + queryFields[f].filter);
				} catch (NumberFormatException nfe) { /* nice try, Mr. Injector ... */ }
			}
		}
		
		if (selectFields.isEmpty())
			throw new IOException("Cannot create empty statistics. Use at least on field.");
		
		StringVector orderFields = new StringVector();
		for (Iterator sit = sortOrderFields.values().iterator(); sit.hasNext();)
			orderFields.addElementIgnoreDuplicates((String) sit.next());
		/*
		SELECT
		  grp_field+
		  aggregate(agg_field) as agg_field*
		FROM DATA_TABLE_NAME
		WHERE
		  grp_field like 'filterValue'*
		  agg_field like 'filterValue'*
		GROUP BY
		  grp_field+
		ORDER BY
		  grp_field+
		  agg_field*
		*/
		
		String query = "SELECT " + selectFields.concatStrings(", ") +
				" FROM " + DATA_TABLE_NAME +
				(predicates.isEmpty() ? "" : (" WHERE (" + predicates.concatStrings(") AND (") + ")")) + 
				(groupFields.isEmpty() ? "" : (" GROUP BY " + groupFields.concatStrings(", "))) +
				(orderFields.isEmpty() ? "" : (" ORDER BY " + orderFields.concatStrings(", "))) +
				";";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query);
			Statistics stat = new Statistics((QueryField[]) resQueryFields.toArray(new QueryField[resQueryFields.size()]));
			
			while (sqr.next()) {
				StringTupel st = new StringTupel();
				for (int f = 0; f < resultFields.size(); f++)
					st.setValue(resultFields.get(f), sqr.getString(f));
				stat.addElement(st);
			}
			return stat;
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSCS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while creating statistics.");
			this.logError("  query was " + query);
			throw new IOException("Could not create statistics: " + sqle.getMessage());
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
}