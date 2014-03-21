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
 *     * Neither the name of the Universität Karlsruhe (TH) / KIT nor the
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
package de.uka.ipd.idaho.goldenGateServer.dcs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.GenericQueriableAnnotationWrapper;
import de.uka.ipd.idaho.gamta.util.gPath.GPathVariableResolver;
import de.uka.ipd.idaho.gamta.util.gPath.types.GPathObject;
import de.uka.ipd.idaho.goldenGateServer.dst.GoldenGateServerDocConstants;
import de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * Generic statistics component for the document collection hosted in a
 * GoldenGATE DIO or GoldenGATE SRS. This class indexes checkin and update
 * time, year, month, and user. All domain specific indexing remains to be
 * added by sub classes.
 * 
 * @author sautter
 */
public abstract class GoldenGateDCS extends GoldenGateEXP implements GoldenGateDcsConstants, GoldenGateServerDocConstants {
	
	private static final String DOCUMENT_ID_HASH_ATTRIBUTE = "docIdHash";
	
	private StatFieldSet fieldSet;
	private StatFieldGroup[] fieldGroups;
	private StatFieldGroup docFieldGroup;
	
	private IoProvider io;
	
	private String tableNamePrefix;
	
	private Properties fieldGroupsToTables = new Properties();
	private Properties fieldGroupsToTableAliases = new Properties();
	
	private TreeMap fieldsByFullName = new TreeMap(String.CASE_INSENSITIVE_ORDER);
	
	/**
	 * @param letterCode the letter code to use
	 */
	protected GoldenGateDCS(String letterCode) {
		super(letterCode);
		this.tableNamePrefix = (letterCode.substring(0, 1).toUpperCase() + letterCode.substring(1).toLowerCase()).replaceAll("[^A-Za-z]", "");
	}
	
	/**
	 * This implementation reads the statistics field definitions. Sub classes
	 * overwriting this method thus have to make the super invocation.
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#initComponent()
	 */
	protected void initComponent() {
		super.initComponent();
		
		//	get database connector
		this.io = this.host.getIoProvider();
		if (!this.io.isJdbcAvailable())
			throw new RuntimeException("DocumentCollectionStatistics: cannot work without database access");
		
		//	load field set
		try {
			this.fieldSet = StatFieldSet.readFieldSet(new BufferedReader(new InputStreamReader(new FileInputStream(new File(this.dataPath, "fields.xml")))));
			this.fieldGroups = this.fieldSet.getFieldGroups();
			for (int g = 0; g < this.fieldGroups.length; g++) {
				if ("doc".equals(this.fieldGroups[g].name))
					this.docFieldGroup = this.fieldGroups[g];
				StatField[] fgFields = this.fieldGroups[g].getFields();
				for (int f = 0; f < fgFields.length; f++)
					this.fieldsByFullName.put(fgFields[f].fullName, fgFields[f]);
			}
		}
		catch (IOException ioe) {
			throw new RuntimeException("DocumentCollectionStatistics: cannot work without field set definition");
		}
		
		//	create document table
		TableDefinition dtd = new TableDefinition(this.tableNamePrefix + "Stats" + "Doc" + "Data");
		dtd.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		dtd.addColumn(DOCUMENT_ID_HASH_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		
		//	store table names and aliases for query construction
		this.fieldGroupsToTables.setProperty("doc", dtd.getTableName());
		this.fieldGroupsToTableAliases.setProperty("doc", dtd.getTableName().replaceAll("[^A-Z]", "").toLowerCase());
		
		//	create other database tables
		TableDefinition[] stds = new TableDefinition[this.fieldGroups.length];
		for (int g = 0; g < this.fieldGroups.length; g++) {
			
			//	find or create table definition
			if ("doc".equals(this.fieldGroups[g].name))
				stds[g] = dtd;
			else if (stds[g] == null) {
				stds[g] = new TableDefinition(this.tableNamePrefix + "Stats" + this.fieldGroups[g].name.substring(0, 1).toUpperCase() + this.fieldGroups[g].name.substring(1) + "Data");
				stds[g].addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
				stds[g].addColumn(DOCUMENT_ID_HASH_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
			}
			
			//	store table names and aliases for query construction
			this.fieldGroupsToTables.setProperty(this.fieldGroups[g].name, stds[g].getTableName());
			this.fieldGroupsToTableAliases.setProperty(this.fieldGroups[g].name, stds[g].getTableName().replaceAll("[^A-Z]", "").toLowerCase());
			
			//	add data fields
			StatField[] fgFields = this.fieldGroups[g].getFields();
			for (int f = 0; f < fgFields.length; f++) {
				if (StatField.STRING_TYPE.equals(fgFields[f].dataType))
					stds[g].addColumn(fgFields[f].name, TableDefinition.VARCHAR_DATATYPE, fgFields[f].dataLength);
				else if (StatField.INTEGER_TYPE.equals(fgFields[f].dataType))
					stds[g].addColumn(fgFields[f].name, TableDefinition.INT_DATATYPE, 0);
				else if (StatField.REAL_TYPE.equals(fgFields[f].dataType))
					stds[g].addColumn(fgFields[f].name, TableDefinition.REAL_DATATYPE, 0);
				else if (StatField.BOOLEAN_TYPE.equals(fgFields[f].dataType))
					stds[g].addColumn(fgFields[f].name, TableDefinition.CHAR_DATATYPE, 1);
			}
		}
		
		//	create and index document table
		if (!this.io.ensureTable(dtd, true))
			throw new RuntimeException("DocumentCollectionStatistics: cannot work without database access");
		this.io.indexColumn(dtd.getTableName(), DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(dtd.getTableName(), DOCUMENT_ID_HASH_ATTRIBUTE);
		
		//	create and index sub tables
		for (int t = 0; t < stds.length; t++) {
			if (stds[t] == dtd)
				continue;
			
			//	create and index table
			if (!this.io.ensureTable(stds[t], true))
				throw new RuntimeException("DocumentCollectionStatistics: cannot work without database access");
			this.io.indexColumn(stds[t].getTableName(), DOCUMENT_ID_ATTRIBUTE);
			this.io.indexColumn(stds[t].getTableName(), DOCUMENT_ID_HASH_ATTRIBUTE);
			
			//	add referential constraints
			this.io.setForeignKey(stds[t].getTableName(), DOCUMENT_ID_ATTRIBUTE, dtd.getTableName(), DOCUMENT_ID_ATTRIBUTE);
			this.io.setForeignKey(stds[t].getTableName(), DOCUMENT_ID_HASH_ATTRIBUTE, dtd.getTableName(), DOCUMENT_ID_HASH_ATTRIBUTE);
		}
		
		/* TODO define fields in XML file
		 * - assemble form dynamically as well
		 * 
		 * ==> embed field group and field classes, with on-board parsing methods, into constant interface
		 * ==> add getFieldGroup() method to client class
		 */
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.wcs.GoldenGateWCS#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(super.getActions()));
		ComponentAction ca;
		
		//	retrieve field definitions
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return (letterCode + GET_FIELDS_COMMAND_SUFFIX);
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				output.write(letterCode + GET_FIELDS_COMMAND_SUFFIX);
				output.newLine();
				fieldSet.writeXml(output);
				output.flush();
			}
		};
		cal.add(ca);
		
		//	compile and retrieve statistics
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return (letterCode + GET_STATISTICS_COMMAND_SUFFIX);
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				String[] outputFields = input.readLine().split("\\s+");
				String[] groupingFields = input.readLine().split("\\s+");
				String[] orderingFields = input.readLine().split("\\s");
				Properties fieldPredicates = new Properties();
				Properties fieldAggregates = new Properties();
				Properties aggregatePredicates = new Properties();
				for (String line; (line = input.readLine()) != null;) {
					line = line.trim();
					if (line.length() == 0)
						break;
					if (line.indexOf(":") == -1)
						continue;
					if (line.indexOf("=") == -1)
						continue;
					if (line.startsWith("FP:")) {
						line = line.substring("FP:".length()).trim();
						if (line.indexOf("=") == -1)
							continue;
						String field = line.substring(0, line.indexOf("="));
						String predicate = URLDecoder.decode(line.substring(line.indexOf("=") + "=".length()), ENCODING);
						fieldPredicates.setProperty(field, predicate);
					}
					else if (line.startsWith("FA:")) {
						line = line.substring("FA:".length()).trim();
						if (line.indexOf("=") == -1)
							continue;
						String field = line.substring(0, line.indexOf("="));
						String aggregate = URLDecoder.decode(line.substring(line.indexOf("=") + "=".length()), ENCODING);
						if (";count;count-distinct;min;max;sum;avg;".indexOf(";" + aggregate.toLowerCase() + ";") != -1)
							fieldPredicates.setProperty(field, aggregate);
					}
					else if (line.startsWith("AP:")) {
						line = line.substring("AP:".length()).trim();
						String field = line.substring(0, line.indexOf("="));
						String predicate = URLDecoder.decode(line.substring(line.indexOf("=") + "=".length()), ENCODING);
						aggregatePredicates.setProperty(field, predicate);
					}
				}
				
				DcStatistics stat = getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates);
				
				output.write(letterCode + GET_STATISTICS_COMMAND_SUFFIX);
				output.newLine();
				stat.writeData(output);
				output.flush();
			}
		};
		cal.add(ca);
		
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#getIndexFields()
	 */
	protected TableColumnDefinition[] getIndexFields() {
		ArrayList tcdList = new ArrayList(Arrays.asList(super.getIndexFields()));
		tcdList.add(new TableColumnDefinition(DOCUMENT_NAME_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 64));
		return ((TableColumnDefinition[]) tcdList.toArray(new TableColumnDefinition[tcdList.size()]));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#getIndexFieldValue(java.lang.String, de.uka.ipd.idaho.gamta.QueriableAnnotation)
	 */
	protected String getIndexFieldValue(String fieldName, QueriableAnnotation doc) {
		if (DOCUMENT_NAME_ATTRIBUTE.equals(fieldName))
			return ((String) doc.getAttribute(DOCUMENT_NAME_ATTRIBUTE));
		else return super.getIndexFieldValue(fieldName, doc);
	}
	
	/**
	 * This implementation disconnects from the database. Sub classes
	 * overwriting this method thus have to make the super invocation.
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#exitComponent()
	 */
	protected void exitComponent() {
		super.exitComponent();
		
		//	disconnect from database
		this.io.close();
	}
	
	private DcStatistics getStatistics(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates) {
		StringBuffer outputFieldString = new StringBuffer("'' AS dummy, count(*) AS DocCount");
		HashSet outputFieldSet = new HashSet();
		StringBuffer tableString = new StringBuffer(this.fieldGroupsToTables.getProperty("doc") + " " + this.fieldGroupsToTableAliases.getProperty("doc"));
		HashSet tableSet = new HashSet();
		tableSet.add("doc");
		StringBuffer whereString = new StringBuffer("1=1");
		StringBuffer joinWhereString = new StringBuffer("1=1");
		StringBuffer groupFieldString = new StringBuffer("dummy");
		HashSet groupFieldSet = new HashSet();
		StringBuffer havingString = new StringBuffer("1=1");
		StringBuffer orderFieldString = new StringBuffer("dummy");
		HashSet orderFieldSet = new HashSet();
		HashSet filterFieldSet = new HashSet();
		
		StringVector statFields = new StringVector();
		statFields.addElement("DocCount");
		
		//	assemble grouping fields
		for (int f = 0; f < groupingFields.length; f++) {
			StatField field = ((StatField) this.fieldsByFullName.get(groupingFields[f]));
			if (field == null)
				continue;
			if (groupFieldSet.add(field.fullName)) {
				groupFieldString.append(", " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.name);
				if (tableSet.add(field.group.name)) {
					tableString.append(", " + this.fieldGroupsToTables.getProperty(field.group.name) + " " + this.fieldGroupsToTableAliases.getProperty(field.group.name));
					joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE);
					joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_HASH_ATTRIBUTE);
				}
			}
		}
		
		//	assemble output fields
		for (int f = 0; f < outputFields.length; f++) {
			StatField field = ((StatField) this.fieldsByFullName.get(outputFields[f]));
			if (field == null)
				continue;
			if (outputFieldSet.add(field.fullName)) {
				if (groupFieldSet.contains(field.fullName))
					outputFieldString.append(", " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.name + " AS " + field.statColName);
				else {
					String aggregate = fieldAggregates.getProperty(field.fullName, field.defAggregate);
					if ((";sum;avg;".indexOf(";" + aggregate + ";") != -1) && !StatField.INTEGER_TYPE.equals(field.dataType) && !StatField.REAL_TYPE.equals(field.dataType))
						continue;
					if ("count-distinct".equals(aggregate))
						outputFieldString.append(", count(DISTINCT " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.name + ") AS " + field.statColName);
					else outputFieldString.append(", " + aggregate + "(" + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.name + ") AS " + field.statColName);
				}
				if (tableSet.add(field.group.name)) {
					tableString.append(", " + this.fieldGroupsToTables.getProperty(field.group.name) + " " + this.fieldGroupsToTableAliases.getProperty(field.group.name));
					joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE);
					joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_HASH_ATTRIBUTE);
				}
				statFields.addElement(field.statColName);
			}
		}
		
		//	assemble ordering fields
		for (int f = 0; f < orderingFields.length; f++) {
			StatField field = ((StatField) this.fieldsByFullName.get(orderingFields[f]));
			if (field == null)
				continue;
			String dataType = field.dataType;
			if (!groupFieldSet.contains(field.fullName)) {
				String aggregate = fieldAggregates.getProperty(field.fullName, field.defAggregate);
				if (";count;count-distinct;".indexOf(";" + aggregate.toLowerCase() + ";") != -1)
					dataType = StatField.INTEGER_TYPE;
			}
			if (orderFieldSet.add(field.fullName) && outputFieldSet.contains(field.fullName))
				orderFieldString.append(", " + field.statColName + (StatField.STRING_TYPE.equals(dataType) ? " ASC" : " DESC"));
		}
		
		//	assemble filter predicates
		for (Iterator ffit = fieldPredicates.keySet().iterator(); ffit.hasNext();) {
			StatField field = ((StatField) this.fieldsByFullName.get((String) ffit.next()));
			if (field == null)
				continue;
			String predicateString = fieldPredicates.getProperty(field.fullName);
			if ((predicateString == null) || (predicateString.trim().length() == 0))
				continue;
			predicateString = predicateString.trim();
			if (filterFieldSet.add(field.fullName)) {
				String minPredicate = null;
				String maxPredicate = null;
				if (StatField.STRING_TYPE.equals(field.dataType)) {
					if ((predicateString.startsWith("\"") || predicateString.startsWith("-")) && (predicateString.endsWith("\"") || predicateString.endsWith("-"))) {
						String[] predicateParts = predicateString.trim().split("\\\"\\s*\\-\\s*\\\"", 2);
						if (predicateParts.length == 1)
							minPredicate = (" = '" + EasyIO.sqlEscape(predicateString) + "'");
						else if (predicateParts.length == 2) {
							if (predicateParts[0].startsWith("\""))
								predicateParts[0] = predicateParts[0].substring(1).trim();
							minPredicate = ((predicateParts[0].length() == 0) ? null : (" >= '" + EasyIO.sqlEscape(predicateParts[0]) + "'"));
							if (predicateParts[1].endsWith("\""))
								predicateParts[1] = predicateParts[1].substring(0, (predicateParts[1].length() - 1)).trim();
							maxPredicate = ((predicateParts[1].length() == 0) ? null : (" <= '" + EasyIO.sqlEscape(predicateParts[1]) + "'"));
						}
					}
					else minPredicate = (" LIKE '" + EasyIO.sqlEscape(predicateString) + "'");
				}
				else if (StatField.INTEGER_TYPE.equals(field.dataType) || StatField.REAL_TYPE.equals(field.dataType)) {
					String[] predicateParts = predicateString.trim().split("\\s*\\-\\s*", 2);
					if (predicateParts.length == 1)
						minPredicate = (" = " + Integer.parseInt(predicateParts[0]));
					else if (predicateParts.length == 2) {
						minPredicate = ((predicateParts[0].length() == 0) ? null : (" >= " + Integer.parseInt(predicateParts[0])));
						maxPredicate = ((predicateParts[1].length() == 0) ? null : (" <= " + Integer.parseInt(predicateParts[1])));
					}
				}
				else if (StatField.BOOLEAN_TYPE.equals(field.dataType))
					minPredicate = (" = '" + ((";0;false;no;".indexOf(";" + predicateString.trim().toLowerCase() + ";") == -1) ? 'T' : 'F') + "'");
				else continue;
				
				if (minPredicate != null)
					whereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.name + minPredicate);
				if (maxPredicate != null)
					whereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.name + maxPredicate);
				
				if (tableSet.add(field.group.name)) {
					tableString.append(", " + this.fieldGroupsToTables.getProperty(field.group.name) + " " + this.fieldGroupsToTableAliases.getProperty(field.group.name));
					joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE);
					joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_HASH_ATTRIBUTE);
				}
			}
		}
		
		//	assemble aggregate predicates
		for (Iterator ffit = aggregatePredicates.keySet().iterator(); ffit.hasNext();) {
			String fieldName = ((String) ffit.next());
			if ("docCount".equals(fieldName)) {
				String predicateString = aggregatePredicates.getProperty(fieldName);
				if ((predicateString == null) || (predicateString.trim().length() == 0))
					continue;
				String[] predicateParts = predicateString.trim().split("\\s*\\-\\s*", 2);
				String minPredicate = null;
				String maxPredicate = null;
				if (predicateParts.length == 1)
					minPredicate = (" = " + Integer.parseInt(predicateParts[0]));
				else if (predicateParts.length == 2) {
					minPredicate = ((predicateParts[0].length() == 0) ? null : (" >= " + Integer.parseInt(predicateParts[0])));
					maxPredicate = ((predicateParts[1].length() == 0) ? null : (" <= " + Integer.parseInt(predicateParts[1])));
				}
				if (minPredicate != null)
					havingString.append(" AND " + "count(*)" + minPredicate);
				if (maxPredicate != null)
					whereString.append(" AND " + "count(*)" + maxPredicate);
				continue;
			}
			StatField field = ((StatField) this.fieldsByFullName.get(fieldName));
			if ((field == null) || StatField.BOOLEAN_TYPE.equals(field.dataType))
				continue;
			String predicateString = aggregatePredicates.getProperty(field.fullName);
			if ((predicateString == null) || (predicateString.trim().length() == 0))
				continue;
			predicateString = predicateString.trim();
			String aggregate = fieldAggregates.getProperty(field.fullName, field.defAggregate);
			if ((";sum;avg;".indexOf(";" + aggregate + ";") != -1) && !StatField.INTEGER_TYPE.equals(field.dataType) && !StatField.REAL_TYPE.equals(field.dataType))
				continue;
			String aggregateDataType = field.dataType;
			if (";count;count-distinct;".indexOf(";" + aggregate.toLowerCase() + ";") != -1)
				aggregateDataType = StatField.INTEGER_TYPE;
			else if (";sum;avg;".indexOf(";" + aggregate.toLowerCase() + ";") != -1)
				aggregateDataType = (StatField.REAL_TYPE.equals(field.dataType) ? StatField.REAL_TYPE : StatField.INTEGER_TYPE);
			String minPredicate = null;
			String maxPredicate = null;
			if (StatField.STRING_TYPE.equals(aggregateDataType)) {
				if ((predicateString.startsWith("\"") || predicateString.startsWith("-")) && (predicateString.endsWith("\"") || predicateString.endsWith("-"))) {
					String[] predicateParts = predicateString.trim().split("\\\"\\s*\\-\\s*\\\"", 2);
					if (predicateParts.length == 1)
						minPredicate = (" = '" + EasyIO.sqlEscape(predicateString) + "'");
					else if (predicateParts.length == 2) {
						if (predicateParts[0].startsWith("\""))
							predicateParts[0] = predicateParts[0].substring(1).trim();
						minPredicate = ((predicateParts[0].length() == 0) ? null : (" >= '" + EasyIO.sqlEscape(predicateParts[0]) + "'"));
						if (predicateParts[1].endsWith("\""))
							predicateParts[1] = predicateParts[1].substring(0, (predicateParts[1].length() - 1)).trim();
						maxPredicate = ((predicateParts[1].length() == 0) ? null : (" <= '" + EasyIO.sqlEscape(predicateParts[1]) + "'"));
					}
				}
				else minPredicate = (" LIKE '" + EasyIO.sqlEscape(predicateString) + "'");
			}
			else if (StatField.INTEGER_TYPE.equals(aggregateDataType) || StatField.REAL_TYPE.equals(aggregateDataType)) {
				String[] predicateParts = predicateString.trim().split("\\s*\\-\\s*", 2);
				if (predicateParts.length == 1)
					minPredicate = (" = " + Integer.parseInt(predicateParts[0]));
				else if (predicateParts.length == 2) {
					minPredicate = ((predicateParts[0].length() == 0) ? null : (" >= " + Integer.parseInt(predicateParts[0])));
					maxPredicate = ((predicateParts[1].length() == 0) ? null : (" <= " + Integer.parseInt(predicateParts[1])));
				}
			}
			else if (StatField.BOOLEAN_TYPE.equals(aggregateDataType))
				minPredicate = (" = '" + ((";0;false;no;".indexOf(";" + predicateString.trim().toLowerCase() + ";") == -1) ? 'T' : 'F') + "'");
			else continue;
			
			String aggreateField = ("count-distinct".equals(aggregate) ? ("count(DISTINCT " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.name + ")") : (aggregate + "(" + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.name + ")"));
			if (minPredicate != null)
				havingString.append(" AND " + aggreateField + minPredicate);
			if (maxPredicate != null)
				whereString.append(" AND " + aggreateField + maxPredicate);
			
			if (tableSet.add(field.group.name)) {
				tableString.append(", " + this.fieldGroupsToTables.getProperty(field.group.name) + " " + this.fieldGroupsToTableAliases.getProperty(field.group.name));
				joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE);
				joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_HASH_ATTRIBUTE);
			}
		}
		
		//	assemble query
		String query = "SELECT " + outputFieldString.toString() +
				" FROM " + tableString.toString() +
				" WHERE " + whereString.toString() +
				" AND " + joinWhereString.toString() +
				" GROUP BY " + groupFieldString.toString() +
				" HAVING " + havingString.toString() +
				" ORDER BY " + orderFieldString.toString() +
				";";
		
		//	produce statistics
		SqlQueryResult sqr = null;
		try {
			if (this.host == null) {
				System.out.println(query);
				return null;
			}
			sqr = this.io.executeSelectQuery(query.toString(), true);
			DcStatistics stat = new DcStatistics(statFields.toStringArray());
			
			while (sqr.next()) {
				StringTupel st = new StringTupel();
				for (int f = 1 /* skip the dummy */; (f < sqr.getColumnCount()) && ((f-1) < statFields.size()); f++) {
					st.setValue(statFields.get(f-1), sqr.getString(f));
				}
				stat.addElement(st);
			}
			return stat;
		}
		catch (SQLException sqle) {
			System.out.println("DocumentCollectionStatistics: exception generating statistics: " + sqle.getMessage());
			System.out.println("  Query was: " + query);
			return null;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doUpdate(de.uka.ipd.idaho.gamta.QueriableAnnotation, java.util.Properties)
	 */
	protected void doUpdate(QueriableAnnotation doc, Properties docAttributes) throws IOException {
		
		//	get document ID
		String docId = ((String) doc.getAttribute(DOCUMENT_ID_ATTRIBUTE));
		
		//	clean up
		this.doDelete(docId, docAttributes);
		
		//	wrap document to format timestamps
		doc = new GenericQueriableAnnotationWrapper(doc) {
			public Object getAttribute(String name, Object def) {
				return this.adjustAttributeValue(name, super.getAttribute(name, def));
			}
			public Object getAttribute(String name) {
				return this.adjustAttributeValue(name, super.getAttribute(name));
			}
			private Object adjustAttributeValue(String name, Object value) {
				if (value == null)
					return value;
				if (CHECKIN_TIME_ATTRIBUTE.equals(name) || UPDATE_TIME_ATTRIBUTE.equals(name)) try {
					return timestampDateFormat.format(new Date(Long.parseLong(value.toString())));
				} catch (Exception e) {}
				return value;
			}
		};
		
		//	assemble insert queries
		String docTableFields = (DOCUMENT_ID_ATTRIBUTE +
				", " + DOCUMENT_ID_HASH_ATTRIBUTE +
				"");
		String docTableValues = ("'" + EasyIO.sqlEscape(docId) + "'" +
				", " + docId.hashCode() +
				"");
		
		//	get global variables
		GPathVariableResolver variables = new GPathVariableResolver();
		StatVariable[] svs = this.fieldSet.getVariables();
		for (int v = 0; v < svs.length; v++) {
			GPathObject value = svs[v].getValue(doc, variables);
			if ((value != null) && (value.asBoolean().value))
				variables.setVariable(svs[v].name, value);
		}
		
		//	fill document table
		this.updateFieldGroup(this.docFieldGroup, doc, doc, variables, docTableFields, docTableValues);
		
		//	fill sub tables
		String subTableFields = (DOCUMENT_ID_ATTRIBUTE +
				", " + DOCUMENT_ID_HASH_ATTRIBUTE);
		String subTableValues = ("'" + EasyIO.sqlEscape(docId) + "'" +
				", " + docId.hashCode());
		for (int g = 0; g < this.fieldGroups.length; g++) {
			if (this.fieldGroups[g] == this.docFieldGroup)
				continue;
			QueriableAnnotation[] contexts = this.fieldGroups[g].defContext.evaluate(doc, variables);
			for (int c = 0; c < contexts.length; c++)
				this.updateFieldGroup(this.fieldGroups[g], doc, contexts[c], variables, subTableFields, subTableValues);
		}
	}
	private static class UtcDateFormat extends SimpleDateFormat {
		UtcDateFormat(String pattern) {
			super(pattern, Locale.US);
			this.setTimeZone(TimeZone.getTimeZone("UTC")); 
		}
	}
	private static final DateFormat timestampDateFormat = new UtcDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private void updateFieldGroup(StatFieldGroup fieldGroup, QueriableAnnotation doc, QueriableAnnotation context, GPathVariableResolver variables, String fieldsPrefix, String valuesPrefix) {
		StringBuffer fieldNames = new StringBuffer(fieldsPrefix);
		StringBuffer fieldValues = new StringBuffer(valuesPrefix);
		
		//	get field values (field group may be null if document table only contains default fields)
		if (fieldGroup != null) {
			StatField[] fields = fieldGroup.getFields();
			for (int f = 0; f < fields.length; f++) {
				GPathObject value = fields[f].getFieldValue(doc, context, variables);
				if (value != null)
					value = this.normalizeValue(fields[f], value);
				if (StatField.STRING_TYPE.equals(fields[f].dataType)) {
					fieldNames.append(", " + fields[f].name);
					if (value == null)
						fieldValues.append(", ''");
					else {
						String str = value.asString().value;
						if (str.length() > fields[f].dataLength)
							str = str.substring(0, fields[f].dataLength);
						fieldValues.append(", '" + EasyIO.sqlEscape(str) + "'");
					}
				}
				else if (StatField.INTEGER_TYPE.equals(fields[f].dataType)) {
					fieldNames.append(", " + fields[f].name);
					if ((value == null) || "NaN".equals(value.asString().value))
						fieldValues.append(", 0");
					else try {
						fieldValues.append(", " + Integer.parseInt(value.asString().value));
					}
					catch (NumberFormatException nfe) {
						fieldValues.append(", 0");
					}
				}
				else if (StatField.REAL_TYPE.equals(fields[f].dataType)) {
					fieldNames.append(", " + fields[f].name);
					if ((value == null) || "NaN".equals(value.asString().value))
						fieldValues.append(", 0");
					else try {
						fieldValues.append(", " + Double.parseDouble(value.asString().value));
					}
					catch (NumberFormatException nfe) {
						fieldValues.append(", 0");
					}
				}
				else if (StatField.BOOLEAN_TYPE.equals(fields[f].dataType)) {
					fieldNames.append(", " + fields[f].name);
					if (value == null)
						fieldValues.append(", 'F'");
					else fieldValues.append(", '" + (value.asBoolean().value ? "T" : "F") + "'");
				}
			}
		}
		
		//	execute insert query
		String query = "INSERT INTO " + this.fieldGroupsToTables.getProperty((fieldGroup == null) ? "doc" : fieldGroup.name) + " (" +
				fieldNames.toString() +
				") VALUES (" +
				fieldValues.toString() +
				");";
		try {
			if (this.host == null)
				System.out.println(query);
			else this.io.executeUpdateQuery(query.toString());
		}
		catch (SQLException sqle) {
			System.out.println("DocumentCollectionStatistics: exception updating statistics table: " + sqle.getMessage());
			System.out.println("  Query was: " + query);
		}
	}
	
	/**
	 * Normalize a field value. This default implementation does simply returns
	 * the argument value. Sub classes are welcome to overwrite it as needed to
	 * provide their own normalization.
	 * @param field the name of the field the value belongs to
	 * @param value the field value as extracted from the document
	 * @return the normalized field value
	 */
	protected GPathObject normalizeValue(StatField field, GPathObject value) {
		return value;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doDelete(java.lang.String, java.util.Properties)
	 */
	protected void doDelete(String docId, Properties docAttributes) throws IOException {
		
		//	clean up sub tables
		for (int g = 0; g < this.fieldGroups.length; g++) {
			if ("doc".equals(this.fieldGroups[g].name))
				continue;
			String query = "DELETE FROM " + this.fieldGroupsToTables.getProperty(this.fieldGroups[g].name) + 
					" WHERE " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() + "" +
					" AND " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
					";";
			try {
				if (this.host == null)
					System.out.println(query);
				else this.io.executeUpdateQuery(query);
			}
			catch (SQLException sqle) {
				System.out.println("DocumentCollectionStatistics: exception cleaning statistics table: " + sqle.getMessage());
				System.out.println("  Query was: " + query);
			}
		}
		
		//	clean up document table
		String query = "DELETE FROM " + this.fieldGroupsToTables.getProperty("doc") + 
				" WHERE " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() + "" +
				" AND " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
				";";
		try {
			if (this.host == null)
				System.out.println(query);
			else this.io.executeUpdateQuery(query);
		}
		catch (SQLException sqle) {
			System.out.println("DocumentCollectionStatistics: exception cleaning statistics table: " + sqle.getMessage());
			System.out.println("  Query was: " + query);
		}
	}
//	
//	/**
//	 * @param args
//	 */
//	public static void main(String[] args) throws Exception {
//		GoldenGateDCS dcs = new GoldenGateDCS("TEST") {
//			protected String getExporterName() {
//				return "TEST";
//			}
//			protected GoldenGateExpBinding getBinding() {
//				return null;
//			}
//		};
//		
//		//	load field set
//		try {
//			dcs.fieldSet = StatFieldSet.readFieldSet(new BufferedReader(new InputStreamReader(new FileInputStream(new File("E:/Projektdaten/GgDcsTestFields.xml")))));
//			dcs.fieldGroups = dcs.fieldSet.getFieldGroups();
//			for (int g = 0; g < dcs.fieldGroups.length; g++) {
//				if ("doc".equals(dcs.fieldGroups[g].name))
//					dcs.docFieldGroup = dcs.fieldGroups[g];
//				StatField[] fgFields = dcs.fieldGroups[g].getFields();
//				for (int f = 0; f < fgFields.length; f++)
//					dcs.fieldsByFullName.put(fgFields[f].fullName, fgFields[f]);
//			}
//		}
//		catch (IOException ioe) {
//			throw new RuntimeException("DocumentCollectionStatistics: cannot work without field set definition");
//		}
//		
//		//	create document table
//		TableDefinition mtd = new TableDefinition(dcs.tableNamePrefix + "Stats" + "Doc" + "Data");
//		mtd.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
//		mtd.addColumn(DOCUMENT_ID_HASH_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
//		
//		//	store table names and aliases for query construction
//		dcs.fieldGroupsToTables.setProperty("doc", mtd.getTableName());
//		dcs.fieldGroupsToTableAliases.setProperty("doc", mtd.getTableName().replaceAll("[^A-Z]", "").toLowerCase());
//		
//		//	create other database tables
//		TableDefinition[] stds = new TableDefinition[dcs.fieldGroups.length];
//		for (int g = 0; g < dcs.fieldGroups.length; g++) {
//			
//			//	find or create table definition
//			if ("doc".equals(dcs.fieldGroups[g].name)) {
//				stds[g] = mtd;
//				dcs.docFieldGroup = dcs.fieldGroups[g];
//			}
//			else if (stds[g] == null) {
//				stds[g] = new TableDefinition(dcs.tableNamePrefix + "Stats" + dcs.fieldGroups[g].name.substring(0, 1).toUpperCase() + dcs.fieldGroups[g].name.substring(1) + "Data");
//				stds[g].addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
//				stds[g].addColumn(DOCUMENT_ID_HASH_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
//			}
//			
//			//	store table names and aliases for query construction
//			dcs.fieldGroupsToTables.setProperty(dcs.fieldGroups[g].name, stds[g].getTableName());
//			dcs.fieldGroupsToTableAliases.setProperty(dcs.fieldGroups[g].name, stds[g].getTableName().replaceAll("[^A-Z]", "").toLowerCase());
//			
//			//	add data fields
//			StatField[] fgFields = dcs.fieldGroups[g].getFields();
//			for (int f = 0; f < fgFields.length; f++) {
//				if (StatField.STRING_TYPE.equals(fgFields[f].dataType))
//					stds[g].addColumn(fgFields[f].name, TableDefinition.VARCHAR_DATATYPE, fgFields[f].dataLength);
//				else if (StatField.INTEGER_TYPE.equals(fgFields[f].dataType))
//					stds[g].addColumn(fgFields[f].name, TableDefinition.INT_DATATYPE, 0);
//				else if (StatField.REAL_TYPE.equals(fgFields[f].dataType))
//					stds[g].addColumn(fgFields[f].name, TableDefinition.REAL_DATATYPE, 0);
//				else if (StatField.BOOLEAN_TYPE.equals(fgFields[f].dataType))
//					stds[g].addColumn(fgFields[f].name, TableDefinition.CHAR_DATATYPE, 1);
//			}
//		}
//		
//		//	run test
//		MutableAnnotation doc = SgmlDocumentReader.readDocument(new InputStreamReader(new FileInputStream(new File("E:/Projektdaten/TaxonxTest/21330.complete.xml"))));
//		doc.setAttribute(DOCUMENT_ID_ATTRIBUTE, "Test0815");
//		dcs.doUpdate(doc, new Properties());
//		
//		//	run search test
//		String[] outputFields = {"tax.familyEpithet", "tax.genusEpithet", "tax.name"};
//		String[] groupingFields = {"tax.familyEpithet"};
//		String[] orderingFields = {"tax.name"};
//		Properties fieldPredicates = new Properties();
//		fieldPredicates.setProperty("bib.year", "2010-2011");
//		Properties fieldAggregates = new Properties();
//		fieldAggregates.setProperty("tax.name", "count");
//		Properties aggregatePredicates = new Properties();
//		aggregatePredicates.setProperty("tax.name", "5-");
//		aggregatePredicates.setProperty("matCit.specimenCount", "1-");
//		dcs.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates);
//	}
}