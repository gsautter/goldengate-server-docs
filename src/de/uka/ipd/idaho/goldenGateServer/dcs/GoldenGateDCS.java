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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

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
import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.XsltUtils;
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
	
	private TreeMap fieldGroupsByName = new TreeMap(String.CASE_INSENSITIVE_ORDER);
	private TreeMap fieldsByFullName = new TreeMap(String.CASE_INSENSITIVE_ORDER);
	
	private Properties fieldLabels = new Properties();
	
	private long lastUpdate = System.currentTimeMillis();
	
	private FormattedStaticStatExport[] staticStatExports = null;
	private long staticStatExportsDue = -1;
	private StaticStatExportThread staticStatExportThread = null;
	
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
			this.fieldLabels.setProperty("DocCount", this.fieldSet.docCountLabel);
			this.fieldGroups = this.fieldSet.getFieldGroups();
			for (int g = 0; g < this.fieldGroups.length; g++) {
				this.fieldGroupsByName.put(this.fieldGroups[g].name, this.fieldGroups[g]);
				if ("doc".equals(this.fieldGroups[g].name))
					this.docFieldGroup = this.fieldGroups[g];
				StatField[] fgFields = this.fieldGroups[g].getFields();
				for (int f = 0; f < fgFields.length; f++) {
					this.fieldsByFullName.put(fgFields[f].fullName, fgFields[f]);
					this.fieldLabels.setProperty(fgFields[f].fullName, fgFields[f].label);
					this.fieldLabels.setProperty(fgFields[f].statColName, fgFields[f].label);
				}
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
		
		//	load static exports
		try {
			this.staticStatExports = this.loadStaticStatExports();
		}
		catch (IOException ioe) {
			System.out.println("Exception loading static exports: " + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}
	}
	
	private static final String RELOAD_STATIC_EXPORTS_COMMAND = "reloadStatic";
	
	private static final String UPDATE_STATIC_EXPORTS_COMMAND = "updateStatic";
	
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
//				int limit;
//				String[] outputFields;
//				String inputLine = input.readLine();
//				try { // TODO_ne switch to code below, and remove this after some grace period
//					limit = Integer.parseInt(inputLine);
//					outputFields = input.readLine().split("\\s+");
//				}
//				catch (NumberFormatException nfe) {
//					System.out.println("DocumentCollectionStatistics: invalid row limit '" + inputLine + "'");
//					limit = -1;
//					outputFields = inputLine.split("\\s+");
//				}
				int limit = Integer.parseInt(input.readLine());
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
							fieldAggregates.setProperty(field, aggregate);
					}
					else if (line.startsWith("AP:")) {
						line = line.substring("AP:".length()).trim();
						String field = line.substring(0, line.indexOf("="));
						String predicate = URLDecoder.decode(line.substring(line.indexOf("=") + "=".length()), ENCODING);
						aggregatePredicates.setProperty(field, predicate);
					}
				}
				
				DcStatistics stat = getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, limit);
				
				output.write(letterCode + GET_STATISTICS_COMMAND_SUFFIX);
				output.newLine();
				stat.writeData(output);
				output.flush();
			}
		};
		cal.add(ca);
		
		//	reload static export definitions
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return RELOAD_STATIC_EXPORTS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = { RELOAD_STATIC_EXPORTS_COMMAND, "Reload the static statistics export definitions." };
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0)
					System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
				else try {
					staticStatExports = loadStaticStatExports();
				}
				catch (IOException ioe) {
					System.out.println("Exception reloading static exports: " + ioe.getMessage());
					ioe.printStackTrace(System.out);
				}
			}
		};
		cal.add(ca);
		
		//	execute static exports
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return UPDATE_STATIC_EXPORTS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = { UPDATE_STATIC_EXPORTS_COMMAND, "Trigger an update for static statistics exports" };
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					updateStaticStatExports(true);
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
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
	
	/**
	 * Compile and retrieve a statistics from the data stored in the backing
	 * DCS instance. Ordering is ascending for strings, but descending for
	 * numbers. The <code>DocCount</code> aggregate field is contained in every
	 * statistics.
	 * @param outputFields the fields to include in the statistics
	 * @param groupingFields the fields to use for grouping
	 * @param orderingFields the fields to use for ordering
	 * @param fieldPredicates filter predicates against individual fields
	 * @param fieldAggregates custom aggregation functions for fields not used for grouping
	 * @param aggregatePredicates filter predicates against aggregate data
	 * @return the requested statistics, packed in a string relation
	 * @throws IOException
	 */
	public DcStatistics getStatistics(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates) {
		return this.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, -1);
	}
	
	/**
	 * Compile and retrieve a statistics from the data stored in the backing
	 * DCS instance. Ordering is ascending for strings, but descending for
	 * numbers. The <code>DocCount</code> aggregate field is contained in every
	 * statistics.
	 * @param outputFields the fields to include in the statistics
	 * @param groupingFields the fields to use for grouping
	 * @param orderingFields the fields to use for ordering
	 * @param fieldPredicates filter predicates against individual fields
	 * @param fieldAggregates custom aggregation functions for fields not used for grouping
	 * @param aggregatePredicates filter predicates against aggregate data
	 * @param limit the maximum number of output rows (-1 returns all rows)
	 * @return the requested statistics, packed in a string relation
	 * @throws IOException
	 */
	public DcStatistics getStatistics(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, int limit) {
		System.out.println(this.getExporterName() + ": processing query:");
		System.out.println(" - output fields: " + Arrays.toString(outputFields));
		System.out.println(" - grouping fields: " + Arrays.toString(groupingFields));
		System.out.println(" - ordering fields: " + Arrays.toString(orderingFields));
		System.out.println(" - field predicates: " + String.valueOf(fieldPredicates));
		System.out.println(" - field aggregates: " + String.valueOf(fieldAggregates));
		System.out.println(" - aggregate predicates: " + String.valueOf(aggregatePredicates));
		
		//	prepare parsing and SQL query assembly
		StringBuffer outputFieldString = new StringBuffer("'' AS dummy, count(DISTINCT " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE + ") AS DocCount");
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
		
		//	prepare output fields
		StringVector statFields = new StringVector();
		statFields.addElement("DocCount");
		
		//	assemble grouping fields (we can order them, as grouping is commutative)
		Arrays.sort(groupingFields);
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
			boolean defaultOrder = true;
			if (orderingFields[f].startsWith("-")) {
				defaultOrder = false;
				orderingFields[f] = orderingFields[f].substring("-".length());
			}
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
				orderFieldString.append(", " + field.statColName + ((StatField.STRING_TYPE.equals(dataType) == defaultOrder) ? " ASC" : " DESC"));
		}
		
		//	assemble filter predicates
		for (Iterator ffit = fieldPredicates.keySet().iterator(); ffit.hasNext();) {
			String predicateTargetName = ((String) ffit.next());
			
			StatFieldGroup fieldGroup = ((StatFieldGroup) this.fieldGroupsByName.get(predicateTargetName));
			if (fieldGroup != null) {
				String predicateString = fieldPredicates.getProperty(fieldGroup.name);
				if ((predicateString == null) || (predicateString.trim().length() == 0))
					continue;
				predicateString = predicateString.trim();
				String[] predicateParts = predicateString.split("\\s+");
				StatField[] fields = fieldGroup.getFields();
				for (int p = 0; p < predicateParts.length; p++) {
					int i = Integer.MIN_VALUE;
					try {
						i = Integer.parseInt(predicateParts[p]);
					} catch (NumberFormatException nfe) {}
					Double d = Double.NEGATIVE_INFINITY;
					try {
						d = Double.parseDouble(predicateParts[p].replaceAll("\\,", "."));
					} catch (NumberFormatException nfe) {}
					StringBuffer predicatePartWhere = new StringBuffer("1=0");
					for (int f = 0; f < fields.length; f++) {
						if (StatField.STRING_TYPE.equals(fields[f].dataType))
							predicatePartWhere.append(" OR " + this.fieldGroupsToTableAliases.getProperty(fieldGroup.name) + "." + fields[f].name + " LIKE '" + EasyIO.sqlEscape(predicateParts[p]) + "'");
						else if (StatField.INTEGER_TYPE.equals(fields[f].dataType) && (i != Integer.MIN_VALUE))
							predicatePartWhere.append(" OR " + this.fieldGroupsToTableAliases.getProperty(fieldGroup.name) + "." + fields[f].name + " = " + i + "");
						else if (StatField.REAL_TYPE.equals(fields[f].dataType) && (d != Double.NEGATIVE_INFINITY))
							predicatePartWhere.append(" OR " + this.fieldGroupsToTableAliases.getProperty(fieldGroup.name) + "." + fields[f].name + " = " + d + "");
					}
					if (predicatePartWhere.length() > 3)
						whereString.append(" AND (" + predicatePartWhere.toString() + ")");
				}
				if (tableSet.add(fieldGroup.name)) {
					tableString.append(", " + this.fieldGroupsToTables.getProperty(fieldGroup.name) + " " + this.fieldGroupsToTableAliases.getProperty(fieldGroup.name));
					joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(fieldGroup.name) + "." + DOCUMENT_ID_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE);
					joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(fieldGroup.name) + "." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_HASH_ATTRIBUTE);
				}
				continue;
			}
			
			StatField field = ((StatField) this.fieldsByFullName.get(predicateTargetName));
			if (field == null)
				continue;
			String predicateString = fieldPredicates.getProperty(field.fullName);
			if ((predicateString == null) || (predicateString.trim().length() == 0))
				continue;
			Predicate predicate = parsePredicate(predicateString, (StatField.INTEGER_TYPE.equals(field.dataType) || StatField.REAL_TYPE.equals(field.dataType)));
			if (predicate.isEmpty()) {
				System.out.println(this.getExporterName() + ": empty predicate for " + field.dataType);
				System.out.println("  input string was " + predicateString);
				continue;
			}
			if (!filterFieldSet.add(field.fullName))
				continue;
			whereString.append(" AND " + predicate.getSql(this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.name));
			if (tableSet.add(field.group.name)) {
				tableString.append(", " + this.fieldGroupsToTables.getProperty(field.group.name) + " " + this.fieldGroupsToTableAliases.getProperty(field.group.name));
				joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE);
				joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_HASH_ATTRIBUTE);
			}
		}
		
		//	assemble aggregate predicates
		for (Iterator ffit = aggregatePredicates.keySet().iterator(); ffit.hasNext();) {
			String fieldName = ((String) ffit.next());
			if ("DocCount".equalsIgnoreCase(fieldName)) {
				String predicateString = aggregatePredicates.getProperty(fieldName);
				if ((predicateString == null) || (predicateString.trim().length() == 0))
					continue;
				Predicate predicate = parsePredicate(predicateString, true);
				if (predicate.isEmpty()) {
					System.out.println(this.getExporterName() + ": empty predicate for DocCount");
					System.out.println("  input string was " + predicateString);
					continue;
				}
				havingString.append(" AND " + predicate.getSql("count(DISTINCT " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE + ")"));
				continue;
			}
			StatField field = ((StatField) this.fieldsByFullName.get(fieldName));
			if ((field == null) || StatField.BOOLEAN_TYPE.equals(field.dataType))
				continue;
			String predicateString = aggregatePredicates.getProperty(field.fullName);
			if ((predicateString == null) || (predicateString.trim().length() == 0))
				continue;
			String aggregate = fieldAggregates.getProperty(field.fullName, field.defAggregate);
			if ((";sum;avg;".indexOf(";" + aggregate + ";") != -1) && !StatField.INTEGER_TYPE.equals(field.dataType) && !StatField.REAL_TYPE.equals(field.dataType))
				continue;
			String aggregateDataType = field.dataType;
			if (";count;count-distinct;".indexOf(";" + aggregate.toLowerCase() + ";") != -1)
				aggregateDataType = StatField.INTEGER_TYPE;
			else if (";sum;avg;".indexOf(";" + aggregate.toLowerCase() + ";") != -1)
				aggregateDataType = (StatField.REAL_TYPE.equals(field.dataType) ? StatField.REAL_TYPE : StatField.INTEGER_TYPE);
			Predicate predicate = parsePredicate(predicateString, (StatField.INTEGER_TYPE.equals(aggregateDataType) || StatField.REAL_TYPE.equals(aggregateDataType)));
			if (predicate.isEmpty()) {
				System.out.println(this.getExporterName() + ": empty predicate for " + aggregateDataType);
				System.out.println("  input string was " + predicateString);
				continue;
			}
			String aggreateField = ("count-distinct".equals(aggregate) ? ("count(DISTINCT " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.name + ")") : (aggregate + "(" + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.name + ")"));
			havingString.append(" AND " + predicate.getSql(aggreateField));
			if (tableSet.add(field.group.name)) {
				tableString.append(", " + this.fieldGroupsToTables.getProperty(field.group.name) + " " + this.fieldGroupsToTableAliases.getProperty(field.group.name));
				joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE);
				joinWhereString.append(" AND " + this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + this.fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_HASH_ATTRIBUTE);
			}
		}
		
		/* produce cache key TODO increase hit rate by exploiting commutative properties
		 * - HEX hash of output field string (includes aggregates)
		 * - HEX hash of ORDER field string
		 * - HEX hash of sorted WHERE string (predicates are commutative TODO use it)
		 * - HEX hash of sorted GROUP field string (grouping is commutative TODO use it)
		 * - HEX hash of sorted HAVING string (predicates are commutative TODO use it)
		 */
		String statsCacheKey = "" + limit +
				"-" + Integer.toString(outputFieldString.toString().hashCode(), 16) +
				"-" + Integer.toString(orderFieldString.toString().hashCode(), 16) +
				"-" + Integer.toString(whereString.toString().hashCode(), 16) +
				"-" + Integer.toString(groupFieldString.toString().hashCode(), 16) +
				"-" + Integer.toString(havingString.toString().hashCode(), 16) +
				"";
		
		//	do cache lookup
		DcStatistics stats = ((DcStatistics) this.cache.get(statsCacheKey));
		if (stats != null)
			return stats;
		
		//	create row limit clause
		/* TODO implement limitation clause distinction for MS SQL Server, Oracle, etc.
		 * see: http://en.wikipedia.org/wiki/Select_%28SQL%29#FETCH_FIRST_clause */
		String topClause;
		String limitClause;
		if (limit < 1) {
			topClause = "";
			limitClause = "";
		}
//		else if (this.io.limitIsTop()) {
//			topClause = ("TOP " + limit + " ");
//			limitClause = "";
//		}
//		else if (this.io.limitIsSupported()) {
//			topClause = "";
//			limitClause = (" LIMIT " + limit);
//		}
		else {
			topClause = "";
			limitClause = "";
		}
		
		//	assemble query
		String query = "SELECT " + topClause + outputFieldString.toString() +
				" FROM " + tableString.toString() +
				" WHERE " + whereString.toString() +
				" AND " + joinWhereString.toString() +
				" GROUP BY " + groupFieldString.toString() +
				" HAVING " + havingString.toString() +
				" ORDER BY " + orderFieldString.toString() +
				limitClause +
				";";
		System.out.println(this.getExporterName() + ": stats query is " + query);
		
		//	produce statistics
		SqlQueryResult sqr = null;
		try {
			
			//	we're testing the query parser ...
			if (this.host == null) {
				System.out.println(query);
				return null;
			}
			
			//	execute query
			sqr = this.io.executeSelectQuery(query.toString(), true);
			
			//	read data
			stats = new DcStatistics(statFields.toStringArray(), lastUpdate);
			while (sqr.next()) {
				StringTupel st = new StringTupel();
				for (int f = 1 /* skip the dummy */; (f < sqr.getColumnCount()) && ((f-1) < statFields.size()); f++)
					st.setValue(statFields.get(f-1), sqr.getString(f));
				stats.addElement(st);
				//	catch databases that don't support the limit clause
				if ((limit > 0) && (stats.size() >= limit))
					break;
			}
			
			//	cache statistics
			this.cache.put(statsCacheKey, stats);
			
			//	finally ...
			return stats;
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
		
		//	clear cache & update timestamp
		this.cache.clear();
		this.lastUpdate = System.currentTimeMillis();
		
		//	trigger update for static exports
		this.updateStaticStatExports(false);
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
	
	private static final int initCacheSize = 128;
	private static final int maxCacheSize = 256;
	private Map cache = Collections.synchronizedMap(new LinkedHashMap(initCacheSize, 0.9f, true) {
		protected boolean removeEldestEntry(Entry eldest) {
			return (this.size() > maxCacheSize);
		}
	});
	
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
		
		//	trigger update for static exports
		this.updateStaticStatExports(false);
	}
	
	private static class Predicate {
		private LinkedList positives;
		private LinkedList negatives;
		void addInterval(Interval interval) {
			if (interval.isNegative) {
				if (this.negatives == null)
					this.negatives = new LinkedList();
				this.negatives.add(interval);
			}
			else {
				if (this.positives == null)
					this.positives = new LinkedList();
				this.positives.add(interval);
			}
		}
		boolean isEmpty() {
			return ((this.positives == null) && (this.negatives == null));
		}
		String getSql(String field) {
			if (this.isEmpty())
				return "1=1";
			StringBuffer sql = new StringBuffer();
			if (this.negatives == null) {
				if (this.positives.size() > 1)
					sql.append("(");
				for (Iterator iit = this.positives.iterator(); iit.hasNext();) {
					Interval pos = ((Interval) iit.next());
					sql.append(pos.getSql(field));
					if (iit.hasNext())
						sql.append(" OR ");
				}
				if (this.positives.size() > 1)
					sql.append(")");
			}
			else if (this.positives == null) {
				if (this.negatives.size() > 1)
					sql.append("(");
				for (Iterator iit = this.negatives.iterator(); iit.hasNext();) {
					Interval neg = ((Interval) iit.next());
					sql.append(neg.getSql(field));
					if (iit.hasNext())
						sql.append(" AND ");
				}
				if (this.negatives.size() > 1)
					sql.append(")");
			}
			else {
				sql.append("(");
				if (this.positives.size() > 1)
					sql.append("(");
				for (Iterator iit = this.positives.iterator(); iit.hasNext();) {
					Interval pos = ((Interval) iit.next());
					sql.append(pos.getSql(field));
					if (iit.hasNext())
						sql.append(" OR ");
				}
				if (this.positives.size() > 1)
					sql.append(")");
				for (Iterator iit = this.negatives.iterator(); iit.hasNext();) {
					Interval neg = ((Interval) iit.next());
					sql.append(" AND ");
					sql.append(neg.getSql(field));
				}
				sql.append(")");
			}
			return sql.toString();
		}
	}
	
	private static class Interval {
		final String left;
		final String right;
		final boolean isNumeric;
		boolean isNegative;
		Interval(int left, int right) {
			if (right < left) {
				this.left = ("" + right);
				this.right = ("" + left);
			}
			else {
				this.left = ((left == Integer.MIN_VALUE) ? null : ("" + left));
				this.right = ((right == Integer.MAX_VALUE) ? null : ("" + right));
			}
			this.isNumeric = true;
		}
		Interval(double left, double right) {
			if (right < left) {
				this.left = ("" + right);
				this.right = ("" + left);
			}
			else {
				this.left = ((left == Double.NEGATIVE_INFINITY) ? null : ("" + left));
				this.right = ((right == Double.POSITIVE_INFINITY) ? null : ("" + right));
			}
			this.isNumeric = true;
		}
		Interval(String left, String right) {
			if ((left != null) && (right != null) && (right.compareTo(left) < 0)) {
				this.left = right;
				this.right = left;
			}
			else {
				this.left = left;
				this.right = right;
			}
			this.isNumeric = false;
		}
		String getSql(String field) {
			if (this.isNegative) {
				if (this.isNumeric) {
					if (this.left == null)
						return ("(" + field + " > " + this.right + ")");
					else if (this.right == null)
						return ("(" + field + " < " + this.left + ")");
					else if (this.left.equals(this.right))
						return ("(" + field + " <> " + this.left + ")");
					else return ("((" + field + " < " + this.left + ") OR (" + field + " > " + this.right + "))");
				}
				else {
					if (this.left == null)
						return ("(" + field + " > '" + EasyIO.sqlEscape(this.right) + "')");
					else if (this.right == null)
						return ("(" + field + " < '" + EasyIO.sqlEscape(this.left) + "')");
					else if (this.left.equals(this.right)) {
						if (this.left.indexOf('%') == -1)
							return ("(" + field + " <> '" + EasyIO.sqlEscape(this.left) + "')");
						else return ("(" + field + " NOT LIKE '" + EasyIO.prepareForLIKE(this.left) + "%')");
					}
					else return ("((" + field + " < '" + EasyIO.sqlEscape(this.left) + "') OR (" + field + " > '" + EasyIO.sqlEscape(this.right) + "'))");
				}
			}
			else {
				if (this.isNumeric) {
					if (this.left == null)
						return ("(" + field + " <= " + this.right + ")");
					else if (this.right == null)
						return ("(" + field + " >= " + this.left + ")");
					else if (this.left.equals(this.right))
						return ("(" + field + " = " + this.left + ")");
					else return ("((" + field + " >= " + this.left + ") AND (" + field + " <= " + this.right + "))");
				}
				else {
					if (this.left == null)
						return ("(" + field + " <= '" + EasyIO.sqlEscape(this.right) + "')");
					else if (this.right == null)
						return ("(" + field + " >= '" + EasyIO.sqlEscape(this.left) + "')");
					else if (this.left.equals(this.right)) {
						if (this.left.indexOf('%') == -1)
							return ("(" + field + " = '" + EasyIO.sqlEscape(this.left) + "')");
						else return ("(" + field + " LIKE '" + EasyIO.prepareForLIKE(this.left) + "%')");
					}
					else return ("((" + field + " >= '" + EasyIO.sqlEscape(this.left) + "') AND (" + field + " <= '" + EasyIO.sqlEscape(this.right) + "'))");
				}
			}
		}
		public String toString() {
			StringBuffer sb = new StringBuffer();
			if (this.isNegative)
				sb.append("!");
			sb.append("[");
			if (this.left != null) {
				if (this.isNumeric)
					sb.append(this.left);
				else {
					sb.append("\"");
					sb.append(this.left.replaceAll("\\\"", "\\\"\\\""));
					sb.append("\"");
				}
			}
			sb.append(",");
			if (this.right != null) {
				if (this.isNumeric)
					sb.append(this.right);
				else {
					sb.append("\"");
					sb.append(this.right.replaceAll("\\\"", "\\\"\\\""));
					sb.append("\"");
				}
			}
			sb.append("]");
			return sb.toString();
		}
	}
	
	private static Predicate parsePredicate(String predString, boolean isNumeric) {
		if (predString == null)
			return null;
		predString = predString.trim();
		if (predString.length() == 0)
			return null;
		
		//	parse predicate into intervals at (non-quoted) spaces
		LinkedHashSet intStrings = new LinkedHashSet();
		StringBuffer intBuf = new StringBuffer();
		char quoter = ((char) 0);
		for (int c = 0; c < predString.length(); c++) {
			char ch = predString.charAt(c);
			
			//	end of quotes
			if (ch == quoter) {
				quoter = ((char) 0);
				intBuf.append(ch);
			}
			
			//	start of quotes
			else if (ch == '"') {
				quoter = ch;
				intBuf.append(ch);
			}
			else if (ch == '[') {
				quoter = ']';
				intBuf.append(ch);
			}
			else if (ch == '(') {
				quoter = ')';
				intBuf.append(ch);
			}
			
			//	in quotes
			else if (quoter != 0)
				intBuf.append(ch);
			
			//	end of interval
			else if (ch < 33) {
				if (intBuf.length() != 0)
					intStrings.add(intBuf.toString());
				intBuf = new StringBuffer();
			}
			
			//	regular character
			else intBuf.append(ch);
		}
		if (intBuf.length() != 0)
			intStrings.add(intBuf.toString());
		
		//	parse intervals
		Predicate predicate = new Predicate();
		for (Iterator isit = intStrings.iterator(); isit.hasNext();) {
			String intString = ((String) isit.next());
			boolean isNegative = false;
			
			//	test if negative and cut marker
			if (intString.startsWith("!")) {
				intString = intString.substring("!".length());
				isNegative = true;
			}
			
			//	parse interval ...
			Interval interval = null;
			
			//	- in bracket notation
			if ((intString.startsWith("[") && intString.endsWith("]")) || (intString.startsWith("(") && intString.endsWith(")")))
				interval = parseBracketInterval(intString.substring("[".length(), (intString.length()-"]".length())).trim(), isNumeric);
			
			//	- in natural notation
			else interval = parseNaturalInterval(intString, isNumeric);
			
			//	set negation flag and store interval
			if (interval != null) {
				interval.isNegative = isNegative;
				predicate.addInterval(interval);
			}
		}
		
		//	finally ...
		return predicate;
	}
	
	private static Interval parseBracketInterval(String intString, boolean isNumeric) {
		
		//	parse numeric interval
		if (isNumeric) try {
			if (",".equals(intString))
				return null;
			String left;
			String right;
			if (intString.indexOf(',') == -1) {
				left = intString;
				right = intString;
			}
			else if (intString.startsWith(",")) {
				left = null;
				right = intString.substring(",".length()).trim();
			}
			else if (intString.endsWith(",")) {
				left = intString.substring(0, (intString.length() - ",".length())).trim();
				right = null;
			}
			else {
				left = intString.substring(0, intString.indexOf(',')).trim();
				right = intString.substring(intString.indexOf(',') + ",".length()).trim();
			}
			try {
				return new Interval(((left == null) ? Integer.MIN_VALUE : Integer.parseInt(left)), ((right == null) ? Integer.MAX_VALUE : Integer.parseInt(right)));
			}
			catch (NumberFormatException nfe) {
				return new Interval(((left == null) ? Double.NEGATIVE_INFINITY : Double.parseDouble(left)), ((right == null) ? Double.POSITIVE_INFINITY : Double.parseDouble(right)));
			}
		}
		catch (NumberFormatException nfe) {
			return null;
		}
		
		//	parse alphanumeric interval
		StringBuffer leftBuf = new StringBuffer();
		StringBuffer rightBuf = new StringBuffer();
		StringBuffer tokBuf = leftBuf;
		char quoter = ((char) 0);
		for (int c = 0; c < intString.length(); c++) {
			char ch = intString.charAt(c);
			
			//	end of quotes
			if (ch == quoter) {
				quoter = ((char) 0);
				tokBuf.append(ch);
			}
			
			//	start of quotes
			else if (ch == '"') {
				quoter = ch;
				tokBuf.append(ch);
			}
			
			//	in quotes
			else if (quoter != 0)
				tokBuf.append(ch);
			
			//	end of (first, left) token
			else if ((ch < 33) || ("-,<>;".indexOf(ch) != -1))
				tokBuf = rightBuf;
			
			//	regular character
			else tokBuf.append(ch);
		}
		
		//	cut and un-escape quotes
		String left = leftBuf.toString().trim();
		if (left.startsWith("\"") && left.endsWith("\"")) {
			left = left.substring("\"".length(), (left.length() - "\"".length()));
			left = left.replaceAll("\\\"\\\"", "\"");
		}
		if (left.length() == 0)
			left = null;
		String right = rightBuf.toString().trim();
		if (right.startsWith("\"") && right.endsWith("\"")) {
			right = right.substring("\"".length(), (right.length() - "\"".length()));
			right = right.replaceAll("\\\"\\\"", "\"");
		}
		if (right.length() == 0)
			right = null;
		
		//	do we have anything at all
		if ((left == null) && (right == null))
			return null;
		
		//	handle point interval
		if ((right == null) && (tokBuf == leftBuf))
			right = left;
		
		//	finally ...
		return new Interval(left, right);
	}
	
	private static Interval parseNaturalInterval(String intString, boolean isNumeric) {
		
		//	parse numeric interval
		if (isNumeric) {
			LinkedList numbers = new LinkedList();
			Matcher numberMatcher = Pattern.compile("[0-9]+(\\.[0-9]+)?").matcher(intString);
			int numberEnd = 0;
			while (numberMatcher.find(numberEnd)) {
				numbers.add(numberMatcher.group());
				numberEnd = numberMatcher.end();
			}
			if (numbers.isEmpty())
				return null;
			
			//	handle single-number intervals
			if (numbers.size() == 1) {
				
				//	<A ==> [,A]   <-A ==> [,-A]
				if (intString.startsWith("<"))
					return parseBracketInterval(("," + intString.substring("<".length())), isNumeric);
				
				//	>A ==> [A,]   >-A ==> [-A,]
				if (intString.startsWith(">"))
					return parseBracketInterval((intString.substring(">".length()) + ","), isNumeric);
				
				//	--A ==> [,-A]
				if (intString.startsWith("--"))
					return parseBracketInterval(("," + intString.substring("-".length())), isNumeric);
				
				//	A- ==> [A,]   -A- ==> [-A,]
				if (intString.endsWith("-"))
					return parseBracketInterval((intString.substring(0, (intString.length() - "-".length())) + ","), isNumeric);
				
				//	-A ==> [-A,-A]   A ==> [A,A]
				return parseBracketInterval(intString, isNumeric);
			}
			
			//	handle two-number (multi-number) intervals
			//	A-B ==> [A,B]   -A-B ==> [-A,B]   -A--B ==> [-A,-B]
			String left = ((String) numbers.getFirst());
			if (intString.startsWith("-"))
				left = ("-" + left);
			String right = ((String) numbers.getLast());
			if (intString.indexOf("--") != -1)
				right = ("-" + right);
			return parseBracketInterval((left + "," + right), isNumeric);
		}
		
		//	parse alphanumeric interval
		else {
			
			//	<A ==> [,A]
			if (intString.startsWith("<"))
				return parseBracketInterval(("," + intString.substring("<".length())), isNumeric);
			
			//	>A ==> [A,]
			if (intString.startsWith(">"))
				return parseBracketInterval((intString.substring(">".length()) + ","), isNumeric);
			
			//	-A ==> [,A]
			if (intString.startsWith("-"))
				return parseBracketInterval(("," + intString.substring("-".length())), isNumeric);
			
			//	A- ==> [A,]
			if (intString.endsWith("-"))
				return parseBracketInterval((intString.substring(0, (intString.length() - "-".length())) + ","), isNumeric);
			
			//	everything else
			return parseBracketInterval(intString, isNumeric);
		}
	}
	
	private abstract class StaticStatExport {
		final String destination;
		StaticStatExport(String destination) {
			this.destination = destination;
		}
		abstract void exportStaticStat() throws IOException;
	}
	
	private class FormattedStaticStatExport extends StaticStatExport {
		private final String format;
		
		private String csvSeparator = null;
		private String xmlRootTag = null;
		private String xmlRowTag = null;
		private String xmlFieldTag = null;
		private ArrayList xmlDerivatives;
		private String jsonVariableName = null;
		private boolean jsonIncludeFields = false;
		private boolean jsonIncludeLabels = false;
		
		private final int limit;
		private String[] outputFields;
		private String[] groupingFields;
		private String[] orderingFields;
		private Properties fieldPredicates;
		private Properties fieldAggregates;
		private Properties aggregatePredicates;
		
		FormattedStaticStatExport(String destination, String format, int limit) {
			super(destination);
			this.format = format;
			this.limit = limit;
		}
		
		void exportStaticStat() throws IOException {
			
			//	get statistics & fields
			DcStatistics stats = getStatistics(this.outputFields, this.groupingFields, this.orderingFields, this.fieldPredicates, this.fieldAggregates, this.aggregatePredicates, this.limit);
			
			//	create export file & writer
			File destCreateFile = new File(this.destination + ".exporting");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(destCreateFile), "UTF-8"));
			
			//	write statistics, dependent on format
			if ("CSV".equals(this.format))
				stats.writeAsCSV(bw, true, this.csvSeparator);
			else if ("XML".equals(this.format))
				stats.writeAsXML(bw, this.xmlRootTag, this.xmlRowTag, this.xmlFieldTag);
			else if ("JSON".equals(this.format))
				stats.writeAsJSON(bw, this.jsonVariableName, this.jsonIncludeFields, (this.jsonIncludeLabels ? fieldLabels : null), false);
			
			//	finish export
			bw.flush();
			bw.close();
			
			//	activate export result (replace old one with it)
			File destFile = new File(this.destination);
			if (destFile.exists())
				destFile.delete();
			destCreateFile.renameTo(destFile);
			
			//	export XML derivatives
			if ("XML".equals(this.format)) {
				for (int d = 0; d < this.xmlDerivatives.size(); d++) try {
					((StaticStatExport) this.xmlDerivatives.get(d)).exportStaticStat();
				}
				catch (IOException ioe) {
					System.out.println("Exception running derivative static export to '" + ((StaticStatExport) this.xmlDerivatives.get(d)).destination + "': " + ioe.getMessage());
					ioe.printStackTrace();
				}
			}
		}
	}
	
	private class DerivativeStaticStatExport extends StaticStatExport {
		private StaticStatExport parent;
		private Transformer transformer;
		DerivativeStaticStatExport(String destination, StaticStatExport parent, Transformer transformer) {
			super(destination);
			this.parent = parent;
			this.transformer = transformer;
		}
		void exportStaticStat() throws IOException {
			
			//	get source file from parent
			File sourceFile = new File(this.parent.destination);
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sourceFile), "UTF-8"));
			
			//	create export file & writer
			File destCreateFile = new File(this.destination + ".exporting");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(destCreateFile), "UTF-8"));
			
			//	do export (transform XML result of parent)
			try {
				this.transformer.transform(new StreamSource(br), new StreamResult(bw));
			}
			catch (TransformerException te) {
				System.out.println("Export to '" + this.destination + "' failed: " + te.getMessage());
				te.printStackTrace(System.out);
			}
			
			//	finish export
			bw.flush();
			bw.close();
			
			//	activate export result (replace old one with it)
			File destFile = new File(this.destination);
			if (destFile.exists())
				destFile.delete();
			destCreateFile.renameTo(destFile);
			
		}
	}
	
	private FormattedStaticStatExport[] loadStaticStatExports() throws IOException {
		BufferedReader sseBr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(this.dataPath, "staticExports.xml")), "UTF-8"));
		final ArrayList sseList = new ArrayList();
		StatFieldSet.parser.stream(sseBr, new TokenReceiver() {
			private FormattedStaticStatExport fsse = null;
			private StringVector outputFields = null;
			private StringVector groupingFields = null;
			private StringVector orderingFields = null;
			private Properties fieldPredicates = null;
			private Properties fieldAggregates = null;
			private Properties aggregatePredicates = null;
			public void storeToken(String token, int treeDepth) throws IOException {
				if (!StatFieldSet.grammar.isTag(token))
					return;
				String type = StatFieldSet.grammar.getType(token);
				
				if ("export".equals(type)) {
					
					if (StatFieldSet.grammar.isEndTag(token)) {
						if (this.fsse != null) {
							this.fsse.outputFields = this.outputFields.toStringArray();
							this.fsse.groupingFields = this.groupingFields.toStringArray();
							this.fsse.orderingFields = this.orderingFields.toStringArray();
							this.fsse.fieldPredicates = this.fieldPredicates;
							this.fsse.fieldAggregates = this.fieldAggregates;
							this.fsse.aggregatePredicates = this.aggregatePredicates;
							sseList.add(this.fsse);
						}
						this.fsse = null;
						this.outputFields = null;
						this.groupingFields = null;
						this.orderingFields = null;
						this.fieldPredicates = null;
						this.fieldAggregates = null;
						this.aggregatePredicates = null;
					}
					
					else {
						TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, StatFieldSet.grammar);
						String destination = tnas.getAttribute("destination");
						String format = tnas.getAttribute("format");
						String limitStr = tnas.getAttribute("limit", "-1");
						if ((destination == null) || (format == null) || !limitStr.matches("\\-?[0-9]+"))
							return;
						this.fsse = new FormattedStaticStatExport(destination, format, Integer.parseInt(limitStr));
						if ("CSV".equals(this.fsse.format))
							this.fsse.csvSeparator = tnas.getAttribute("separator", ",");
						else if ("XML".equals(this.fsse.format)) {
							this.fsse.xmlRootTag = tnas.getAttribute("rootTag", "statistics");
							this.fsse.xmlRowTag = tnas.getAttribute("rowTag", "statData");
							this.fsse.xmlFieldTag = tnas.getAttribute("fieldTag", "statField");
							this.fsse.xmlDerivatives = new ArrayList(2);
						}
						else if ("JSON".equals(this.fsse.format)) {
							this.fsse.jsonVariableName = tnas.getAttribute("variableName");
							this.fsse.jsonIncludeFields = "true".equals(tnas.getAttribute("includeFields", "true"));
							this.fsse.jsonIncludeLabels = "true".equals(tnas.getAttribute("includeLabels", "true"));
						}
						this.outputFields = new StringVector();
						this.groupingFields = new StringVector();
						this.orderingFields = new StringVector();
						this.fieldPredicates = new Properties();
						this.fieldAggregates = new Properties();
						this.aggregatePredicates = new Properties();
					}
				}
				
				else if (this.fsse == null)
					return;
				
				else if ("field".equals(type)) {
					TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, StatFieldSet.grammar);
					String name = tnas.getAttribute("name");
					if (name == null)
						return;
					boolean isOutput = "true".equals(tnas.getAttribute("output", "true"));
					if (isOutput)
						this.outputFields.addElement(name);
					String predicate = tnas.getAttribute("predicate");
					if (predicate != null)
						this.fieldPredicates.setProperty(name, predicate);
					String aggregate = tnas.getAttribute("aggregate");
					if (";count;count-distinct;min;max;sum;avg;".indexOf(";" + aggregate.toLowerCase() + ";") != -1)
						this.fieldAggregates.setProperty(name, aggregate);
					String aggregatePredicate = tnas.getAttribute("aggregatePredicate");
					if (aggregatePredicate != null)
						this.aggregatePredicates.setProperty(name, aggregatePredicate);
					String sortOrder = tnas.getAttribute("sort");
					if (sortOrder != null)
						this.orderingFields.addElement(("desc".equalsIgnoreCase(sortOrder) ? "-" : "") + name);
				}
				
				else if ("derivative".equals(type) && "XML".equals(this.fsse.format)) {
					TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, StatFieldSet.grammar);
					String destination = tnas.getAttribute("destination");
					if (destination == null)
						return;
					String xslt = tnas.getAttribute("xslt");
					if (xslt != null) try {
						Transformer transformer = XsltUtils.getTransformer(new File(dataPath, xslt));
						this.fsse.xmlDerivatives.add(new DerivativeStaticStatExport(destination, this.fsse, transformer));
					}
					catch (IOException ioe) {
						System.out.println("Exception creating export to '" + destination + "': " + ioe.getMessage());
						ioe.printStackTrace(System.out);
					}
				}
			}
			public void close() throws IOException {}
		});
		
		return ((FormattedStaticStatExport[]) sseList.toArray(new FormattedStaticStatExport[sseList.size()]));
	}
	
	private class StaticStatExportThread extends Thread {
		StaticStatExportThread() {
			staticStatExportThread = this;
			this.start();
		}
		public void run() {
			try {
				
				//	wait until exports are due (might move on as new updates happen)
				while (true) {
					if (staticStatExportsDue < 0)
						return;
					long time = System.currentTimeMillis();
					if (staticStatExportsDue <= time)
						break;
					else try {
						sleep(time - staticStatExportsDue);
					} catch (InterruptedException ie) {}
				}
				
				//	do exports
				for (int x = 0; x < staticStatExports.length; x++) try {
					staticStatExports[x].exportStaticStat();
				}
				catch (Exception e) {
					System.out.println("Exception running static export to '" + staticStatExports[x].destination + "': " + e.getMessage());
					e.printStackTrace(System.out);
				}
			}
			finally {
				staticStatExportThread = null;
			}
		}
	}
	
	private void updateStaticStatExports(boolean immediately) {
		this.staticStatExportsDue = (System.currentTimeMillis() + (immediately ? 0 : (1000 * 60 * 2)));
		if (this.staticStatExportThread == null)
			this.staticStatExportThread = new StaticStatExportThread();
	}
//	
//	//	TEST FOR STATIC EXPORTS
//	public static void main(String[] args) throws Exception {
//		
//	}
//	
//	
//	//	TEST FOR PREDICATE PARSING
//	public static void main(String[] args) throws Exception {
//		Predicate np = parsePredicate("1990.10-2010 (1990, 2010) \"1990 -2010\" -100 -100--50 --100 !2000", true);
//		System.out.println(np.getSql("MyNumber"));
//		Predicate sp = parsePredicate("A-Z \"A-Z\" -N !\"Fungi%\" -N- N-", false);
//		System.out.println(sp.getSql("MyString"));
//	}
//	
//	//	TEST FOR DATA EXTRACTION FROM DOCUMENTS
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