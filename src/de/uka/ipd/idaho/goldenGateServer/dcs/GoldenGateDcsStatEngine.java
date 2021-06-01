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
 *     * Neither the name of the Universitaet Karlsruhe (TH) / KIT nor the
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
package de.uka.ipd.idaho.goldenGateServer.dcs;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.gPath.GPathVariableResolver;
import de.uka.ipd.idaho.gamta.util.gPath.types.GPathObject;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * @author sautter
 */
public class GoldenGateDcsStatEngine implements GoldenGateDcsConstants {
	
	private static final String DOCUMENT_ID_HASH_ATTRIBUTE = "docIdHash";
	
	private String statName;
	
	private StatFieldSet fieldSet;
	private StatFieldGroup[] fieldGroups;
	private StatFieldGroup docFieldGroup;
	
	private IoProvider io;
	
	private String tableNamePrefix;
	
	private Properties fieldGroupsToTables = new Properties();
	private Properties fieldGroupsToTableAliases = new Properties();
	
	private TreeMap fieldGroupsByName = new TreeMap(String.CASE_INSENSITIVE_ORDER);
	private TreeMap fieldsByFullName = new TreeMap(String.CASE_INSENSITIVE_ORDER);
	
	private long lastUpdate = System.currentTimeMillis();
	
	/** Constructor
	 * @param statName the name of the statistics maintained by the engine
	 * @param fieldSet the statistics field set defining the data tables
	 * @param io the IO provider to use for data storage
	 * @param tableNamePrefix the name prefix for database tables
	 */
	public GoldenGateDcsStatEngine(String statName, StatFieldSet fieldSet, IoProvider io, String tableNamePrefix) {
		this(statName, fieldSet, io, tableNamePrefix, true);
	}
	
	/** Constructor
	 * @param statName the name of the statistics maintained by the engine
	 * @param fieldSet the statistics field set defining the data tables
	 * @param io the IO provider to use for data storage
	 * @param tableNamePrefix the name prefix for database tables
	 * @param useForeignKeyConstraints use explicit foreign key constraints on
	 *            database tables?
	 */
	public GoldenGateDcsStatEngine(String statName, StatFieldSet fieldSet, IoProvider io, String tableNamePrefix, boolean useForeignKeyConstraints) {
		this.statName = statName;
		this.fieldSet = fieldSet;
		this.io = io;
		this.tableNamePrefix = tableNamePrefix;
		
		//	load field set
		this.fieldGroups = this.fieldSet.getFieldGroups();
		for (int g = 0; g < this.fieldGroups.length; g++) {
			this.fieldGroupsByName.put(this.fieldGroups[g].name, this.fieldGroups[g]);
			if ("doc".equals(this.fieldGroups[g].name))
				this.docFieldGroup = this.fieldGroups[g];
			StatField[] fgFields = this.fieldGroups[g].getFields();
			for (int f = 0; f < fgFields.length; f++)
				this.fieldsByFullName.put(fgFields[f].fullName, fgFields[f]);
		}
		
		//	create document table
		TableDefinition docTableDef = new TableDefinition(this.tableNamePrefix + "Stats" + "Doc" + "Data");
		docTableDef.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		docTableDef.addColumn(DOCUMENT_ID_HASH_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		
		//	store table names and aliases for query construction
		this.fieldGroupsToTables.setProperty("doc", docTableDef.getTableName());
		String docTableAlias = docTableDef.getTableName().replaceAll("[^A-Z]", "").toLowerCase();
		this.fieldGroupsToTableAliases.setProperty("doc", docTableAlias);
		
		//	create other database tables
		TableDefinition[] statsTableDefs = new TableDefinition[this.fieldGroups.length];
		TreeSet usedTableAliases = new TreeSet(String.CASE_INSENSITIVE_ORDER); // need to make sure table aliases are unique
		usedTableAliases.add(docTableAlias);
		for (int g = 0; g < this.fieldGroups.length; g++) {
			
			//	find or create table definition
			if ("doc".equals(this.fieldGroups[g].name))
				statsTableDefs[g] = docTableDef;
			else if (this.fieldGroups[g].virtualTableName == null) {
				statsTableDefs[g] = new TableDefinition(this.tableNamePrefix + "Stats" + this.fieldGroups[g].name.substring(0, 1).toUpperCase() + this.fieldGroups[g].name.substring(1) + "Data");
				statsTableDefs[g].addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
				statsTableDefs[g].addColumn(DOCUMENT_ID_HASH_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
			}
			else {
				statsTableDefs[g] = new TableDefinition(this.fieldGroups[g].virtualTableName);
				if (findVirtualIdField(this.fieldGroups[g]) == null)
					statsTableDefs[g].addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 1);
			}
			
			//	store table names and aliases for query construction
			this.fieldGroupsToTables.setProperty(this.fieldGroups[g].name, statsTableDefs[g].getTableName());
			String tableAlias = statsTableDefs[g].getTableName().replaceAll("[^A-Z]", "").toLowerCase();
			if (usedTableAliases.contains(tableAlias)) {
				for (char c = 'a'; c <= 'z'; c++)
					if (!usedTableAliases.contains(tableAlias + c)) {
						tableAlias = (tableAlias + c);
						break;
					}
			}
			this.fieldGroupsToTableAliases.setProperty(this.fieldGroups[g].name, tableAlias);
			usedTableAliases.add(tableAlias);
			
			//	add data fields, and collect indexed columns
			StatField[] fgFields = this.fieldGroups[g].getFields();
			for (int f = 0; f < fgFields.length; f++) {
				if (StatField.STRING_TYPE.equals(fgFields[f].dataType))
					statsTableDefs[g].addColumn(fgFields[f].columnName, TableDefinition.VARCHAR_DATATYPE, ((this.fieldGroups[g].virtualTableName == null) ? fgFields[f].dataLength : 1));
				else if (StatField.INTEGER_TYPE.equals(fgFields[f].dataType))
					statsTableDefs[g].addColumn(fgFields[f].columnName, TableDefinition.INT_DATATYPE, 0);
				else if (StatField.LONG_TYPE.equals(fgFields[f].dataType))
					statsTableDefs[g].addColumn(fgFields[f].columnName, TableDefinition.BIGINT_DATATYPE, 0);
				else if (StatField.REAL_TYPE.equals(fgFields[f].dataType))
					statsTableDefs[g].addColumn(fgFields[f].columnName, TableDefinition.REAL_DATATYPE, 0);
				else if (StatField.BOOLEAN_TYPE.equals(fgFields[f].dataType))
					statsTableDefs[g].addColumn(fgFields[f].columnName, TableDefinition.CHAR_DATATYPE, 1);
			}
		}
		
		//	create and index document table
		if (!this.io.ensureTable(docTableDef, true))
			throw new RuntimeException("DocumentCollectionStatistics: cannot work without database access");
		this.io.indexColumn(docTableDef.getTableName(), DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(docTableDef.getTableName(), DOCUMENT_ID_HASH_ATTRIBUTE);
		if (useForeignKeyConstraints)
			this.io.setPrimaryKey(docTableDef.getTableName(), DOCUMENT_ID_ATTRIBUTE);
		
		//	add configured column indices
		StatField[] docFgFields = this.docFieldGroup.getFields();
		for (int f = 0; f < docFgFields.length; f++) {
			if (docFgFields[f].indexed)
				this.io.indexColumn(docTableDef.getTableName(), docFgFields[f].columnName);
		}
		
		//	create and index sub tables
		for (int t = 0; t < statsTableDefs.length; t++) {
			if (statsTableDefs[t] == docTableDef)
				continue;
			
			//	no need to create virtual tables (only have to check whether or not they exist)
			if (this.fieldGroups[t].virtualTableName != null) {
				if (!this.io.ensureTable(statsTableDefs[t], false))
					throw new RuntimeException("DocumentCollectionStatistics: cannot work without virtual table " + this.fieldGroups[t].virtualTableName);
				continue;
			}
			
			//	create and index table
			if (!this.io.ensureTable(statsTableDefs[t], true))
				throw new RuntimeException("DocumentCollectionStatistics: cannot work without database access");
			this.io.indexColumn(statsTableDefs[t].getTableName(), DOCUMENT_ID_ATTRIBUTE);
			this.io.indexColumn(statsTableDefs[t].getTableName(), DOCUMENT_ID_HASH_ATTRIBUTE);
			
			//	add referential constraints
			if (useForeignKeyConstraints)
				this.io.setForeignKey(statsTableDefs[t].getTableName(), DOCUMENT_ID_ATTRIBUTE, docTableDef.getTableName(), DOCUMENT_ID_ATTRIBUTE);
			
			//	add configured column indices
			StatField[] fgFields = this.fieldGroups[t].getFields();
			for (int f = 0; f < fgFields.length; f++) {
				if (fgFields[f].indexed)
					this.io.indexColumn(statsTableDefs[t].getTableName(), fgFields[f].columnName);
			}
		}
	}
	
	private Map virtualIdFieldCache = Collections.synchronizedMap(new LinkedHashMap());
	private StatField findVirtualIdField(StatFieldGroup fieldGroup) {
		if (this.virtualIdFieldCache.containsKey(fieldGroup.name))
			return ((StatField) this.virtualIdFieldCache.get(fieldGroup.name));
		StatField[] fgFields = fieldGroup.getFields();
		StatField idField = null;
		for (int f = 0; f < fgFields.length; f++)
			if (fgFields[f].isVirtualRefId) {
				idField = fgFields[f];
				break;
			}
		this.virtualIdFieldCache.put(fieldGroup.name, idField); // need to cache null as well
		return idField;
	}
	
	/**
	 * Update the data belonging to a specific document in the statistics
	 * tables underlying the engine.
	 * @param doc the document to update
	 * @throws IOException
	 */
	public void updateDocument(QueriableAnnotation doc) throws IOException {
		this.updateDocument(doc, null);
	}
	
	/**
	 * Update the data belonging to a specific document in the statistics
	 * tables underlying the engine. The argument set provides a filter for the
	 * field groups to update, with a null argument updating all field groups.
	 * The filter mechanism is helpful for reducing effort in whole-collection
	 * updates, e.g. after a modification to parts of the field set definition.
	 * @param doc the document to update
	 * @param fieldGroups a set holding the names of the field groups to update
	 * @throws IOException
	 */
	public void updateDocument(QueriableAnnotation doc, HashSet fieldGroups) throws IOException {
		
		//	get document ID
		String docId = ((String) doc.getAttribute(DOCUMENT_ID_ATTRIBUTE));
		
		//	clean up
		this.deleteDocument(docId, fieldGroups);
		
		//	assemble insert queries
		String docTableFields = (DOCUMENT_ID_ATTRIBUTE + ", " + DOCUMENT_ID_HASH_ATTRIBUTE);
		String docTableValues = ("'" + EasyIO.sqlEscape(docId) + "'" + ", " + docId.hashCode());
		
		//	get global variables
		GPathVariableResolver variables = new GPathVariableResolver();
		StatVariable[] fsVars = this.fieldSet.getVariables();
		for (int v = 0; v < fsVars.length; v++) {
			GPathObject value = fsVars[v].getValue(doc, doc, variables);
			if ((value != null) && (value.asBoolean().value))
				variables.setVariable(fsVars[v].name, value);
		}
		
		//	fill document table
		if ((fieldGroups == null) || fieldGroups.contains("doc"))
			this.updateFieldGroup(this.docFieldGroup, doc, doc, variables, docTableFields, docTableValues);
		
		//	fill sub tables
		String subTableFields = (DOCUMENT_ID_ATTRIBUTE + ", " + DOCUMENT_ID_HASH_ATTRIBUTE);
		String subTableValues = ("'" + EasyIO.sqlEscape(docId) + "'" + ", " + docId.hashCode());
		for (int g = 0; g < this.fieldGroups.length; g++) {
			if (this.fieldGroups[g] == this.docFieldGroup)
				continue; // we've done this one above
			if (this.fieldGroups[g].virtualTableName != null)
				continue; // no need to update virtual table names
			if ((fieldGroups != null) && !fieldGroups.contains(this.fieldGroups[g].name))
				continue; // filtered out
			StatVariable[] fgVars = this.fieldGroups[g].getVariables();
			QueriableAnnotation[] contexts = {doc};
			if (this.fieldGroups[g].defContext != null)
				contexts = this.fieldGroups[g].defContext.evaluate(doc, variables);
			for (int c = 0; c < contexts.length; c++) {
				GPathVariableResolver cVariables = new GPathVariableResolver(variables);
				for (int v = 0; v < fgVars.length; v++) {
					GPathObject value = fgVars[v].getValue(doc, contexts[c], cVariables);
					if ((value != null) && (value.asBoolean().value))
						cVariables.setVariable(fgVars[v].name, value);
				}
				this.updateFieldGroup(this.fieldGroups[g], doc, contexts[c], cVariables, subTableFields, subTableValues);
			}
		}
		
		//	clear cache & update timestamp
		this.cache.clear();
		this.lastUpdate = System.currentTimeMillis();
	}
	
	private void updateFieldGroup(StatFieldGroup fieldGroup, QueriableAnnotation doc, QueriableAnnotation context, GPathVariableResolver variables, String fieldsPrefix, String valuesPrefix) {
		StringBuffer fieldNames = new StringBuffer(fieldsPrefix);
		StringBuffer fieldValues = new StringBuffer(valuesPrefix);
		
		//	get field values (field group may be null if document table only contains default fields)
		if (fieldGroup != null) {
			StatField[] fields = fieldGroup.getFields();
			for (int f = 0; f < fields.length; f++) {
				GPathObject value = fields[f].getFieldValue(doc, context, variables);
				if (value != null)
					value = this.normalizeFieldValue(fields[f], value);
				if (StatField.STRING_TYPE.equals(fields[f].dataType)) {
					fieldNames.append(", " + fields[f].columnName);
					if (value == null)
						fieldValues.append(", ''");
					else {
						String str = value.asString().value;
						str = str.trim();
						str = str.replaceAll("\\s+", " ");
						if (str.length() > fields[f].dataLength)
							str = str.substring(0, fields[f].dataLength);
						fieldValues.append(", '" + EasyIO.sqlEscape(str) + "'");
					}
				}
				else if (StatField.INTEGER_TYPE.equals(fields[f].dataType)) {
					fieldNames.append(", " + fields[f].columnName);
					if ((value == null) || "NaN".equals(value.asString().value))
						fieldValues.append(", 0");
					else try {
						fieldValues.append(", " + Integer.parseInt(value.asString().value));
					}
					catch (NumberFormatException nfeInt) {
						try {
							fieldValues.append(", " + ((int) Math.round(Double.parseDouble(value.asString().value))));
						}
						catch (NumberFormatException nfeCast) {
							fieldValues.append(", 0");
						}
					}
				}
				else if (StatField.LONG_TYPE.equals(fields[f].dataType)) {
					fieldNames.append(", " + fields[f].columnName);
					if ((value == null) || "NaN".equals(value.asString().value))
						fieldValues.append(", 0");
					else try {
						fieldValues.append(", " + Long.parseLong(value.asString().value));
					}
					catch (NumberFormatException nfeInt) {
						try {
							fieldValues.append(", " + Math.round(Double.parseDouble(value.asString().value)));
						}
						catch (NumberFormatException nfeCast) {
							fieldValues.append(", 0");
						}
					}
				}
				else if (StatField.REAL_TYPE.equals(fields[f].dataType)) {
					fieldNames.append(", " + fields[f].columnName);
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
					fieldNames.append(", " + fields[f].columnName);
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
			if (this.statName == null)
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
	protected GPathObject normalizeFieldValue(StatField field, GPathObject value) {
		return value;
	}
	
	/**
	 * Delete the data belonging to a specific document from the statistics
	 * tables underlying the engine.
	 * @param docId the ID of the document to delete
	 * @throws IOException
	 */
	public void deleteDocument(String docId) throws IOException {
		this.deleteDocument(docId, null);
	}
	
	private void deleteDocument(String docId, HashSet fieldGroups) throws IOException {
		
		//	clean up sub tables first (we might have foreign keys to observe)
		for (int g = 0; g < this.fieldGroups.length; g++) {
			if ("doc".equals(this.fieldGroups[g].name))
				continue; // we'll do this one below
			if (this.fieldGroups[g].virtualTableName != null)
				continue; // no need to update virtual table name
			if ((fieldGroups != null) && !fieldGroups.contains(this.fieldGroups[g].name))
				continue; // filtered out
			String query = "DELETE FROM " + this.fieldGroupsToTables.getProperty(this.fieldGroups[g].name) + 
					" WHERE " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() + "" +
					" AND " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
					";";
			try {
				if (this.statName == null)
					System.out.println(query);
				else this.io.executeUpdateQuery(query);
			}
			catch (SQLException sqle) {
				System.out.println("DocumentCollectionStatistics: exception cleaning statistics table: " + sqle.getMessage());
				System.out.println("  Query was: " + query);
			}
		}
		
		if ((fieldGroups != null) && !fieldGroups.contains("doc"))
			return; // no need to take on document table
		
		//	clean up document table last (now that all referencing tuples are deleted)
		String query = "DELETE FROM " + this.fieldGroupsToTables.getProperty("doc") + 
				" WHERE " + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + docId.hashCode() + "" +
				" AND " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
				";";
		try {
			if (this.statName == null)
				System.out.println(query);
			else this.io.executeUpdateQuery(query);
		}
		catch (SQLException sqle) {
			System.out.println("DocumentCollectionStatistics: exception cleaning statistics table: " + sqle.getMessage());
			System.out.println("  Query was: " + query);
		}
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
	 * @param customFilters custom filters to apply to the statistics fields
	 * @return the requested statistics, packed in a string relation
	 * @throws IOException
	 */
	public DcStatistics getStatistics(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, String[] customFilters) {
		return this.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, customFilters, -1);
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
	 * @param customFilters custom filters to apply to the statistics fields
	 * @param limit the maximum number of output rows (-1 returns all rows)
	 * @return the requested statistics, packed in a string relation
	 * @throws IOException
	 */
	public DcStatistics getStatistics(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, String[] customFilters, int limit) {
		System.out.println(this.statName + ": processing query:");
		System.out.println(" - output fields: " + Arrays.toString(outputFields));
		System.out.println(" - grouping fields: " + Arrays.toString(groupingFields));
		System.out.println(" - ordering fields: " + Arrays.toString(orderingFields));
		System.out.println(" - field predicates: " + String.valueOf(fieldPredicates));
		System.out.println(" - field aggregates: " + String.valueOf(fieldAggregates));
		System.out.println(" - aggregate predicates: " + String.valueOf(aggregatePredicates));
		System.out.println(" - custom filters: " + Arrays.toString(customFilters));
		
		//	prepare parsing and SQL query assembly
		FieldListBuffer outputFieldString = new FieldListBuffer(null);
		TableListBuffer tableString = new TableListBuffer();
		StringBuffer whereString = new StringBuffer("1=1");
		FieldListBuffer groupFieldString = new FieldListBuffer(" GROUP BY ");
		StringBuffer havingString = new StringBuffer("1=1");
		FieldListBuffer orderFieldString = new FieldListBuffer(" ORDER BY ");
		HashSet filterFieldSet = new HashSet();
		
		//	parse custom filters
		ArrayList whereCustomFilters = new ArrayList();
		ArrayList havingCustomFilters = new ArrayList();
		for (int f = 0; f < customFilters.length; f++) {
			CustomFilter cf = parseCustomFilter(customFilters[f]);
			if (cf == null)
				continue;
			if ("raw".equals(cf.target))
				whereCustomFilters.add(cf);
			else if ("res".equals(cf.target))
				havingCustomFilters.add(cf);
		}
		
		//	prepare output fields
		StringVector statFields = new StringVector();
		statFields.addElement("DocCount");
		
		//	assemble grouping fields (we can order them, as grouping is commutative)
		Arrays.sort(groupingFields);
		for (int f = 0; f < groupingFields.length; f++) {
			StatField field = ((StatField) this.fieldsByFullName.get(groupingFields[f]));
			if (field == null)
				continue;
			if (groupFieldString.append(field))
				tableString.appendTableForField(field);
		}
		
		//	assemble output fields
		for (int f = 0; f < outputFields.length; f++) {
			StatField field = ((StatField) this.fieldsByFullName.get(outputFields[f]));
			if (field == null)
				continue;
			if (outputFieldString.containsField(field))
				continue;
			
			if (groupFieldString.containsField(field))
				outputFieldString.append(field);
			else {
				String aggregate = fieldAggregates.getProperty(field.fullName, field.defAggregate).toLowerCase();
				if (("sum".equals(aggregate) || "avg".equals(aggregate)) && !StatField.INTEGER_TYPE.equals(field.dataType) && !StatField.LONG_TYPE.equals(field.dataType) && !StatField.REAL_TYPE.equals(field.dataType))
					continue;
				outputFieldString.append(field, aggregate);
			}
			tableString.appendTableForField(field);
			statFields.addElement(field.statColName);
		}
		
		//	assemble ordering fields
		for (int f = 0; f < orderingFields.length; f++) {
			String fieldName = orderingFields[f];
			boolean defaultOrder = true;
			if (fieldName.startsWith("-")) {
				defaultOrder = false;
				fieldName = fieldName.substring("-".length());
			}
			StatField field = ((StatField) this.fieldsByFullName.get(fieldName));
			if (field == null)
				continue;
			if (orderFieldString.containsField(field))
				continue;
			if (!outputFieldString.containsField(field))
				continue;
			String dataType = field.dataType;
			if (!groupFieldString.containsField(field)) {
				String aggregate = fieldAggregates.getProperty(field.fullName, field.defAggregate);
				if ("count".equals(aggregate) || "count-distinct".equals(aggregate))
					dataType = StatField.INTEGER_TYPE;
			}
			orderFieldString.append(field, ((StatField.STRING_TYPE.equals(dataType) == defaultOrder) ? " ASC" : " DESC"));
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
					long l = Long.MIN_VALUE;
					try {
						l = Long.parseLong(predicateParts[p]);
					} catch (NumberFormatException nfe) {}
					Double d = Double.NEGATIVE_INFINITY;
					try {
						d = Double.parseDouble(predicateParts[p].replaceAll("\\,", "."));
					} catch (NumberFormatException nfe) {}
					StringBuffer predicatePartWhere = new StringBuffer("1=0");
					for (int f = 0; f < fields.length; f++) {
						if (StatField.STRING_TYPE.equals(fields[f].dataType))
							predicatePartWhere.append(" OR " + this.getQualifiedFieldName(fields[f]) + " LIKE '" + EasyIO.sqlEscape(predicateParts[p]) + "'");
						else if (StatField.INTEGER_TYPE.equals(fields[f].dataType) && (i != Integer.MIN_VALUE))
							predicatePartWhere.append(" OR " + this.getQualifiedFieldName(fields[f]) + " = " + i + "");
						else if (StatField.LONG_TYPE.equals(fields[f].dataType) && (l != Long.MIN_VALUE))
							predicatePartWhere.append(" OR " + this.getQualifiedFieldName(fields[f]) + " = " + i + "");
						else if (StatField.REAL_TYPE.equals(fields[f].dataType) && (d != Double.NEGATIVE_INFINITY))
							predicatePartWhere.append(" OR " + this.getQualifiedFieldName(fields[f]) + " = " + d + "");
					}
					if (predicatePartWhere.length() > "1=0".length())
						whereString.append(" AND (" + predicatePartWhere.toString() + ")");
				}
				tableString.appendTableForFieldGroup(fieldGroup);
				continue;
			}
			
			StatField field = ((StatField) this.fieldsByFullName.get(predicateTargetName));
			if (field == null)
				continue;
			String predicateString = fieldPredicates.getProperty(field.fullName);
			if ((predicateString == null) || (predicateString.trim().length() == 0))
				continue;
			Predicate predicate = parsePredicate(predicateString, (StatField.INTEGER_TYPE.equals(field.dataType) || StatField.LONG_TYPE.equals(field.dataType) || StatField.REAL_TYPE.equals(field.dataType)));
			if (predicate.isEmpty()) {
				System.out.println(this.statName + ": empty predicate for " + field.dataType);
				System.out.println("  input string was " + predicateString);
				continue;
			}
			if (!filterFieldSet.add(field.fullName))
				continue;
			whereString.append(" AND " + predicate.getSql(this.getQualifiedFieldName(field)));
			tableString.appendTableForField(field);
		}
		for (int f = 0; f < whereCustomFilters.size(); f++) {
			CustomFilter cf = ((CustomFilter) whereCustomFilters.get(f));
			StatField leftField = ((StatField) this.fieldsByFullName.get(cf.leftField));
			if (leftField == null)
				continue;
			StatField rightField = ((StatField) this.fieldsByFullName.get(cf.rightField));
			if (rightField == null)
				continue;
			whereString.append(" AND " + this.getQualifiedFieldName(leftField) + " " + cf.operator + " " + this.getQualifiedFieldName(rightField));
			tableString.appendTableForField(leftField);
			tableString.appendTableForField(rightField);
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
					System.out.println(this.statName + ": empty predicate for DocCount");
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
			String aggregate = fieldAggregates.getProperty(field.fullName, field.defAggregate).toLowerCase();
			if (("sum".equals(aggregate) || "avg".equals(aggregate)) && !StatField.INTEGER_TYPE.equals(field.dataType) && !StatField.LONG_TYPE.equals(field.dataType) && !StatField.REAL_TYPE.equals(field.dataType))
				continue;
			String aggregateDataType = field.dataType;
			if ("count".equals(aggregate) || "count-distinct".equals(aggregate))
				aggregateDataType = StatField.INTEGER_TYPE;
			else if ("sum".equals(aggregate) || "avg".equals(aggregate))
				aggregateDataType = (StatField.REAL_TYPE.equals(field.dataType) ? StatField.REAL_TYPE : (StatField.LONG_TYPE.equals(field.dataType) ? StatField.LONG_TYPE : StatField.INTEGER_TYPE));
			Predicate predicate = parsePredicate(predicateString, (StatField.INTEGER_TYPE.equals(aggregateDataType) || StatField.LONG_TYPE.equals(aggregateDataType) || StatField.REAL_TYPE.equals(aggregateDataType)));
			if (predicate.isEmpty()) {
				System.out.println(this.statName + ": empty predicate for " + aggregateDataType);
				System.out.println("  input string was " + predicateString);
				continue;
			}
			String aggreateField = ("count-distinct".equals(aggregate) ? ("count(DISTINCT " + this.getQualifiedFieldName(field) + ")") : (aggregate + "(" + this.getQualifiedFieldName(field) + ")"));
			havingString.append(" AND " + predicate.getSql(aggreateField));
			tableString.appendTableForField(field);
		}
		for (int f = 0; f < havingCustomFilters.size(); f++) {
			CustomFilter cf = ((CustomFilter) havingCustomFilters.get(f));
			
			StatField leftField = ((StatField) this.fieldsByFullName.get(cf.leftField));
			if (leftField == null)
				continue;
			String leftAggregate = fieldAggregates.getProperty(leftField.fullName, leftField.defAggregate).toLowerCase();
			if (("sum".equals(leftAggregate) || "avg".equals(leftAggregate)) && !StatField.INTEGER_TYPE.equals(leftField.dataType) && !StatField.LONG_TYPE.equals(leftField.dataType) && !StatField.REAL_TYPE.equals(leftField.dataType))
				continue;
			String leftAggregateDataType = leftField.dataType;
			if ("count".equals(leftAggregate) || "count-distinct".equals(leftAggregate))
				leftAggregateDataType = StatField.INTEGER_TYPE;
			else if ("sum".equals(leftAggregate) || "avg".equals(leftAggregate))
				leftAggregateDataType = (StatField.REAL_TYPE.equals(leftField.dataType) ? StatField.REAL_TYPE : (StatField.LONG_TYPE.equals(leftField.dataType) ? StatField.LONG_TYPE : StatField.INTEGER_TYPE));
			
			StatField rightField = ((StatField) this.fieldsByFullName.get(cf.rightField));
			if (rightField == null)
				continue;
			String rightAggregate = fieldAggregates.getProperty(rightField.fullName, rightField.defAggregate).toLowerCase();
			if (("sum".equals(rightAggregate) || "avg".equals(rightAggregate)) && !StatField.INTEGER_TYPE.equals(rightField.dataType) && !StatField.LONG_TYPE.equals(rightField.dataType) && !StatField.REAL_TYPE.equals(rightField.dataType))
				continue;
			String rightAggregateDataType = rightField.dataType;
			if ("count".equals(rightAggregate) || "count-distinct".equals(rightAggregate))
				rightAggregateDataType = StatField.INTEGER_TYPE;
			else if ("sum".equals(rightAggregate) || "avg".equals(rightAggregate))
				rightAggregateDataType = (StatField.REAL_TYPE.equals(rightField.dataType) ? StatField.REAL_TYPE : (StatField.LONG_TYPE.equals(rightField.dataType) ? StatField.LONG_TYPE : StatField.INTEGER_TYPE));
			
			if ((StatField.INTEGER_TYPE.equals(leftAggregateDataType) || StatField.LONG_TYPE.equals(leftAggregateDataType) || StatField.REAL_TYPE.equals(leftAggregateDataType)) != (StatField.INTEGER_TYPE.equals(rightAggregateDataType) || StatField.LONG_TYPE.equals(rightAggregateDataType) || StatField.REAL_TYPE.equals(rightAggregateDataType)))
				continue;
			String leftAggreateField = ("count-distinct".equals(leftAggregate) ? ("count(DISTINCT " + this.getQualifiedFieldName(leftField) + ")") : (leftAggregate + "(" + this.getQualifiedFieldName(leftField) + ")"));
			String rightAggreateField = ("count-distinct".equals(rightAggregate) ? ("count(DISTINCT " + this.getQualifiedFieldName(rightField) + ")") : (rightAggregate + "(" + this.getQualifiedFieldName(rightField) + ")"));
			
			havingString.append(" AND " + leftAggreateField + " " + cf.operator + " " + rightAggreateField);
			tableString.appendTableForField(leftField);
			tableString.appendTableForField(rightField);
		}
		
		/* produce cache key TODO increase hit rate by exploiting commutative properties
		 * - HEX hash of output field string (includes aggregates)
		 * - HEX hash of ORDER field string
		 * - HEX hash of sorted WHERE string (predicates are commutative TODO use it)
		 * - HEX hash of sorted GROUP field string (grouping is commutative TODO use it)
		 * - HEX hash of sorted HAVING string (predicates are commutative TODO use it)
		 */
		String statsCacheKey = "" + limit +
				"-" + Integer.toString(outputFieldString.getFieldListHash(), 16) +
				"-" + Integer.toString(orderFieldString.getFieldListHash(), 16) +
				"-" + Integer.toString(whereString.toString().hashCode(), 16) +
				"-" + Integer.toString(groupFieldString.getFieldListHash(), 16) +
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
				" AND " + tableString.getJoinWhereString() +
				groupFieldString.toString() +
				" HAVING " + havingString.toString() +
				orderFieldString.toString() +
				limitClause +
				";";
		System.out.println(this.statName + ": stats query is " + query);
		
		//	produce statistics
		SqlQueryResult sqr = null;
		try {
			
			//	we're testing the query parser ...
			if (this.statName == null) {
				System.out.println(query);
				return null;
			}
			
			//	execute query
			sqr = this.io.executeSelectQuery(query.toString(), true);
			
			//	read data
			stats = new DcStatistics(statFields.toStringArray(), lastUpdate);
			while (sqr.next()) {
				StringTupel st = new StringTupel();
				for (int f = 0; (f < sqr.getColumnCount()) && (f < statFields.size()); f++)
					st.setValue(statFields.get(f), sqr.getString(f));
				stats.addElement(st);
				//	catch databases that don't support the limit clause
				if ((limit > 0) && (stats.size() >= limit))
					break;
			}
			
			//	lock and cache statistics
			stats.setReadOnly();
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
	
	private String getQualifiedFieldName(StatField field) {
		return (this.fieldGroupsToTableAliases.getProperty(field.group.name) + "." + field.columnName);
	}
	
	private class FieldListBuffer {
		private StringBuffer sb = null;
		private HashSet fields = new HashSet();
		private String fieldListPrefix;
		FieldListBuffer(String fieldListPrefix) {
			this.fieldListPrefix = fieldListPrefix;
			if (this.fieldListPrefix == null)
				this.sb = new StringBuffer("count(DISTINCT " + fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE + ") AS DocCount");
		}
		boolean append(StatField field) {
			return this.append(field, null);
		}
		boolean append(StatField field, String aggregateOrOrder) {
			if (!this.fields.add(field.fullName))
				return false;
			
			//	make sure of separator
			if (this.sb == null)
				this.sb = new StringBuffer();
			else this.sb.append(", ");
			
			//	SELECT clause
			if (this.fieldListPrefix == null) {
				if (aggregateOrOrder == null)
					this.sb.append(getQualifiedFieldName(field));
				else if ("count-distinct".equals(aggregateOrOrder))
					this.sb.append("count(DISTINCT " + getQualifiedFieldName(field) + ")");
				else this.sb.append(aggregateOrOrder + "(" + getQualifiedFieldName(field) + ")");
				this.sb.append(" AS " + field.statColName);
			}
			
			//	GROUP BY clause
			else if (" GROUP BY ".equals(this.fieldListPrefix))
				this.sb.append(getQualifiedFieldName(field));
			
			//	ORDER BY clause
			else this.sb.append(field.statColName + ((aggregateOrOrder == null) ? "" : aggregateOrOrder));
			
			//	indicate we did something
			return true;
		}
		boolean containsField(StatField field) {
			return this.fields.contains(field.fullName);
		}
		public String toString() {
			return ((this.sb == null) ? "" : (((this.fieldListPrefix == null) ? "" : this.fieldListPrefix) + this.sb.toString()));
		}
		int getFieldListHash() {
			return ((this.sb == null) ? 0 : this.sb.toString().hashCode());
		}
	}
	
	private class TableListBuffer {
		private StringBuffer sb;
		private HashSet tables = new HashSet();
		private StringBuffer joinWhere = new StringBuffer("1=1");
		TableListBuffer() {
			this.sb = new StringBuffer(fieldGroupsToTables.getProperty("doc") + " " + fieldGroupsToTableAliases.getProperty("doc"));
			this.tables.add("doc");
		}
		void appendTableForField(StatField field) {
			this.appendTableForFieldGroup(field.group);
		}
		void appendTableForFieldGroup(StatFieldGroup fieldGroup) {
			if (!this.tables.add(fieldGroup.name))
				return;
			this.sb.append(", " + fieldGroupsToTables.getProperty(fieldGroup.name) + " " + fieldGroupsToTableAliases.getProperty(fieldGroup.name));
			if (fieldGroup.virtualTableName == null) {
				this.joinWhere.append(" AND " + fieldGroupsToTableAliases.getProperty(fieldGroup.name) + "." + DOCUMENT_ID_ATTRIBUTE + " = " + fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE);
				this.joinWhere.append(" AND " + fieldGroupsToTableAliases.getProperty(fieldGroup.name) + "." + DOCUMENT_ID_HASH_ATTRIBUTE + " = " + fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_HASH_ATTRIBUTE);
			}
			else /* ID hash not necessarily available in virtual table */ {
				StatField idField = findVirtualIdField(fieldGroup);
				if (fieldGroup.refIdFieldName == null)
					this.joinWhere.append(" AND " + fieldGroupsToTableAliases.getProperty(fieldGroup.name) + "." + ((idField == null) ? DOCUMENT_ID_ATTRIBUTE : idField.columnName) + " = " + fieldGroupsToTableAliases.getProperty("doc") + "." + DOCUMENT_ID_ATTRIBUTE);
				else {
					StatField refIdField = ((StatField) fieldsByFullName.get(fieldGroup.refIdFieldName));
					this.appendTableForField(refIdField);
					this.joinWhere.append(" AND " + fieldGroupsToTableAliases.getProperty(fieldGroup.name) + "." + ((idField == null) ? DOCUMENT_ID_ATTRIBUTE : idField.columnName) + " = " + fieldGroupsToTableAliases.getProperty(refIdField.group.name) + "." + refIdField.columnName);
				}
			}
		}
		public String toString() {
			return this.sb.toString();
		}
		String getJoinWhereString() {
			StringBuffer sfgJoinWhere = new StringBuffer();
			for (Iterator sfgnit = this.tables.iterator(); sfgnit.hasNext();) {
				StatFieldGroup sfg = ((StatFieldGroup) fieldGroupsByName.get(sfgnit.next()));
				StatFieldGroupJoin[] sfgjs = sfg.getJoins();
				if (sfgjs.length == 0)
					continue;
				for (int j = 0; j < sfgjs.length; j++) {
					StatField sf = ((StatField) fieldsByFullName.get(sfg.name + "." + sfgjs[j].fieldName));
					if (sf == null)
						continue; // some configuration error
					StatField jsf = ((StatField) fieldsByFullName.get(sfgjs[j].joinFieldName));
					if (jsf == null)
						continue; // some configuration error
					if (!this.tables.contains(jsf.group.name))
						continue; // other table not in query
					sfgJoinWhere.append(" AND " + getQualifiedFieldName(sf) + " = " + getQualifiedFieldName(jsf));
				}
			}
			return (this.joinWhere.toString() + sfgJoinWhere.toString());
		}
	}
	
	private static final int initCacheSize = 128;
	private static final int maxCacheSize = 256;
	private Map cache = Collections.synchronizedMap(new LinkedHashMap(initCacheSize, 0.9f, true) {
		protected boolean removeEldestEntry(Entry eldest) {
			return (this.size() > maxCacheSize);
		}
	});
	
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
//						if (this.left.indexOf('%') == -1)
//							return ("(" + field + " <> '" + EasyIO.sqlEscape(this.left) + "')");
//						else return ("(" + field + " NOT LIKE '" + EasyIO.prepareForLIKE(this.left) + "%')");
						if (this.left.indexOf('%') != -1)
							return ("(" + field + " NOT LIKE '" + EasyIO.prepareForLIKE(this.left) + "%')");
						else if (this.left.indexOf('_') != -1)
							return ("(" + field + " NOT LIKE '" + EasyIO.prepareForLIKE(this.left) + "')");
						else return ("(" + field + " <> '" + EasyIO.sqlEscape(this.left) + "')");
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
//						if (this.left.indexOf('%') == -1)
//							return ("(" + field + " = '" + EasyIO.sqlEscape(this.left) + "')");
//						else return ("(" + field + " LIKE '" + EasyIO.prepareForLIKE(this.left) + "%')");
						if (this.left.indexOf('%') != -1)
							return ("(" + field + " LIKE '" + EasyIO.prepareForLIKE(this.left) + "%')");
						else if (this.left.indexOf('_') != -1)
							return ("(" + field + " LIKE '" + EasyIO.prepareForLIKE(this.left) + "')");
						else return ("(" + field + " = '" + EasyIO.sqlEscape(this.left) + "')");
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
	
	private static class CustomFilter {
		final String target;
		final String leftField;
		final String operator;
		final String rightField;
		CustomFilter(String target, String leftField, String operator, String rightField) {
			this.target = target;
			this.leftField = leftField;
			this.operator = operator;
			this.rightField = rightField;
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
			
			//	in quotes
			else if (quoter != 0)
				intBuf.append(ch);
			
			//	start of quotes
			else if (ch == '"') {
				quoter = '"';
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
				quoter = '"';
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
	
	private static CustomFilter parseCustomFilter(String customFilter) {
		String[] cfData = customFilter.split("\\s");
		if (cfData.length != 4)
			return null;
		String target = cfData[0];
		if (!"raw".equals(target) && !"res".equals(target))
			return null;
		String operator = cfData[2];
		if ("lt".equals(operator))
			operator = "<";
		else if ("le".equals(operator))
			operator = "<=";
		else if ("eq".equals(operator))
			operator = "=";
		else if ("ne".equals(operator))
			operator = "<>";
		else if ("ge".equals(operator))
			operator = ">=";
		else if ("gt".equals(operator))
			operator = ">";
		else return null;
		String leftField = cfData[1];
		String rightField = cfData[3];
		return new CustomFilter(target, leftField, operator, rightField);
	}
}