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
package de.uka.ipd.idaho.goldenGateServer.exp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Properties;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.gamta.util.gPath.GPath;
import de.uka.ipd.idaho.gamta.util.gPath.GPathExpression;
import de.uka.ipd.idaho.gamta.util.gPath.GPathParser;
import de.uka.ipd.idaho.gamta.util.gPath.exceptions.GPathException;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerActivityLogger;
import de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP;
import de.uka.ipd.idaho.stringUtils.StringUtils;

/**
 * GoldenGATE Server Exporter (EXP) is a convenience super class for components
 * exporting documents from GoldenGATE Server to external targets triggered by
 * document update events. This class provides an event queue and a service
 * thread working off events asynchronously. Further, it provides extensible
 * document indexing functionality to facilitate console-triggered updates.<br>
 * It is up to sub classes to provide bindings that connect to document sources,
 * listen for update events and enqueue them.<br>
 * It is also up to sub classes to implement document preparation and
 * connections to the destination of the exports.<br>
 * Further, sub classes have to overwrite either version of the
 * <code>doUpdate()</code> and <code>doDelete()</code> methods in order to
 * perform their actual update actions.
 * 
 * @author sautter
 */
public abstract class GoldenGateEXP extends GoldenGateAEP implements LiteratureConstants {
	
	/** the name of the attribute set in the <code>docAttributes</code> argument
	 * to the <code>doUpdate()</code> method if that method is called for the
	 * first time for the argument document, namely 'isNewDocument' */
	protected static final String IS_NEW_DOCUMENT_ATTRIBUTE = "isNewDocument";
	
	/** the name of the index table */
	protected final String DATA_TABLE_NAME;
	
	/** the name of the column in the index table that is filled with a 'D' if a document is deleted */
	protected static final String DELETED_MARKER_COLUMN_NAME = "Deleted";
	
	/** the value in the 'Deleted' column indicating that a document is deleted */
	protected static final char DELETED_MARKER = 'D';
	
	private TableColumnDefinition[] indexFields;
	
	/** the binding to the underlying document repository */
	protected GoldenGateExpBinding binding;
	
	private GPathExpression[] filters = new GPathExpression[0];
	
	/**
	 * Constructor
	 * @param letterCode the letter code identifying the component
	 */
	protected GoldenGateEXP(String letterCode, String exporterName) {
		super(letterCode, exporterName);
		this.DATA_TABLE_NAME = (exporterName + "Data");
	}
	
	/**
	 * This method exists so sub classes can provide an informative name for
	 * themselves. The name returned by this method must consist of letters only
	 * and must not include whitespace, as among other things it serves as the
	 * name for the database table this component uses for keeping track of the
	 * documents in the backing source. Implementations of this method must not
	 * depend on loading any external resources because this method is called
	 * from the constructor.
	 * @return the name of the exporter
	 */
	protected String getExporterName() {
		return this.getEventProcessorName();
	}
	
	/**
	 * This method exists so sub classes can add custom fields to the export
	 * document index, which is necessary to enable respective console-triggered
	 * updates. For each field returned by this method, an
	 * update&lt;fieldName&gt; console action is created. Further, sub classes
	 * returning custom index fields must implement the getIndexFieldValue()
	 * method such that it extracts a sensible value for each index field from a
	 * given document. Otherwise, console triggered updates will not work
	 * properly. It is highly recommended that field names are short yet telling
	 * to simplify console use. This default implementation returns an empty
	 * array, sub classes are welcome to overwrite it as needed.
	 * @return an array of custom index fields
	 */
	protected TableColumnDefinition[] getIndexFields() {
		return new TableColumnDefinition[0];
	}
	
	/**
	 * Obtain the value of a custom index field for a given document. This
	 * method must at least attempt to return a non-null value for the names of
	 * the fields returned by the getIndexFields() method, even if it is the
	 * empty string as a last resort. If this method does return null for any
	 * index field, the document will be ignored and is not exported. This
	 * default implementation returns null, sub classes are welcome to overwrite
	 * it as needed.
	 * @param fieldName the name of the custom index field
	 * @param doc the document to index
	 * @return the value of the argument index field for the argument document
	 */
	protected String getIndexFieldValue(String fieldName, QueriableAnnotation doc) {
		return null;
	}
	
	/**
	 * This method establishes the database connection as well as the table for
	 * keeping track of documents. Sub classes overwriting this method thus
	 * have to make the super invocation. This method invokes the
	 * getIndexFields() method, so sub classes whose custom index fields depend
	 * on reading any configuration parameters should read the latter before
	 * making the super invocation.
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#initComponent()
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	initialize super class
		super.initComponent();
		
		//	get database connection
		this.io = this.host.getIoProvider();
		if (!this.io.isJdbcAvailable())
			throw new RuntimeException(this.getExporterName() + " cannot work without database access.");
		
		//	get custom index fields
		this.indexFields = this.getIndexFields();
		
		//	ensure data table
		//	TODO consider introducing docIdHash
		TableDefinition td = new TableDefinition(DATA_TABLE_NAME);
		td.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(DELETED_MARKER_COLUMN_NAME, TableDefinition.CHAR_DATATYPE, 1);
		td.addColumn(DOCUMENT_DATE_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		for (int f = 0; f < this.indexFields.length; f++)
			td.addColumn(this.indexFields[f]);
		if (!this.io.ensureTable(td, true))
			throw new RuntimeException(this.getExporterName() + " cannot work without database access.");
		
		//	add indexes
		this.io.indexColumn(DATA_TABLE_NAME, DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(DATA_TABLE_NAME, DOCUMENT_DATE_ATTRIBUTE);
		for (int f = 0; f < this.indexFields.length; f++)
			this.io.indexColumn(DATA_TABLE_NAME, this.indexFields[f].getColumnName());
		
		//	read filters
		this.loadFilters(this);
	}
	
	private void loadFilters(GoldenGateServerActivityLogger log) {
		File filterFile = new File(this.dataPath, "filters.cnfg");
		
		//	clear filters
		if (!filterFile.exists())
			this.filters = new GPathExpression[0];
		
		//	read filters
		else try {
			BufferedReader filterBr = new BufferedReader(new InputStreamReader(new FileInputStream(filterFile), ENCODING));
			ArrayList filterExpressions = new ArrayList();
			for (String filterLine; (filterLine = filterBr.readLine()) != null;) {
				filterLine = filterLine.trim();
				if ((filterLine.length() == 0) || filterLine.startsWith("//"))
					continue;
				try {
					filterExpressions.add(GPathParser.parseExpression(filterLine));
				}
				catch (GPathException gpe) {
					log.logError(this.getExporterName() + ": could not load GPath filter '" + filterLine + "'");
					log.logError(gpe);
				}
			}
			filterBr.close();
			this.filters = ((GPathExpression[]) filterExpressions.toArray(new GPathExpression[filterExpressions.size()]));
		}
		
		//	well, seems we're not supposed to filter
		catch (IOException ioe) {
			log.logError(this.getExporterName() + ": could not load GPath filters");
			log.logError(ioe);
		}
	}
	
	/**
	 * This implementation produces the binding that establishes the connection
	 * to the backing document source. Sub classes overwriting this method to
	 * link up to specific document sources thus have to make the super
	 * invocation, preferably at the end of their own implementation.
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		this.binding = this.getBinding();
	}
	
	/**
	 * This implementation connects the binding to its backing document source.
	 * Afterward, it starts the event handling thread. Sub classes overwriting
	 * this method thus have to make the super invocation.
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#linkInit()
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		
		//	connect to backing document source
		this.binding.connect();
		
		//	link initialize super class
		super.linkInit();
	}
	
	/**
	 * Retrieve the binding that connects this exporter to its backing document
	 * source. This method is invoked at the end of linkInit(), so sub classes
	 * can first establish their uplinks before producing a respective binding.
	 * This method must not return null. If the intended source is unavailable,
	 * this method should rather throw a RuntimeException.
	 * @return the binding that connects this exporter to its backing document
	 *         source
	 */
	protected abstract GoldenGateExpBinding getBinding();
	
	/**
	 * Execute a sub class specific query against the index table. This method
	 * permits only selections, no updates. Sub classes are responsible for
	 * creating proper queries, including escaping special characters, for
	 * handling exceptions, and for closing the result after they are done with
	 * it.
	 * @param query the query to execute
	 * @return the query result
	 * @throws SQLException
	 */
	protected SqlQueryResult queryIndexTable(String query) throws SQLException {
		return this.io.executeSelectQuery(query);
	}
	
	private static final String RELOAD_FILTERS_COMMAND = "reloadFilters";
	private static final String UPDATE_FIELD_COMMAND_PREFIX = "update";
	private static final String UPDATE_YEAR_COMMAND = "updateYear";
	private static final String UPDATE_ALL_COMMAND = "updateAll";
	private static final String UPDATE_DELETE_COMMAND = "updateDel";
	private static final String DIFF_DOCS_COMMAND = "diffDocs";
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(super.getActions()));
		ComponentAction ca;
		
		//	reload export filters
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return RELOAD_FILTERS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						RELOAD_FILTERS_COMMAND,
						"Reload the filters precluding documents from export."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					loadFilters(this);
					this.reportResult("Export filters reloaded, got " + filters.length + " filters now.");
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	get explanations of custom parameters
		LinkedHashMap updateParams = this.getUpdateParams();
		final String updateParamString;
		final String[] updateParamExplanations;
		if (updateParams != null) {
			StringBuffer updateParamList = new StringBuffer();
			updateParamExplanations = new String[updateParams.size()];
			int upei = 0;
			for (Iterator upnit = updateParams.keySet().iterator(); upnit.hasNext();) {
				String upn = ((String) upnit.next());
				updateParamList.append(" <");
				updateParamList.append(upn);
				updateParamList.append(">");
				updateParamExplanations[upei++] = ((String) updateParams.get(upn));
			}
			updateParamString = updateParamList.toString();
		}
		else {
			updateParamString = "";
			updateParamExplanations = null;
		}
		
		//	update data matching a filter on a custom index field
		for (int f = 0; f < this.indexFields.length; f++) {
			final String fieldName = this.indexFields[f].getColumnName();
			final String dataType = this.indexFields[f].getDataType();
			final int fieldLength = this.indexFields[f].getColumnLength();
			ca = new ComponentActionConsole() {
				public String getActionCommand() {
					return (UPDATE_FIELD_COMMAND_PREFIX + StringUtils.capitalize(fieldName));
				}
				public String[] getExplanation() {
					String[] explanation = {
							((UPDATE_FIELD_COMMAND_PREFIX + StringUtils.capitalize(fieldName)) + " <" + fieldName.toLowerCase() + "> <priority>" + updateParamString),
							("Issue update events for documents with a specific " + fieldName.toLowerCase() + ":"),
							("- <" + fieldName.toLowerCase() + ">: the " + fieldName.toLowerCase() + " to issue update events for"),
							"- <priority>: set to '-n' or '-h' to issue normal or high-priority events, respectively (optional)."
						};
					return mergeArrays(explanation, updateParamExplanations);
				}
				public void performActionConsole(String[] arguments) {
					if (arguments.length == 0) {
						this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at least the " + fieldName.toLowerCase() + ".");
						return;
					}
					else if ((arguments.length > 2) && (updateParamString.length() == 0)) {
						this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the " + fieldName.toLowerCase() + " and priority as the only arguments.");
						return;
					}
					char priority = PRIORITY_LOW;
					int paramStart = 2;
					if (arguments.length > 1) {
						if ("-h".equals(arguments[1]))
							priority = PRIORITY_HIGH;
						else if ("-n".equals(arguments[1]))
							priority = PRIORITY_NORMAL;
						else if ("-l".equals(arguments[1]))
							priority = PRIORITY_LOW;
						else paramStart--;
					}
					long params;
					try {
						params = ((arguments.length <= paramStart) ? 0 : encodeUpdateParams(Arrays.copyOfRange(arguments, paramStart, arguments.length)));
					}
					catch (IllegalArgumentException iae) {
						this.reportError(" Invalid arguments for '" + this.getActionCommand() + "': " + iae.getMessage());
						return;
					}
					String where;
					if (TableDefinition.INT_DATATYPE.equals(dataType) || TableDefinition.BIGINT_DATATYPE.equals(dataType))
						where = (fieldName + " = " + Integer.parseInt(arguments[0]));
					else {
						String fieldValue = EasyIO.sqlEscape(arguments[0]);
						if (fieldValue.length() > fieldLength)
							fieldValue = fieldValue.substring(0, fieldLength);
						where = (fieldName + " LIKE '" + fieldValue + "%'");
					}
					int count = triggerUpdatesFromIndex(where, priority, params);
					this.reportResult("Issued update events for " + count + " documents.");
				}
			};
			cal.add(ca);
		}
		
		//	update data from a specific year
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return UPDATE_YEAR_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						(UPDATE_YEAR_COMMAND + " <year> <priority>" + updateParamString),
						"Issue update events for documents from a specific year:",
						"- <year>: the year to issue update events for",
						"- <priority>: set to '-n' or '-h' to issue normal or high-priority events, respectively (optional)."
					};
//				return explanation;
				return mergeArrays(explanation, updateParamExplanations);
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at least the year.");
					return;
				}
				else if ((arguments.length > 2) && (updateParamString.length() == 0)) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the year and priority as the only arguments.");
					return;
				}
				char priority = PRIORITY_LOW;
				int paramStart = 2;
				if (arguments.length > 1) {
					if ("-h".equals(arguments[1]))
						priority = PRIORITY_HIGH;
					else if ("-n".equals(arguments[1]))
						priority = PRIORITY_NORMAL;
					else if ("-l".equals(arguments[1]))
						priority = PRIORITY_LOW;
					else paramStart--;
				}
				long params;
				try {
					params = ((arguments.length <= paramStart) ? 0 : encodeUpdateParams(Arrays.copyOfRange(arguments, paramStart, arguments.length)));
				}
				catch (IllegalArgumentException iae) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "': " + iae.getMessage());
					return;
				}
				int count = triggerUpdatesFromIndex((DOCUMENT_DATE_ATTRIBUTE + " = " + Integer.parseInt(arguments[0])), priority, params);
				this.reportResult("Issued update events for " + count + " documents.");
			}
		};
		cal.add(ca);
		
		//	update all data
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return UPDATE_ALL_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						(UPDATE_ALL_COMMAND + updateParamString),
						("Issue update events for the whole document collection" + ((updateParamString.length() == 1) ? "." : ":"))
					};
				return mergeArrays(explanation, updateParamExplanations);
			}
			public void performActionConsole(String[] arguments) {
				if ((arguments.length != 0) && (updateParamString.length() == 0)) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no argument.");
					return;
				}
				long params;
				try {
					params = encodeUpdateParams(arguments);
				}
				catch (IllegalArgumentException iae) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "': " + iae.getMessage());
					return;
				}
				int count = triggerUpdatesFromIndex("1=1", PRIORITY_LOW, params);
				this.reportResult("Issued update events for " + count + " documents.");
			}
		};
		cal.add(ca);
		
		//	re-trigger deletions
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return UPDATE_DELETE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						UPDATE_DELETE_COMMAND,
						"Trigger forwarding pending document deletions."
					};
//				return explanation;
				return mergeArrays(explanation, updateParamExplanations);
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0)
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no argument.");
				else {
					String deletedDocIdQuery = "SELECT " + DOCUMENT_ID_ATTRIBUTE + 
							" FROM " + DATA_TABLE_NAME + 
							" WHERE " + DELETED_MARKER_COLUMN_NAME + " = '" + DELETED_MARKER + "'" +
							";";
					SqlQueryResult sqr = null;
					try {
						sqr = io.executeSelectQuery(deletedDocIdQuery);
						int count = 0;
						while (sqr.next()) {
							dataDeleted(sqr.getString(0), null, PRIORITY_LOW);
							count++;
						}
						this.reportResult("Issued delete events for " + count + " documents.");
					}
					catch (SQLException sqle) {
						this.reportError(getExporterName() + ": " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting deleted document IDs.");
						this.reportError("  query was " + deletedDocIdQuery);
					}
					finally {
						if (sqr != null)
							sqr.close();
					}
				}
			}
		};
		cal.add(ca);
		
		//	diff documents with source
		ca = new ComponentActionConsole() {
			private Thread diffThread = null;
			public String getActionCommand() {
				return DIFF_DOCS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						DIFF_DOCS_COMMAND,
						"Diff the erporter's document table with the backing source."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no argument.");
					return;
				}
				if (this.diffThread != null) {
					this.reportError(" A document diff operation is already in progress.");
					return;
				}
				this.diffThread = new Thread() {
					public void run() {
						try {
							doDiff();
						}
						finally {
							diffThread = null;
						}
					}
				};
				this.diffThread.start();
			}
			private void doDiff() {
				
				//	get non-deleted document IDs from own database table
				HashSet deleteDocIDs = new HashSet();
				int retained = 0;
				int deleted = 0;
				String docIdQuery = "SELECT " + DOCUMENT_ID_ATTRIBUTE + 
						" FROM " + DATA_TABLE_NAME + 
						" WHERE " + DELETED_MARKER_COLUMN_NAME + " <> '" + DELETED_MARKER + "'" +
						";";
				SqlQueryResult sqr = null;
				try {
					sqr = io.executeSelectQuery(docIdQuery, true);
					this.reportResult("Got document list, starting diff.");
					for (int docs = 1; sqr.next(); docs++) {
						if ((docs % 500) == 0)
							this.reportResult(" - diffed " + docs + " documents so far, deleted " + deleted + ", retained " + retained);
						
						//	try and see if document still exists
						if (binding.isDocumentAvailable(sqr.getString(0))) {
							retained++;
							continue;
						}
						else {
							deleteDocIDs.add(sqr.getString(0));
							deleted++;
						}
						
						//	flag this round of documents as deleted, no need to buffer them all
						if (deleteDocIDs.size() >= 32) {
							this.deleteDocs(deleteDocIDs);
							deleteDocIDs.clear();
						}
					}
				}
				catch (SQLException sqle) {
					this.reportError(getExporterName() + ": " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting deleted document IDs.");
					this.reportError("  query was " + docIdQuery);
				}
				finally {
					if (sqr != null)
						sqr.close();
				}
				
				//	flag remaining documents as deleted
				if (deleteDocIDs.size() != 0) {
					this.deleteDocs(deleteDocIDs);
					deleteDocIDs.clear();
				}
				
				//	issue final report
				this.reportResult("Issued delete events for " + deleted + " documents, retained " + retained + " ones.");
			}
			private void deleteDocs(HashSet deleteDocIDs) {
				
				//	flag obsolete documents as deleted
				StringBuffer docUpdateQuery = new StringBuffer("UPDATE " + DATA_TABLE_NAME + 
						" SET " + DELETED_MARKER_COLUMN_NAME + " = '" + DELETED_MARKER + "'" +
						" WHERE " + DOCUMENT_ID_ATTRIBUTE + " IN (");
				for (Iterator idit = deleteDocIDs.iterator(); idit.hasNext();) {
					String deleteDocId = ((String) idit.next());
					docUpdateQuery.append("'" + deleteDocId + "'");
					if (idit.hasNext())
						docUpdateQuery.append(", ");
				}
				docUpdateQuery.append(");");
				try {
					io.executeUpdateQuery(docUpdateQuery.toString());
				}
				catch (SQLException sqle) {
					this.reportError(getExporterName() + ": " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while flagging documents as deleted.");
					this.reportError("  query was " + docUpdateQuery.toString());
				}
				
				//	trigger update events
				for (Iterator idit = deleteDocIDs.iterator(); idit.hasNext();)
					dataDeleted(((String) idit.next()), null); // set attributes to null initially to prevent holding whole index table in memory for large console-triggered updates
				this.reportResult(" - issued delete events for " + deleteDocIDs.size() + " documents.");
			}
		};
		cal.add(ca);
		
		//	re-investigate whole collection
		ca = this.binding.getReingestAction();
		if (ca != null)
			cal.add(ca);
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private String[] mergeArrays(String[] strs1, String[] strs2) {
		if ((strs1 == null) || (strs1.length == 0))
			return strs2;
		else if ((strs2 == null) || (strs2.length == 0))
			return strs1;
		String[] strs = new String[strs1.length + strs2.length];
		System.arraycopy(strs1, 0, strs, 0, strs1.length);
		System.arraycopy(strs2, 0, strs, strs1.length, strs2.length);
		return strs;
	}
	
	/**
	 * Provide an map of names of subclass specific parameters to be included
	 * in update triggering console actions, the mapped values being the
	 * associated explanations. This default implementation returns null to
	 * indicate 'no further parameters', subclasses are welcome to overwrite it
	 * as needed, but also need to overwrite the
	 * <code>encodeUpdateParams()</code> method to handle the named parameters.
	 * @return an array holding the parameters
	 */
	protected LinkedHashMap getUpdateParams() {
		return null;
	}
	
	/**
	 * Encode a series of custom update parameters in a bit vector. Subclasses
	 * that use this mechanism have to overwrite the four-argument version of
	 * the <code>doUpdate()</code>^method to gain access to the encoded
	 * parameter values on event processing. The runtime length of the argument
	 * array can vary and is not immediately bound to the length of the array
	 * returned by the <code>getUpdateParamExplanations()</code> method.
	 * @param params an array holding the parameter values to encode
	 * @return a long encoding the parameter values in the argument array
	 * @throws IllegalArgumentException if any of the parameters values in the
	 *            argument array are invalid
	 */
	protected long encodeUpdateParams(String[] params) throws IllegalArgumentException {
		return 0;
	}
	
	private int triggerUpdatesFromIndex(String where, char priority, long params) {
		String docQuery = "SELECT " + DOCUMENT_ID_ATTRIBUTE + 
				" FROM " + DATA_TABLE_NAME + 
				" WHERE " + where +
					" AND " + DELETED_MARKER_COLUMN_NAME + " <> '" + DELETED_MARKER + "'" +
				";";
		SqlQueryResult sqr = null;
		int count = 0;
		try {
			sqr = io.executeSelectQuery(docQuery);
			while (sqr.next()) {
				this.dataUpdated(sqr.getString(0), false, null, priority, params);
				count++;
			}
		}
		catch (SQLException sqle) {
			this.logError(getExporterName() + ": " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting document IDs for " + where + ".");
			this.logError("  query was " + docQuery);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		return count;
	}
	
	/**
	 * Notify this class that a document has been updated. This method updates
	 * the index table if necessary and enqueues an update. Actual listening for
	 * update events is up to sub classes.
	 * @param docId the ID of the document
	 * @param doc the document
	 * @param user the user responsible for the update
	 * @return the position at which the update event is enqueued
	 */
	public int documentUpdated(String docId, QueriableAnnotation doc, String user) {
		
		//	apply filters if doc given
		if ((doc != null) && this.filterOut(doc))
			return -1;
		
		//	gather export parameters
		Properties docAttributes = new Properties();
		boolean isNewDoc;
		boolean isHighPriority = (doc != null);
		
		//	assemble fields
		StringBuffer fields = new StringBuffer(DOCUMENT_DATE_ATTRIBUTE);
		for (int f = 0; f < this.indexFields.length; f++)
			fields.append(", " + this.indexFields[f].getColumnName());
		
		//	check if document in data table
		String checkQuery = "SELECT " + fields.toString() +
				" FROM " + DATA_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
				";";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(checkQuery);
			
			//	if table entry exists
			if (sqr.next()) {
				
				//	document not given (console triggered update) ==> nothing to do
				if (doc == null) {
					docAttributes.setProperty(DOCUMENT_DATE_ATTRIBUTE, sqr.getString(0));
					for (int f = 0; f < this.indexFields.length; f++)
						docAttributes.setProperty(this.indexFields[f].getColumnName(), sqr.getString(f+1));
				}
				
				//	document given (event triggered update) ==> update table entry
				else {
					String docDate = ((String) doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE, "-1"));
					try {
						Integer.parseInt(docDate);
					}
					catch (RuntimeException re) {
						docDate = "-1";
					}
					docAttributes.setProperty(DOCUMENT_DATE_ATTRIBUTE, docDate);
					StringBuffer updates = new StringBuffer(" " + DOCUMENT_DATE_ATTRIBUTE + " = " + EasyIO.sqlEscape(docDate));
					for (int f = 0; f < this.indexFields.length; f++) {
						String fieldValue = this.getIndexFieldValue(this.indexFields[f].getColumnName(), doc);
						if (fieldValue == null)
							return -1;
						docAttributes.setProperty(this.indexFields[f].getColumnName(), fieldValue);
						if (TableDefinition.isEscapedType(this.indexFields[f].getDataType())) {
							if (fieldValue.length() > this.indexFields[f].getColumnLength())
								fieldValue = fieldValue.substring(0, this.indexFields[f].getColumnLength());
							updates.append(", " + this.indexFields[f].getColumnName() + " = '" + EasyIO.sqlEscape(fieldValue) + "'");
						}
						else updates.append(", " + this.indexFields[f].getColumnName() + " = " + EasyIO.sqlEscape(fieldValue));
					}
					updates.append(", " + DELETED_MARKER_COLUMN_NAME + " = ' '");
					
					//	update database
					String updateQuery = "UPDATE " + DATA_TABLE_NAME +
							" SET" + updates.toString() + 
							" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
							";";
					try {
						this.io.executeUpdateQuery(updateQuery);
					}
					catch (SQLException sqle) {
						this.logError(this.getExporterName() + ": " + sqle.getMessage() + " while updating table entry.");
						this.logError("  query was " + updateQuery);
					}
				}
				
				//	indicate that document is not new
				isNewDoc = false;
			}
			
			else {
				
				//	load document on demand for console triggered updates
				if (doc == null) try {
					doc = this.binding.getDocument(docId);
					
					//	apply filters
					if (this.filterOut(doc))
						return -1;
				}
				
				//	document unavailable, we're done here
				catch (IOException ioe) {
					this.logError(this.getExporterName() + ": " + ioe.getMessage() + " while loading document '" + docId + "' from binding.");
					this.logError(ioe);
					return -1;
				}
				
				//	collect document attributes ...
				String docDate = ((String) doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE, "-1"));
				try {
					Integer.parseInt(docDate);
				}
				catch (RuntimeException re) {
					docDate = "-1";
				}
				docAttributes.setProperty(DOCUMENT_DATE_ATTRIBUTE, docDate);
				StringBuffer values = new StringBuffer(EasyIO.sqlEscape(docDate));
				for (int f = 0; f < this.indexFields.length; f++) {
					String fieldValue = this.getIndexFieldValue(this.indexFields[f].getColumnName(), doc);
					if (fieldValue == null)
						return -1;
					docAttributes.setProperty(this.indexFields[f].getColumnName(), fieldValue);
					if (TableDefinition.isEscapedType(this.indexFields[f].getDataType())) {
						if (fieldValue.length() > this.indexFields[f].getColumnLength())
							fieldValue = fieldValue.substring(0, this.indexFields[f].getColumnLength());
						values.append(", '" + EasyIO.sqlEscape(fieldValue) + "'");
					}
					else values.append(", " + EasyIO.sqlEscape(fieldValue));
				}
				
				//	... and store them in database table
				String insertQuery = "INSERT INTO " + DATA_TABLE_NAME +
						" (" + DOCUMENT_ID_ATTRIBUTE + ", " + DELETED_MARKER_COLUMN_NAME + ", " + fields.toString() + ")" +
						" VALUES" +
						" ('" + EasyIO.sqlEscape(docId) + "', ' ', " + values.toString() + ")" +
						";";
				
				try {
					this.io.executeUpdateQuery(insertQuery);
				}
				catch (SQLException sqle) {
					this.logError(this.getExporterName() + ": " + sqle.getMessage() + " while creating table entry.");
					this.logError("  query was " + insertQuery);
				}
				
				//	indicate that document is new
				isNewDoc = true;
				docAttributes.setProperty(IS_NEW_DOCUMENT_ATTRIBUTE, "true");
			}
		}
		catch (SQLException sqle) {
			this.logError(this.getExporterName() + ": " + sqle.getMessage() + " while checking table entry.");
			this.logError("  query was " + checkQuery);
			return -1;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		//	enqueue update
		return this.dataUpdated(docId, isNewDoc, user, (isHighPriority ? PRIORITY_NORMAL : PRIORITY_LOW));
	}
	
	/**
	 * Filter documents. This default implementation applies the GPath filter
	 * expressions loaded from the <code>filters.cnfg</code> file in the data
	 * path of the exporter. If any of the filters evaluates to true, the
	 * document is filtered out. Sub classes may overwrite this method to use
	 * additional non-GPath filters. To keep the GPath based filers active,
	 * however, they should make the super call to this implementation.
	 * @param doc the document to filter
	 * @return true if the document should be ignored for export, false
	 *            otherwise
	 */
	protected boolean filterOut(QueriableAnnotation doc) {
		for (int f = 0; f < this.filters.length; f++) try {
			if (GPath.evaluateExpression(this.filters[f], doc, null).asBoolean().value)
				return true;
		}
		catch (GPathException gpe) {
			this.logError(this.getExporterName() + ": could not apply GPath filter '" + this.filters[f].toString() + "'");
			this.logError(gpe);
		}
		return false;
	}

	/**
	 * Notify this class that a document has been deleted. This method enqueues
	 * a respective deletion. Actual listening for deletion events is up to sub
	 * classes.
	 * @param docId the ID of the document
	 * @param user the user responsible for the update
	 * @return the position at which the deletion event is enqueued
	 */
	public int documentDeleted(String docId, String user) {
		
		//	mark document as deleted in index table
		String deleteMarkerQuery = "UPDATE " + DATA_TABLE_NAME +
				" SET " + DELETED_MARKER_COLUMN_NAME + " = '" + DELETED_MARKER + "'" +
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
				";";
		try {
			this.io.executeUpdateQuery(deleteMarkerQuery);
		}
		catch (SQLException sqle) {
			this.logError(this.getExporterName() + ": " + sqle.getMessage() + " while marking document as deleted.");
			this.logError("  query was " + deleteMarkerQuery);
		}
		
		//	enqueue event
		return this.dataDeleted(docId, user);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#loadDataAttributes(java.lang.String)
	 */
	protected Properties loadDataAttributes(String dataId) {
		
		//	assemble fields and query
		StringBuffer fields = new StringBuffer(DOCUMENT_DATE_ATTRIBUTE);
		for (int f = 0; f < this.indexFields.length; f++)
			fields.append(", " + this.indexFields[f].getColumnName());
		String loadQuery = "SELECT " + fields.toString() +
				" FROM " + DATA_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(dataId) + "'" +
				";";
		
		//	do lookup
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(loadQuery);
			if (sqr.next()) {
				Properties dataAttributes = new Properties();
				try {
					Properties docAttributes = this.binding.getDocumentAttributes(dataId);
					if (docAttributes != null)
						dataAttributes.putAll(docAttributes);
				}
				catch (IOException ioe) {
					this.logError(this.getExporterName() + ": " + ioe.getMessage() + " while loading document attributes.");
					this.logError(ioe);
				}
				dataAttributes.setProperty(DOCUMENT_DATE_ATTRIBUTE, sqr.getString(0));
				for (int f = 0; f < this.indexFields.length; f++)
					dataAttributes.setProperty(this.indexFields[f].getColumnName(), sqr.getString(f+1));
				return dataAttributes;
			}
			else return null;
		}
		catch (SQLException sqle) {
			this.logError(this.getExporterName() + ": " + sqle.getMessage() + " while loading document attributes.");
			this.logError("  query was " + loadQuery);
			return null;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#doUpdate(java.lang.String, java.lang.String, java.util.Properties, long)
	 */
	protected void doUpdate(String dataId, String user, Properties dataAttributes, long params) throws IOException {
		this.doUpdate(this.binding.getDocument(dataId), dataAttributes);
	}
	
	/**
	 * Write an update to the underlying export destination. If this method is
	 * called for the first time for the argument document, the argument
	 * <code>docAttributes</code> object contains <code>isNewDocument</code> as
	 * an additional key to indicate so. Sub classes can either overwrite this
	 * dummy implementation, or overwrite the four-argument version of this
	 * method directly.
	 * @param doc the document to export
	 * @param docAttributes the index field values of the document
	 * @throws IOException
	 */
	protected void doUpdate(QueriableAnnotation doc, Properties docAttributes) throws IOException {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#doDelete(java.lang.String, java.lang.String, java.util.Properties, long)
	 */
	protected void doDelete(String dataId, String user, Properties dataAttributes, long params) throws IOException {
		this.doDelete(dataId, dataAttributes);
	}
	
	/**
	 * Write a deletion to the underlying export destination. Sub classes can
	 * either overwrite this dummy implementation, or overwrite the four-
	 * argument version of this method directly.
	 * @param docId the ID of the document that was deleted
	 * @param docAttributes the index field values of the document
	 * @throws IOException
	 */
	protected void doDelete(String docId, Properties docAttributes) throws IOException {};
	
	/**
	 * A GoldenGATE Server EXP Binding hooks up GoldenGATE Server EXP to other
	 * server components that store documents and issue update events.
	 * 
	 * @author sautter
	 */
	public static abstract class GoldenGateExpBinding {
		
		/** the GoldenGATE Server EXP the binding belongs to */
		protected GoldenGateEXP host;
		
		/**
		 * Constructor
		 * @param host the GoldenGATE EXP the binding belongs to
		 */
		protected GoldenGateExpBinding(GoldenGateEXP host) {
			this.host = host;
		}
		
		/**
		 * Connect the binding to its backing source.
		 */
		public abstract void connect();
		
		/**
		 * Check if a document is available from the component backing the binding
		 * @param docId the ID of the document to check
		 * @return true if a document with the specified ID exists
		 * @throws IOException
		 */
		public abstract boolean isDocumentAvailable(String docId);
		
		/**
		 * Retrieve the attributes of an updated document from the component
		 * backing the binding
		 * @param docId the ID of the document to load
		 * @return the attributes of the document with the specified ID
		 * @throws IOException
		 */
		public abstract Properties getDocumentAttributes(String docId) throws IOException;
		
		/**
		 * Retrieve an updated document from the component backing the binding
		 * @param docId the ID of the document to load
		 * @return the document with the specified ID
		 * @throws IOException
		 */
		public abstract QueriableAnnotation getDocument(String docId) throws IOException;
		
		/**
		 * Obtain a console action that can trigger update events for all
		 * documents available from its backing source. This operation is
		 * optional, as - depending on their backing source - not all bindings
		 * may be able to provide respective functionality. Thus, this default
		 * implementation returns null. Sub classes willing to provide
		 * collection update functionality have to overwrite this method with a
		 * meaningful implementation.
		 */
		public ComponentActionConsole getReingestAction() {
			return null;
		}
	}
}
