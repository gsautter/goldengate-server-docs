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
package de.uka.ipd.idaho.goldenGateServer.exp;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Properties;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
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
 * connections to the destination of the exports.
 * 
 * @author sautter
 */
public abstract class GoldenGateEXP extends AbstractGoldenGateServerComponent implements LiteratureConstants {
	
	private static int instanceCount = 0;
	private static synchronized void countInstance(String exporterName) {
		instanceCount++;
		System.out.println(exporterName + ": registered as instance number " + instanceCount + ".");
	}
	
	private IoProvider io;
	
	/** the name of the index table */
	protected final String DATA_TABLE_NAME;
	
	private static final String DELETED_MARKER_COLUMN_NAME = "Deleted";
	private TableColumnDefinition[] indexFields;
	
	private GoldenGateExpBinding binding;
	
	/**
	 * Constructor
	 * @param letterCode the letter code identifying the component
	 */
	protected GoldenGateEXP(String letterCode) {
		super(letterCode);
		this.DATA_TABLE_NAME = (this.getExporterName() + "Data");
		countInstance(this.getExporterName());
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
	protected abstract String getExporterName();
	
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
	 * making the super invokation.
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	get database connection
		this.io = this.host.getIoProvider();
		if (!this.io.isJdbcAvailable())
			throw new RuntimeException(this.getExporterName() + " cannot work without database access.");
		
		//	get custom index fields
		this.indexFields = this.getIndexFields();
		
		//	ensure data table
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
	}
	
	/**
	 * This implementation produces the binding that establishes the connection
	 * to the backing document source. Sub classes overwriting this method to
	 * link up to specific document sources thus have to make the super
	 * invokation, preferably at the end of their own implementation.
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		this.binding = this.getBinding();
	}
	
	/**
	 * This implementation connects the binding to its backing document source.
	 * Afterward, it starts the event handling thread. Sub classes overwriting
	 * this method thus have to make the super invokation.
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		this.binding.connect();
		
		//	start event processing service
		this.eventHandler = new UpdateEventHandler();
		this.eventHandler.start();
		System.out.println(this.getExporterName() + ": event handler started");
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
	 * This method shuts down asynchronous event handling. Sub classes
	 * overwriting this method thus have to make the super invocation.
	 * @see de.goldenGateScf.AbstractServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		
		//	shut down event handle
		this.eventHandler.shutdown();
		System.out.println(this.getExporterName() + ": event handler shut down");
		
		//	disconnect from database
		this.io.close();
	}
	
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
	
	private static final String UPDATE_FIELD_COMMAND_PREFIX = "update";
	
	private static final String UPDATE_YEAR_COMMAND = "updateYear";
	
	private static final String UPDATE_ALL_COMMAND = "updateAll";
	
	private static final String UPDATE_DELETE_COMMAND = "updateDel";
	
	private static final String QUEUE_SIZE_COMMAND = "queueSize";
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList();
		ComponentAction ca;
		
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
							(UPDATE_FIELD_COMMAND_PREFIX + StringUtils.capitalize(fieldName)) + " <" + fieldName.toLowerCase() + ">",
							"Issue update events for documents with a specific " + fieldName.toLowerCase() + ":",
							"- <" + fieldName.toLowerCase() + ">: the " + fieldName.toLowerCase() + " to issue update events for"
						};
					return explanation;
				}
				public void performActionConsole(String[] arguments) {
					if (arguments.length == 0)
						System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify the " + fieldName.toLowerCase() + " as the only argument.");
					else {
						String where;
						if (TableDefinition.INT_DATATYPE.equals(dataType) || TableDefinition.BIGINT_DATATYPE.equals(dataType))
							where = (fieldName + " = " + Integer.parseInt(arguments[0]));
						else {
							String fieldValue = EasyIO.sqlEscape(arguments[0]);
							for (int a = 1; a < arguments.length; a++)
								fieldValue += ("%" + EasyIO.sqlEscape(arguments[a]));
							if (fieldValue.length() > fieldLength)
								fieldValue = fieldValue.substring(0, fieldLength);
							where = (fieldName + " LIKE '" + fieldValue + "%'");
						}
						int count = triggerUpdatesFromIndex(where);
						System.out.println("Issued update events for " + count + " documents.");
					}
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
						UPDATE_YEAR_COMMAND + " <year>",
						"Issue update events for documents from a specific year:",
						"- <year>: the year to issue update events for"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 1)
					System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify the year as the only argument.");
				else {
					int count = triggerUpdatesFromIndex(DOCUMENT_DATE_ATTRIBUTE + " = " + Integer.parseInt(arguments[0]));
					System.out.println("Issued update events for " + count + " documents.");
				}
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
						UPDATE_ALL_COMMAND,
						"Issue update events for the whole document collection."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0)
					System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no argument.");
				else {
					int count = triggerUpdatesFromIndex("1=1");
					System.out.println("Issued update events for " + count + " documents.");
				}
			}
		};
		cal.add(ca);
		
		//	retrigger deletions
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return UPDATE_DELETE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						UPDATE_DELETE_COMMAND,
						"Trigger forwarding pending document deletions."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0)
					System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no argument.");
				else {
					String docQuery = "SELECT " + DOCUMENT_ID_ATTRIBUTE + 
							" FROM " + DATA_TABLE_NAME + 
							" WHERE " + DELETED_MARKER_COLUMN_NAME + " = 'D'" +
							";";
					SqlQueryResult sqr = null;
					try {
						sqr = io.executeSelectQuery(docQuery);
						int count = 0;
						while (sqr.next()) {
							documentDeleted(sqr.getString(0), ((Properties) null)); // set attributes to null initially to prevent holding whole index table in memory for large console-triggered updates
							count++;
						}
						System.out.println("Issued delete events for " + count + " documents.");
					}
					catch (SQLException sqle) {
						System.out.println(getExporterName() + ": " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting deleted document IDs.");
						System.out.println("  query was " + docQuery);
					}
					finally {
						if (sqr != null)
							sqr.close();
					}
				}
			}
		};
		cal.add(ca);
		
		//	re-investigate whole collection
		ca = this.binding.getReingestAction();
		if (ca != null)
			cal.add(ca);
		
		//	indicate current size of update queue
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return QUEUE_SIZE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						QUEUE_SIZE_COMMAND,
						"Show current size of event queue, i.e., number of pending updates."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0)
					System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
				else System.out.println(eventQueue.size() + " update events pending.");
			}
		};
		cal.add(ca);
		
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private int triggerUpdatesFromIndex(String where) {
		String docQuery = "SELECT " + DOCUMENT_ID_ATTRIBUTE + 
				" FROM " + DATA_TABLE_NAME + 
				" WHERE " + where +
					" AND " + DELETED_MARKER_COLUMN_NAME + " <> 'D'" +
				";";
		SqlQueryResult sqr = null;
		int count = 0;
		try {
			sqr = io.executeSelectQuery(docQuery);
			while (sqr.next()) {
				documentUpdated(sqr.getString(0), ((Properties) null)); // set attributes to null initially to prevent holding whole index table in memory for large console-triggered updates
				count++;
			}
		}
		catch (SQLException sqle) {
			System.out.println(getExporterName() + ": " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting document IDs for " + where + ".");
			System.out.println("  query was " + docQuery);
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
	 */
	public void documentUpdated(String docId, QueriableAnnotation doc) {
		Properties docAttributes = new Properties();
		
		//	assemble fields
		StringBuffer fields = new StringBuffer(DOCUMENT_DATE_ATTRIBUTE);
		for (int f = 0; f < this.indexFields.length; f++)
			fields.append(", " + this.indexFields[f].getColumnName());
		
		//	check if document in data table
		String checkQuery = "SELECT " + fields.toString() +
				" FROM " + DATA_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
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
					String docDate = ((String) doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE));
					if (docDate == null)
						return;
					docAttributes.setProperty(DOCUMENT_DATE_ATTRIBUTE, docDate);
					StringBuffer updates = new StringBuffer(" " + DOCUMENT_DATE_ATTRIBUTE + " = " + EasyIO.sqlEscape(docDate));
					for (int f = 0; f < this.indexFields.length; f++) {
						String fieldValue = this.getIndexFieldValue(this.indexFields[f].getColumnName(), doc);
						if (fieldValue == null)
							return;
						docAttributes.setProperty(this.indexFields[f].getColumnName(), fieldValue);
						if (TableDefinition.isEscapedType(this.indexFields[f].getDataType())) {
							if (fieldValue.length() > this.indexFields[f].getColumnLength())
								fieldValue = fieldValue.substring(0, this.indexFields[f].getColumnLength());
							updates.append(", " + this.indexFields[f].getColumnName() + " = '" + EasyIO.sqlEscape(fieldValue) + "'");
						}
						else updates.append(", " + this.indexFields[f].getColumnName() + " = " + EasyIO.sqlEscape(fieldValue));
					}
					
					//	update database
					String updateQuery = "UPDATE " + DATA_TABLE_NAME +
							" SET" + updates.toString() + 
							" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
							";";
					try {
						this.io.executeUpdateQuery(updateQuery);
					}
					catch (SQLException sqle) {
						System.out.println(this.getExporterName() + ": " + sqle.getMessage() + " while updating table entry.");
						System.out.println("  query was " + updateQuery);
					}
				}
			}
			
			else {
				
				//	load document on demand for console triggered updates
				if (doc == null) try {
					doc = this.binding.getDocument(docId);
				}
				catch (IOException ioe) {
					System.out.println(this.getExporterName() + ": " + ioe.getMessage() + " while loading document '" + docId + "' from SRS.");
					ioe.printStackTrace(System.out);
					return;
				}
				
				String docDate = ((String) doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE));
				if (docDate == null)
					return;
				docAttributes.setProperty(DOCUMENT_DATE_ATTRIBUTE, docDate);
				StringBuffer values = new StringBuffer(EasyIO.sqlEscape(docDate));
				for (int f = 0; f < this.indexFields.length; f++) {
					String fieldValue = this.getIndexFieldValue(this.indexFields[f].getColumnName(), doc);
					if (fieldValue == null)
						return;
					docAttributes.setProperty(this.indexFields[f].getColumnName(), fieldValue);
					if (TableDefinition.isEscapedType(this.indexFields[f].getDataType())) {
						if (fieldValue.length() > this.indexFields[f].getColumnLength())
							fieldValue = fieldValue.substring(0, this.indexFields[f].getColumnLength());
						values.append(", '" + EasyIO.sqlEscape(fieldValue) + "'");
					}
					else values.append(", " + EasyIO.sqlEscape(fieldValue));
				}
				
				String insertQuery = "INSERT INTO " + DATA_TABLE_NAME +
						" (" + DOCUMENT_ID_ATTRIBUTE + ", " + DELETED_MARKER_COLUMN_NAME + ", " + fields.toString() + ")" +
						" VALUES" +
						" ('" + EasyIO.sqlEscape(docId) + "', ' ', " + values.toString() + ")" +
						";";
				
				try {
					this.io.executeUpdateQuery(insertQuery);
				}
				catch (SQLException sqle) {
					System.out.println(this.getExporterName() + ": " + sqle.getMessage() + " while creating table entry.");
					System.out.println("  query was " + insertQuery);
				}
			}
		}
		catch (SQLException sqle) {
			System.out.println(this.getExporterName() + ": " + sqle.getMessage() + " while checking table entry.");
			System.out.println("  query was " + checkQuery);
			return;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		//	enqueue update
		this.documentUpdated(docId, docAttributes);
	}
	
	private void documentUpdated(String docId, Properties docAttributes) {
		this.enqueueEvent(new UpdateEvent(docId, docAttributes, false));
	}
	
	/**
	 * Notify this class that a document has been deleted. This method enqueues
	 * a respective deletion. Actual listening for deletion events is up to sub
	 * classes.
	 * @param docId the ID of the document
	 */
	public void documentDeleted(String docId) {
		Properties docAttributes = this.loadDocAttributes(docId);
		
		//	we don't know about this document
		if (docAttributes == null)
			return;
		
		//	mark document as deleted in index table
		String deleteMarkerQuery = "UPDATE " + DATA_TABLE_NAME +
				" SET " + DELETED_MARKER_COLUMN_NAME + " = 'D'" +
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				";";
		try {
			this.io.executeUpdateQuery(deleteMarkerQuery);
		}
		catch (SQLException sqle) {
			System.out.println(this.getExporterName() + ": " + sqle.getMessage() + " while marking document as deleted.");
			System.out.println("  query was " + deleteMarkerQuery);
		}
		
		//	enqueue event
		this.documentDeleted(docId, docAttributes);
	}
	
	private void documentDeleted(String docId, Properties docAttributes) {
		this.enqueueEvent(new UpdateEvent(docId, docAttributes, true));
	}
	
	private Properties loadDocAttributes(String docId) {
		
		//	assemble fields and query
		StringBuffer fields = new StringBuffer(DOCUMENT_DATE_ATTRIBUTE);
		for (int f = 0; f < this.indexFields.length; f++)
			fields.append(", " + this.indexFields[f].getColumnName());
		String checkQuery = "SELECT " + fields.toString() +
				" FROM " + DATA_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
				";";
		
		//	do lookup
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(checkQuery);
			if (sqr.next()) {
				Properties docAttributes = new Properties();
				docAttributes.setProperty(DOCUMENT_DATE_ATTRIBUTE, sqr.getString(0));
				for (int f = 0; f < this.indexFields.length; f++)
					docAttributes.setProperty(this.indexFields[f].getColumnName(), sqr.getString(f+1));
				return docAttributes;
			}
			else return null;
		}
		catch (SQLException sqle) {
			System.out.println(this.getExporterName() + ": " + sqle.getMessage() + " while loading document attributes.");
			System.out.println("  query was " + checkQuery);
			return null;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	private class UpdateEvent {
		final String docId;
		final DocAttributes docAttributes;
		final boolean isDelete;
		UpdateEvent(String documentId, Properties docAttributes, boolean isDelete) {
			this.docId = documentId;
			this.docAttributes = new DocAttributes(docAttributes);
			this.isDelete = isDelete;
		}
	}
	
	private class DocAttributes extends Properties {
		DocAttributes(Properties defaults) {
			super(defaults);
		}
		void setDefaults(Properties defaults) {
			this.defaults = defaults;
		}
		Properties getDefaults() {
			return this.defaults;
		}
	}
	
	private void enqueueEvent(UpdateEvent event) {
		synchronized (this.eventQueue) {
			this.eventQueue.addLast(event); // enqueue event for asynchronous handling
			this.eventQueue.notify(); // wake up event handler
		}
	}
	
	private LinkedList eventQueue = new LinkedList();
	
	private UpdateEventHandler eventHandler;
	
	private static final boolean DEBUG_EXPORT = true;
//	private static final boolean DEBUG_LINK_FILTER = (DEBUG_EXPORT && false);
	
	private class UpdateEventHandler extends Thread {
		private boolean shutdown = false;
		public void run() {
			
			//	wake up starting thread
			synchronized (this) {
				this.notify();
			}
			
			//	wait a little before starting to work
			try {
				Thread.sleep(10000);
			} catch (InterruptedException ie) {}
			
			//	run indefinitely
			while (!this.shutdown) {
				
				//	get next event, or wait until next event available
				UpdateEvent ue = null;
				synchronized (eventQueue) {
					if (eventQueue.size() == 0) try {
						eventQueue.wait();
					} catch (InterruptedException ie) {}
					if (eventQueue.size() != 0)
						ue = ((UpdateEvent) eventQueue.removeFirst());
				}
				
				//	keep track of resource use
				long eventProcessingTime;
				
				//	nothing to do
				if (ue == null)
					eventProcessingTime = 0;
				
				//	got event to process
				else {
					long eventProcessingStart = System.currentTimeMillis();
					if (DEBUG_EXPORT) System.out.println(getExporterName() + ": got " + (ue.isDelete ? "delete" : "update") + " event for document '" + ue.docId + "'");
					try {
						
						//	load document attributes if not done before
						if (ue.docAttributes.getDefaults() == null)
							ue.docAttributes.setDefaults(loadDocAttributes(ue.docId));
						
						//	delete
						if (ue.isDelete) {
							doDelete(ue.docId, ue.docAttributes);
							
							//	delete document from index table
							String deleteQuery = "DELETE FROM " + DATA_TABLE_NAME +
									" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(ue.docId) + "'" +
									";";
							try {
								io.executeUpdateQuery(deleteQuery);
							}
							catch (SQLException sqle) {
								System.out.println(getExporterName() + ": " + sqle.getMessage() + " while deleting document.");
								System.out.println("  query was " + deleteQuery);
							}
						}
						
						//	update
						else {
							
							//	get document
							QueriableAnnotation doc = binding.getDocument(ue.docId);
							if (doc != null) {
								if (DEBUG_EXPORT) System.out.println("  - document loaded");
								
								//	do update
								doUpdate(doc, ue.docAttributes);
							}
							else if (DEBUG_EXPORT)
								System.out.println("  - document does not exist or is not suitable for export");
						}
					}
					catch (Exception e) {
						System.out.println(getExporterName() + ": Error forwarding update for document '" + ue.docId + "' - " + e.getMessage());
						e.printStackTrace(System.out);
					}
					catch (Throwable t) {
						System.out.println(getExporterName() + ": Error forwarding update for document '" + ue.docId + "' - " + t.getMessage());
						t.printStackTrace(System.out);
					}
					finally {
						eventProcessingTime = (System.currentTimeMillis() - eventProcessingStart);
						if (DEBUG_EXPORT) System.out.println("  - event processed in " + eventProcessingTime + "ms");
					}
				}
				
				//	give the others a little time (and the export target as well), dependent on number of exporters and activity (time spent on actual exports)
				try {
					long sleepTime = (0 + 
							250 + // base sleep
							(50 * instanceCount) + // a little extra for every instance
							eventProcessingTime + // the time we just occupied the CPU or other resources
							0);
					if (DEBUG_EXPORT) System.out.println(getExporterName() + ": sleeping for " + sleepTime + "ms");
					Thread.sleep(sleepTime);
				} catch (InterruptedException ie) {}
			}
		}
		
		public synchronized void start() {
			super.start();
			try {
				this.wait();
			} catch (InterruptedException ie) {}
		}
		
		private void shutdown() {
			synchronized (eventQueue) {
				this.shutdown = true;
				eventQueue.clear();
				eventQueue.notify();
			}
		}
	}
	
	/**
	 * Write an update to the undelying export destination.
	 * @param doc the document to export
	 * @param docAttributes the index field values of the document
	 * @throws IOException
	 */
	protected abstract void doUpdate(QueriableAnnotation doc, Properties docAttributes) throws IOException;
	
	/**
	 * Write a deletion to the undelying export destination.
	 * @param docId the ID of the document that was deleted
	 * @param docAttributes the index field values of the document
	 * @throws IOException
	 */
	protected abstract void doDelete(String docId, Properties docAttributes) throws IOException;
	
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
