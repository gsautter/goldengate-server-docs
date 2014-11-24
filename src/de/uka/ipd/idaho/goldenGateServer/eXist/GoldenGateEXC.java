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
package de.uka.ipd.idaho.goldenGateServer.eXist;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.AnnotationInputStream;
import de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP;
import de.uka.ipd.idaho.goldenGateServer.util.Base64;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.XsltUtils;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * The GoldenGATE Server eXist Connector keeps XML files in an external eXist
 * XML database synchronized with XML files hosted in one of GoldenGATE Server's
 * components, using HTTP PUT and HTTP DELETE. This connector supports basic
 * authentication with Base64 encoding. Documents can be transformed through an
 * XSLT stylesheet before upload. It leaves abstract the actual binding to a
 * backing server component that store documents and issue update events.
 * 
 * @author sautter
 */
public abstract class GoldenGateEXC extends GoldenGateEXP {
	
	private TreeMap eXistConnections = new TreeMap();
	
	private class EXistConnection {
		String name;
		String outputUrl = null;//"http://plazi.cs.umb.edu:8080/exist/rest/db/taxonx_docs/";
		String user;
		String pswd;
		String authentication = null;
		TreeMap storageFormats = new TreeMap();
		
		EXistConnection(String name, String outputUrl) {
			this.name = name;
			this.outputUrl = outputUrl;
		}
		
		void handleUpdate(String rawDocumentName, QueriableAnnotation document) throws IOException {
			
			//	build document base name
			String documentBaseName = this.buildDocumentBaseName(rawDocumentName);
			if (DEBUG_EXPORT) System.out.println("      - document base name is '" + documentBaseName + "'");
			
			//	go through different storage formats
			for (Iterator fit = storageFormats.values().iterator(); fit.hasNext();) {
				ExportFormat sf = ((ExportFormat) fit.next());
				if (DEBUG_EXPORT) System.out.println("      - exporting storage format '" + sf.name + "'");
				
				try {
					
					//	build document name
					String documentName = documentBaseName;
					if (sf.fileNameSuffix.length() != 0)
						documentName += ("_" + sf.fileNameSuffix);
					documentName += ".xml";
					if (DEBUG_EXPORT) System.out.println("        - document name is '" + documentName + "'");
					
					//	prepare upload URL
					HttpURLConnection putCon = this.getConnection(documentName, "PUT");
					
					//	do upload
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(putCon.getOutputStream(), "UTF-8"));
					
					//	no XSLT transformer, send plain data
					if (sf.xsltUrls.length == 0)
//						AnnotationUtils.writeXML(document, bw, null, null, true);
						AnnotationUtils.writeXML(document, bw);
					
					//	do transformation
					else {
						
						//	build transformer chain
						InputStream is = new AnnotationInputStream(document, "  ", "utf-8"); 
						for (int x = 0; x < (sf.xsltUrls.length - 1); x++) {
							Transformer xslt = this.getTransformer(sf.xsltUrls[x]);
							if (xslt == null) {
								if (DEBUG_EXPORT) System.out.println("        - could not instantiate transformer from XSLT at '" + sf.xsltUrls[x] + "'");
								throw new IOException("XSLT transformer chain broken at '" + sf.xsltUrls[x] + "'");
							}
							else {
								if (DEBUG_EXPORT) System.out.println("        - chained in transformer from XSLT at '" + sf.xsltUrls[x] + "'");
								is = XsltUtils.chain(is, xslt);
							}
						}
						
						//	build last transformer
						Transformer xslt = this.getTransformer(sf.xsltUrls[sf.xsltUrls.length-1]);
						if (xslt == null) {
							if (DEBUG_EXPORT) System.out.println("        - could not instantiate transformer from XSLT at '" + sf.xsltUrls[sf.xsltUrls.length-1] + "'");
							throw new IOException("XSLT transformer chain broken at '" + sf.xsltUrls[sf.xsltUrls.length-1] + "'");
						}
						
						//	process data
						try {
							xslt.transform(new StreamSource(is), new StreamResult(bw));
						}
						catch (TransformerException te) {
							throw new IOException(te.getMessageAndLocation());
						}						
					}
					
					if (DEBUG_EXPORT) System.out.println("      - document uploaded");
					
					//	print server's response
					bw.flush();
					if (DEBUG_EXPORT) System.out.println(putCon.getResponseCode() + ": " + putCon.getResponseMessage());
					bw.close();
				}
				
				catch (IOException ioe) {
					System.out.println("GoldenGateEXC: Error forwarding update for document '" + rawDocumentName + "' - " + ioe.getMessage());
					ioe.printStackTrace(System.out);
				}
			}
		}
		
		void handleDeletion(String rawDocumentName) {
			
			//	build document base name
			String documentBaseName = this.buildDocumentBaseName(rawDocumentName);
			if (DEBUG_EXPORT) System.out.println("      - document base name is '" + documentBaseName + "'");
			
			//	go through different storage formats
			for (Iterator fit = storageFormats.values().iterator(); fit.hasNext();) {
				ExportFormat sf = ((ExportFormat) fit.next());
				if (DEBUG_EXPORT) System.out.println("      - deleting for storage format '" + sf.name + "'");
				
				try {
					
					//	build document name
					String documentName = documentBaseName;
					if (sf.fileNameSuffix.length() != 0)
						documentName += ("_" + sf.fileNameSuffix);
					documentName += ".xml";
					if (DEBUG_EXPORT) System.out.println("      - document name is '" + documentName + "'");
					
					//	prepare delete URL
					HttpURLConnection deleteCon = this.getConnection(documentName, "DELETE");
					
					//	print server's response
					deleteCon.connect();
					BufferedReader br = new BufferedReader(new InputStreamReader(deleteCon.getInputStream(), "UTF-8"));
					String responseLine;
					while ((responseLine = br.readLine()) != null)
						System.out.println(responseLine);
					br.close();
				}
				
				catch (IOException ioe) {
					System.out.println("GoldenGateEXC: Error forwarding deletion of document '" + documentBaseName + "' to '" + this.name + "' - " + ioe.getMessage());
					ioe.printStackTrace(System.out);
				}
			}
		}
		
		private Transformer getTransformer(String xsltUrl) throws IOException {
			if (xsltUrl.startsWith("http://"))
				return XsltUtils.getTransformer(xsltUrl);
			else return XsltUtils.getTransformer(new File(dataPath, xsltUrl));
		}
		
		private HttpURLConnection getConnection(String documentName, String httpMethod) throws IOException {
			URL url = new URL(this.outputUrl + documentName);
			if (DEBUG_EXPORT) System.out.println("      - URL is '" + url.toString() + "'");
			HttpURLConnection connection = ((HttpURLConnection) url.openConnection());
			connection.setRequestMethod(httpMethod);
			if (authentication != null)
				connection.setRequestProperty("Authorization", ("Basic " + authentication));
			connection.setRequestProperty("Content-Type", "text/xml");
			connection.setDoOutput(true);
			return connection;
		}
		
		private String buildDocumentBaseName(String rawDocumentName) {
			if (rawDocumentName.endsWith(".xml"))
				rawDocumentName = rawDocumentName.substring(0, (rawDocumentName.length() - ".xml".length()));
			for (Iterator fit = storageFormats.values().iterator(); fit.hasNext();) {
				ExportFormat sf = ((ExportFormat) fit.next());
				if (rawDocumentName.endsWith("_" + sf.fileNameSuffix))
					rawDocumentName = rawDocumentName.substring(0, (rawDocumentName.length() - ("_" + sf.fileNameSuffix).length()));
			}
			return rawDocumentName;
		}
	}
	
	private class ExportFormat {
		String name;
		String fileNameSuffix;
		String[] xsltUrls;
		
		ExportFormat(String name, String nameSuffix, String[] xsltUrls) {
			this.name = name;
			this.fileNameSuffix = nameSuffix;
			this.xsltUrls = xsltUrls;
		}
	}
	
	private static final String DOCUMENT_NAME_COLUMN_NAME = "Name";
	private static final int DOCUMENT_NAME_COLUMN_LENGTH = 256;
	
	/**
	 * Constructor
	 * @param letterCode the letter code identifying the connector
	 */
	protected GoldenGateEXC(String letterCode) {
		super(letterCode);
	}
	
	/**
	 * This implementation adds a column for the document name on the connected
	 * eXist servers. Sub classes overwriting this method thus have to make the
	 * super invocation and add this column to their own list of index fields.
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#getIndexFields()
	 */
	protected TableColumnDefinition[] getIndexFields() {
		TableColumnDefinition[] tcds = {
			new TableColumnDefinition(DOCUMENT_NAME_COLUMN_NAME, TableDefinition.VARCHAR_DATATYPE, DOCUMENT_NAME_COLUMN_LENGTH)
		};
		return tcds;
	}
	
	/**
	 * This implementation reads the value for the document name on the
	 * connected eXist servers. Sub classes overwriting this method thus have to
	 * make the super invocation.
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#getIndexFieldValue(java.lang.String, de.uka.ipd.idaho.gamta.QueriableAnnotation)
	 */
	protected String getIndexFieldValue(String fieldName, QueriableAnnotation doc) {
		if (DOCUMENT_NAME_COLUMN_NAME.equals(fieldName))
			return this.getDocumentName(doc);
		else return super.getIndexFieldValue(fieldName, doc);
	}
	
	/**
	 * Generate the name for a document on the connected eXist servers. If this
	 * method returns null, the argument document is not exported.
	 * @param doc the document to generate the name for
	 * @return the name for the document
	 */
	protected abstract String getDocumentName(QueriableAnnotation doc);
	
	/**
	 * This implementation loads the configures connections to eXist servers.
	 * Sub classes overwriting this method thus have to make the super
	 * invocation.
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#initComponent()
	 */
	protected void initComponent() {
		
		//	get connection files
		File[] connectionFiles = this.dataPath.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return (file.isFile() && file.getName().endsWith(".eXist.cnfg"));
			}
		});
		
		//	load connections
		for (int c = 0; c < connectionFiles.length; c++) {
			Settings connectionSettings = Settings.loadSettings(connectionFiles[c]);
			
			//	load target server URL
			String outputUrl = connectionSettings.getSetting("outputUrl");
			if (outputUrl != null) {
				if (!outputUrl.endsWith("/")) outputUrl += "/";
				
				String name = connectionFiles[c].getName();
				name = name.substring(0, (name.length() - ".eXist.cnfg".length()));
				
				EXistConnection eXistConnection = new EXistConnection(name, outputUrl);
				
				//	load storage formats
				Settings storageFormatSettings = connectionSettings.getSubset("storageFormat");
				String[] storageFormatNames = storageFormatSettings.getSubsetPrefixes();
				for (int f = 0; f < storageFormatNames.length; f++) {
					Settings storageFormat = storageFormatSettings.getSubset(storageFormatNames[f]);
					String fileNameSuffix = storageFormat.getSetting("nameSuffix");
					if (fileNameSuffix != null) {
						StringVector xsltUrls = new StringVector();
						String xsltUrl;
						int xsltUrlNumber = 0;
						while ((xsltUrl = storageFormat.getSetting("xsltUrl" + xsltUrlNumber++)) != null)
							xsltUrls.addElement(xsltUrl);
						eXistConnection.storageFormats.put(storageFormatNames[f], new ExportFormat(storageFormatNames[f], fileNameSuffix, xsltUrls.toStringArray()));
					}
				}
				
				//	load authentication data
				eXistConnection.user = connectionSettings.getSetting("authUser");
				eXistConnection.pswd = connectionSettings.getSetting("authPswd");
				if ((eXistConnection.user != null) && (eXistConnection.pswd != null)) {
					String authentication = (eXistConnection.user + ":" + eXistConnection.pswd);
					int[] authenticationBytes = new int[authentication.length()];
					for (int a = 0; a < authentication.length(); a++)
						authenticationBytes[a] = authentication.charAt(a);
					eXistConnection.authentication = Base64.encode(authenticationBytes);
				}
				
				//	store connection
				this.eXistConnections.put(eXistConnection.name, eXistConnection);
			}
		}
		
		//	initialize the rest
		super.initComponent();
	}
	
	private EXistConnection getEXistConnection(String name) {
		return ((EXistConnection) this.eXistConnections.get(name));
	}
	
	private void storeEXistConnection(EXistConnection eXistConnection) throws IOException {
		Settings connectionSettings = new Settings();
		
		//	save basic data
		connectionSettings.setSetting("outputUrl", eXistConnection.outputUrl);
		if ((eXistConnection.user != null) && (eXistConnection.pswd != null)) {
			connectionSettings.setSetting("authUser", eXistConnection.user);
			connectionSettings.setSetting("authPswd", eXistConnection.pswd);
		}
		
		//	save storage formats
		Settings storageFormatSettings = connectionSettings.getSubset("storageFormat");
		storageFormatSettings.clear();
		for (Iterator fit = eXistConnection.storageFormats.values().iterator(); fit.hasNext();) {
			ExportFormat sf = ((ExportFormat) fit.next());
			Settings sfSet = storageFormatSettings.getSubset(sf.name);
			sfSet.setSetting("nameSuffix", sf.fileNameSuffix);
			for (int x = 0; x < sf.xsltUrls.length; x++)
				sfSet.setSetting(("xsltUrl" + x), sf.xsltUrls[x]);
		}
		
		//	save to file
		connectionSettings.storeAsText(new File(this.dataPath, (eXistConnection.name + ".eXist.cnfg")));
	}
	
	private static final String LIST_CONNECTIONS_COMMAND = "listCons";
	private static final String ADD_CONNECTION_COMMAND = "addCon";
	private static final String CHANGE_CONNECTION_COMMAND = "changeCon";
	private static final String DROP_CONNECTION_COMMAND = "dropCon";
	
	private static final String LIST_FORMATS_COMMAND = "listFormats";
	private static final String ADD_FORMAT_COMMAND = "addFormat";
	private static final String REFRESH_FORMAT_COMMAND = "refreshFormat";
	private static final String CHANGE_FORMAT_COMMAND = "changeFormat";
	private static final String DROP_FORMAT_COMMAND = "dropFormat";
	
	/* (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(super.getActions()));
		ComponentAction ca;
		
		//	add a storage format to export
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return ADD_FORMAT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						ADD_FORMAT_COMMAND + " <connection> <name> <fileNameSuffix> <xsltUrl>",
						"Add a storage format to be exported to the backing eXist database:",
						"- <connection>: The name of the connection to add the format to",
						"- <name>: The name for the storage format",
						"- <fileNameExtension>: The infix to add to a file name before the '.xml' ending",
						"- <xsltUrls>: The URLs (space separated, relative or absolute) of the XSLT stylesheets to run documents through for this export"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length >= 3) {
					EXistConnection ec = getEXistConnection(arguments[0]);
					if (ec == null) {
						System.out.println(" Invalid connection name '" + arguments[0] + "'");
						return;
					}
					
					String name = arguments[1];
					if (ec.storageFormats.containsKey(name)) {
						System.out.println(" A format named '" + arguments[0] + "' already exists, use " + CHANGE_FORMAT_COMMAND);
						return;
					}
					
					String fileNameExtension = arguments[2];
					String[] xsltUrls = new String[arguments.length - 3];
					System.arraycopy(arguments, 3, xsltUrls, 0, xsltUrls.length);
					ec.storageFormats.put(name, new ExportFormat(name, fileNameExtension, xsltUrls));
					
					try {
						storeEXistConnection(ec);
						System.out.println(" Format '" + name + "' added successfully");
					}
					catch (IOException e) {
						System.out.println(" Error storing connection: " + e.getMessage());
						e.printStackTrace(System.out);
					}
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify only the listed arguments.");
			}
		};
		cal.add(ca);
		
		//	change a storage format to export
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return REFRESH_FORMAT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						REFRESH_FORMAT_COMMAND + " <connection> <name>",
						"Refresh a storage format (i.e., reload the XSLT stylesheets it uses) being exported to the backing eXist database:",
						"- <connection>: The name of the connection the format to refresh belongs to",
						"- <name>: The name for the storage format to refresh",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 2) {
					EXistConnection ec = getEXistConnection(arguments[0]);
					if (ec == null) {
						System.out.println(" Invalid connection name '" + arguments[0] + "'");
						return;
					}
					
					String name = arguments[1];
					ExportFormat sf = ((ExportFormat) ec.storageFormats.get(name));
					if (sf == null) {
						System.out.println(" A format named '" + arguments[0] + "' does not exist, use " + ADD_FORMAT_COMMAND);
						return;
					}
					
					for (int x = 0; x < sf.xsltUrls.length; x++) try {
						if (sf.xsltUrls[x].startsWith("http://"))
							XsltUtils.getTransformer(sf.xsltUrls[x], false);
						else XsltUtils.getTransformer(new File(dataPath, sf.xsltUrls[x]), false);
					}
					catch (IOException e) {
						System.out.println(" Error loading XSLT staylesheet from " + sf.xsltUrls[x] + ": " + e.getMessage());
						e.printStackTrace(System.out);
					}
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify only the listed arguments.");
			}
		};
		cal.add(ca);
		
		//	change a storage format to export
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return CHANGE_FORMAT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						CHANGE_FORMAT_COMMAND + " <connection> <name> <fileNameSuffix> <xsltUrl>",
						"Change a storage format being exported to the backing eXist database:",
						"- <connection>: The name of the connection the format to change belongs to",
						"- <name>: The name for the storage format to change",
						"- <fileNameExtension>: The new infix to add to a file name before the '.xml' ending",
						"- <xsltUrls>: The URLs (space separated, relative or absolute) of the XSLT stylesheets to run documents through for this export"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length >= 3) {
					EXistConnection ec = getEXistConnection(arguments[0]);
					if (ec == null) {
						System.out.println(" Invalid connection name '" + arguments[0] + "'");
						return;
					}
					
					String name = arguments[1];
					if (!ec.storageFormats.containsKey(name)) {
						System.out.println(" A format named '" + arguments[0] + "' does not exist, use " + ADD_FORMAT_COMMAND);
						return;
					}
					
					String fileNameExtension = arguments[2];
					String[] xsltUrls = new String[arguments.length - 3];
					System.arraycopy(arguments, 3, xsltUrls, 0, xsltUrls.length);
					ec.storageFormats.put(name, new ExportFormat(name, fileNameExtension, xsltUrls));
					
					try {
						storeEXistConnection(ec);
						System.out.println(" Format '" + name + "' changed successfully");
					}
					catch (IOException e) {
						System.out.println(" Error storing connection: " + e.getMessage());
						e.printStackTrace(System.out);
					}
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify only the listed arguments.");
			}
		};
		cal.add(ca);
		
		//	drop a storage format
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return DROP_FORMAT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						DROP_FORMAT_COMMAND + " <connection> <name>",
						"Drop a storage format so it is no longer exported to the backing eXist database:",
						"- <connection>: The name of the connection to drop the format from",
						"- <name>: The name of the storage format"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 2) {
					EXistConnection ec = getEXistConnection(arguments[0]);
					if (ec == null)
						System.out.println(" Invalid connection name '" + arguments[0] + "'");
					
					else if (ec.storageFormats.remove(arguments[1]) != null) {
						try {
							storeEXistConnection(ec);
							System.out.println(" Format '" + arguments[1] + "' dropped successfully");
						}
						catch (IOException e) {
							System.out.println(" Error storing connection: " + e.getMessage());
							e.printStackTrace(System.out);
						}
					}
						
					else System.out.println(" Invalid format name '" + arguments[1] + "'");
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify only  the name of the connection and the name of the format to drop.");
			}
		};
		cal.add(ca);
		
		//	list all storage formats currently installed
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_FORMATS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_FORMATS_COMMAND,
						"List all storage formats currently installed"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					EXistConnection ec = getEXistConnection(arguments[0]);
					if (ec == null) {
						System.out.println(" Invalid connection name '" + arguments[0] + "'");
						return;
					}
					for (Iterator fit = ec.storageFormats.values().iterator(); fit.hasNext();) {
						ExportFormat sf = ((ExportFormat) fit.next());
						System.out.println(sf.name + ", " + sf.fileNameSuffix);
						for (int x = 0; x < sf.xsltUrls.length; x++)
							System.out.println("- " + sf.xsltUrls[x]);
					}
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify only the name of the connection.");
			}
		};
		cal.add(ca);
		
		
		
		//	add a connection to export to
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return ADD_CONNECTION_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						ADD_CONNECTION_COMMAND + " <name> <url> <user> <password>",
						"Add a connection to an eXist database to export documents to:",
						"- <name>: The name for the connection",
						"- <url>: The URL to connect to",
						"- <user>: The user name to use for authentication (optional parameter)",
						"- <password>: The password to use for authentication (optional parameter)",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((arguments.length == 2) || (arguments.length == 4)) {
					String name = arguments[0];
					if (eXistConnections.containsKey(name)) {
						System.out.println(" A connection named '" + arguments[0] + "' already exists, use " + CHANGE_CONNECTION_COMMAND);
						return;
					}
					
					String url = arguments[1];
					EXistConnection ec = new EXistConnection(name, url);
					
					if (arguments.length == 4) {
						ec.user = arguments[2];
						ec.pswd = arguments[3];
						String authentication = (ec.user + ":" + ec.pswd);
						int[] authenticationBytes = new int[authentication.length()];
						for (int a = 0; a < authentication.length(); a++)
							authenticationBytes[a] = authentication.charAt(a);
						ec.authentication = Base64.encode(authenticationBytes);
					}
					
					try {
						storeEXistConnection(ec);
						eXistConnections.put(ec.name, ec);
						System.out.println(" Connection '" + name + "' added successfully");
					}
					catch (IOException e) {
						System.out.println(" Error storing connection: " + e.getMessage());
						e.printStackTrace(System.out);
					}
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify only the listed arguments.");
			}
		};
		cal.add(ca);
		
		//	change a connection
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return CHANGE_CONNECTION_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						CHANGE_CONNECTION_COMMAND + " <name> (<url> | (<user> <password>))",
						"Change a connection to an eXist database (two arguments change the URL, three arguments change the authentication, four change the whole connection):",
						"- <name>: The name of the connection to change",
						"- <url>: The URL to connect to",
						"- <user>: The user name to use for authentication (specify only with password)",
						"- <password>: The password to use for authentication (specify only with user name)",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((arguments.length >= 2) && (arguments.length <= 4)) {
					EXistConnection ec = getEXistConnection(arguments[0]);
					if (ec == null) {
						System.out.println(" Invalid connection name '" + arguments[0] + "'");
						return;
					}
					
					String changed;
					if (arguments.length == 2) {
						ec.outputUrl = arguments[1];
						changed = "URL of connection";
					}
					
					else if (arguments.length == 3) {
						ec.user = arguments[1];
						ec.pswd = arguments[2];
						String authentication = (ec.user + ":" + ec.pswd);
						int[] authenticationBytes = new int[authentication.length()];
						for (int a = 0; a < authentication.length(); a++)
							authenticationBytes[a] = authentication.charAt(a);
						ec.authentication = Base64.encode(authenticationBytes);
						changed = "Authentication of connection";
					}
					
					else {
						ec.outputUrl = arguments[1];
						ec.user = arguments[2];
						ec.pswd = arguments[3];
						String authentication = (ec.user + ":" + ec.pswd);
						int[] authenticationBytes = new int[authentication.length()];
						for (int a = 0; a < authentication.length(); a++)
							authenticationBytes[a] = authentication.charAt(a);
						ec.authentication = Base64.encode(authenticationBytes);
						changed = "Connection";
					}
					
					try {
						storeEXistConnection(ec);
						System.out.println(" " + changed + " '" + arguments[0] + "' changed successfully");
					}
					catch (IOException e) {
						System.out.println(" Error storing connection: " + e.getMessage());
						e.printStackTrace(System.out);
					}
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify only the listed arguments.");
			}
		};
		cal.add(ca);
		
		//	drop a connection
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return DROP_CONNECTION_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						DROP_CONNECTION_COMMAND + " <name>",
						"Drop a storage format so it is no longer exported to the backing eXist database:",
						"- <name>: The name of the connection to drop",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					EXistConnection ec = getEXistConnection(arguments[0]);
					if (ec == null)
						System.out.println(" Invalid connection name '" + arguments[0] + "'");
					
					else {
						eXistConnections.remove(arguments[0]);
						System.out.println(" Connection '" + arguments[0] + "' dropped successfully");
					}
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify only  the name of the connection to drop.");
			}
		};
		cal.add(ca);
		
		//	list all connections currently installed
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_CONNECTIONS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_CONNECTIONS_COMMAND,
						"List all connections currently installed"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					for (Iterator cit = eXistConnections.values().iterator(); cit.hasNext();) {
						EXistConnection ec = ((EXistConnection) cit.next());
						System.out.println(ec.name + ", " + ec.outputUrl + ((ec.authentication == null) ? "" : (", " + ec.user + ":" + ec.pswd)));
						for (Iterator fit = ec.storageFormats.values().iterator(); fit.hasNext();) {
							ExportFormat sf = ((ExportFormat) fit.next());
							System.out.println("- " + sf.name + ", " + sf.fileNameSuffix);
							for (int x = 0; x < sf.xsltUrls.length; x++)
								System.out.println("  - " + sf.xsltUrls[x]);
						}
					}
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		
		//	return the actions
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doUpdate(de.uka.ipd.idaho.gamta.QueriableAnnotation, java.util.Properties)
	 */
	protected void doUpdate(QueriableAnnotation doc, Properties docAttributes) throws IOException {
		if (DEBUG_EXPORT) System.out.println("  - document was updated");
		
		//	no export destination, save effort for document preparation
		if (this.eXistConnections.isEmpty()) {
			if (DEBUG_EXPORT)
				System.out.println("  - no eXist server yet to export to");
			return;
		}
		
		//	document updated, and well formed
		if (AnnotationUtils.isWellFormedNesting(doc)) {
			if (DEBUG_EXPORT) System.out.println("  - document well-formed, exporting");
			
			//	forward to connected eXist servers
			for (Iterator cit = this.eXistConnections.values().iterator(); cit.hasNext();) {
				EXistConnection ec = ((EXistConnection) cit.next());
				if (DEBUG_EXPORT) System.out.println("      - forwarding update to '" + ec.name + "'");
				ec.handleUpdate(docAttributes.getProperty(DOCUMENT_NAME_COLUMN_NAME), doc);
			}
			
			if (DEBUG_EXPORT) System.out.println("  - update forwarded");
		}
		
		else if (DEBUG_EXPORT) System.out.println("  - document not yet well-formed");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doDelete(java.lang.String, java.util.Properties)
	 */
	protected void doDelete(String docId, Properties docAttributes) throws IOException {
		if (DEBUG_EXPORT) System.out.println("  - document was deleted");
		
		//	forward to connected eXist servers
		for (Iterator cit = this.eXistConnections.values().iterator(); cit.hasNext();) {
			EXistConnection ec = ((EXistConnection) cit.next());
			if (DEBUG_EXPORT) System.out.println("      - forwarding deletion to '" + ec.name + "'");
			ec.handleDeletion(docAttributes.getProperty(DOCUMENT_NAME_COLUMN_NAME));
		}
		
		if (DEBUG_EXPORT) System.out.println("  - deletion forwarded");
	}
	
	private static final boolean DEBUG_EXPORT = true;
}
