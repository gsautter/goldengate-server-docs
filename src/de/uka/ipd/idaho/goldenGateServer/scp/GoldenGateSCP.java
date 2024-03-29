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
package de.uka.ipd.idaho.goldenGateServer.scp;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.xml.transform.ErrorListener;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.AnnotationUtils.XmlOutputOptions;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.AnnotationInputStream;
import de.uka.ipd.idaho.gamta.util.GenericQueriableAnnotationWrapper;
import de.uka.ipd.idaho.gamta.util.gPath.GPath;
import de.uka.ipd.idaho.gamta.util.gPath.GPathExpression;
import de.uka.ipd.idaho.gamta.util.gPath.GPathParser;
import de.uka.ipd.idaho.gamta.util.gPath.exceptions.GPathException;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.AsynchronousWorkQueue;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerActivityLogger;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSRS;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants.SrsDocumentEvent.SrsDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentList;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentListElement;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.XsltUtils;

/**
 * GoldenGATE SRS Collection Packer manages dumps of the document collection
 * hosted by a GoldenGATE SRS, as well as a slim XML index file listing the
 * documents in the backing SRS collection in reverse update time order, i.e.,
 * most recently updated documents first. This helps simplify incremental pulls
 * of the document collection.
 * 
 * @author sautter
 */
public class GoldenGateSCP extends AbstractGoldenGateServerComponent implements GoldenGateSrsConstants {
	
	private GoldenGateSRS srs;
	
	private GPathExpression[] filters = new GPathExpression[0];
	
	private long exportsDue = Long.MAX_VALUE;
	private long exportsStart = -1;
	
	private CollectionExporter collectionExporter;
	private AsynchronousWorkQueue collectionExporterMonitor;
	
	private File indexFile;
	private Map uuidPatterns = new LinkedHashMap();
	
	private Dump[] dumps = new Dump[0];
	
	private File dumpIndexFile;
	private DumpIndexEntry[] dumpIndexEntries = new DumpIndexEntry[0];
	
	/** Constructor handing 'SCP' as the letter code to the super constructor
	 */
	public GoldenGateSCP() {
		super("SCP");
	}
	
	private static class UrlPattern {
		final String name;
		final String urlPrefix;
		final String label;
		final String description;
		UrlPattern(String name, String urlPrefix, String label, String description) {
			this.name = name;
			this.label = label;
			this.urlPrefix = urlPrefix;
			this.description = description;
		}
	}
	
	private static class Dump {
		final String name;
		final File dumpFile;
		final XmlOutputOptions options;
		final Transformer xslt;
		final String fileExtension;
		long exportDue;
		Dump(String name, File dumpFile, XmlOutputOptions options, Transformer xslt, String fileExtension) {
			this.name = name;
			this.dumpFile = dumpFile;
			this.options = options;
			this.xslt = xslt;
			this.fileExtension = fileExtension;
			if (this.dumpFile.exists())
				this.exportDue = (this.dumpFile.lastModified() + millisPerWeek);
			else this.exportDue = System.currentTimeMillis();
		}
	}
	
	private static class DumpIndexEntry {
		final String fileName;
		final String label;
		DumpIndexEntry(String fileName, String label) {
			this.fileName = fileName;
			this.label = label;
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	read filters
		File filterFile = new File(this.dataPath, "filters.cnfg");
		if (filterFile.exists()) try {
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
					System.out.println("SRS Feed Generator: could not load GPath filter '" + filterLine + "'");
					gpe.printStackTrace(System.out);
				}
			}
			filterBr.close();
			this.filters = ((GPathExpression[]) filterExpressions.toArray(new GPathExpression[filterExpressions.size()]));
		}
		
		//	well, seems we're not supposed to filter
		catch (IOException ioe) {
			System.out.println("SRS Collection Packer: could not load GPath filters");
			ioe.printStackTrace(System.out);
		}
		
		//	load export configuration
		this.readExportConfig(this);
	}
	
	private void readExportConfig(GoldenGateServerActivityLogger log) {
		
		//	clean up
		this.indexFile = null;
		this.uuidPatterns.clear();
		
		//	read valid UUID prefixes and respective explanations
		Settings indexSet = Settings.loadSettings(new File(this.dataPath, "index.cnfg"));
		
		//	get index file destination
		String iFile = indexSet.getSetting("file");
		if (iFile != null) {
			this.indexFile = new File(iFile);
			
			//	read data flavors
			String[] dfNames = indexSet.getSubsetPrefixes();
			for (int f = 0; f < dfNames.length; f++) {
				Settings dfSet = indexSet.getSubset(dfNames[f]);
				String dfUrlPrefix = dfSet.getSetting("urlPrefix");
				if (dfUrlPrefix == null)
					continue;
				String dfDescription = dfSet.getSetting("description");
				if (dfDescription == null)
					continue;
				this.uuidPatterns.put(dfNames[f], new UrlPattern(dfNames[f], dfUrlPrefix, dfSet.getSetting("label", dfNames[f]), dfDescription));
			}
		}
		
		//	clean up
		this.cachedStylesheets.clear();
		this.dumps = new Dump[0];
		
		//	get dump files
		File[] dumpFiles = this.dataPath.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return (file.isFile() && file.getName().endsWith(".dump.cnfg"));
			}
		});
		
		//	read dumps
		ArrayList dumps = new ArrayList();
		for (int d = 0; d < dumpFiles.length; d++) try {
			Settings dumpSet = Settings.loadSettings(dumpFiles[d]);
			String dName = dumpFiles[d].getName();
			dName = dName.substring(0, dName.indexOf(".dump.cnfg"));
			String dFile = dumpSet.getSetting("file");
			if (dFile == null)
				continue;
			XmlOutputOptions dOptions = new XmlOutputOptions();
			NameSetFilter dAnnotationTypes = this.parseNameSetFilter(dumpSet.getSetting("annotationTypes"));
			if (dAnnotationTypes != null)
				dOptions.setAnnotationTypes(dAnnotationTypes.filterSet, dAnnotationTypes.invert);
			NameSetFilter dAttributeNames = this.parseNameSetFilter(dumpSet.getSetting("attributeNames"));
			if (dAttributeNames != null)
				dOptions.setAttributeNames(dAttributeNames.filterSet, dAttributeNames.invert);
			NameSetFilter dIncludeIdTypes = this.parseNameSetFilter(dumpSet.getSetting("includeIdTypes"));
			if (dIncludeIdTypes != null)
				dOptions.setIncludeIdTypes(dIncludeIdTypes.filterSet, dIncludeIdTypes.invert);
			String dXsltName = dumpSet.getSetting("xslt");
			String dFileExtension = dumpSet.getSetting("fileExtension", "xml");
			while (dFileExtension.startsWith("."))
				dFileExtension = dFileExtension.substring(".".length());
			dumps.add(new Dump(dName, new File(dFile), dOptions, ((dXsltName == null) ? null : this.getTransformer(dXsltName)), dFileExtension));
		}
		catch (IOException ioe) {
			log.logError("SRS Collection Packer: could not load dump definition from " + dumpFiles[d].getName());
			log.logError(ioe);
		}
		
		//	replace dumps
		this.dumps = ((Dump[]) dumps.toArray(new Dump[dumps.size()]));
		
		//	clean up
		this.dumpIndexFile = null;
		this.dumpIndexEntries = new DumpIndexEntry[0];
		
		//	read dump index configuration
		Settings dumpIndexSet = Settings.loadSettings(new File(this.dataPath, "index.dumps.cnfg"));
		
		//	get dump index file destination
		String diFile = dumpIndexSet.getSetting("file");
		if (diFile != null) {
			this.dumpIndexFile = new File(diFile);
			
			//	read dump index entries
			String[] dieNames = dumpIndexSet.getSubsetPrefixes();
			ArrayList dies = new ArrayList(dieNames.length);
			for (int e = 0; e < dieNames.length; e++) {
				Settings dieSet = dumpIndexSet.getSubset(dieNames[e]);
				String dieFileName = dieSet.getSetting("file");
				if (dieFileName == null)
					continue;
				String dieLabel = dieSet.getSetting("label", "");
				dies.add(new DumpIndexEntry(dieFileName, dieLabel));
			}
			
			//	replace dump index entries
			this.dumpIndexEntries = ((DumpIndexEntry[]) dies.toArray(new DumpIndexEntry[dies.size()]));
		}
	}
	
	private static class NameSetFilter {
		final HashSet filterSet;
		final boolean invert;
		NameSetFilter(HashSet filterSet, boolean invert) {
			this.filterSet = filterSet;
			this.invert = invert;
		}
	}
	private NameSetFilter parseNameSetFilter(String nameStr) {
		if (nameStr == null)
			return null;
		nameStr = nameStr.trim();
		if (nameStr.length() == 0)
			return null;
		ArrayList names = new ArrayList(Arrays.asList(nameStr.split("\\s+")));
		boolean invert = false;
		if ("-".equals(names.get(0))) {
			invert = true;
			names.remove(0);
		}
		return (names.isEmpty() ? null : new NameSetFilter(new HashSet(names), invert));
	}
	
	private HashSet cachedStylesheets = new HashSet();
	private Transformer getTransformer(String xsltName) throws IOException {
		Transformer xslt;
		if (xsltName.indexOf("://") != -1)
			xslt = XsltUtils.getTransformer(xsltName, !this.cachedStylesheets.add(xsltName));
		else if (xsltName.startsWith("/") || (xsltName.indexOf(":") != -1))
			xslt = XsltUtils.getTransformer(new File(xsltName), !this.cachedStylesheets.add(xsltName));
		else xslt = XsltUtils.getTransformer(new File(this.dataPath, xsltName), !this.cachedStylesheets.add(xsltName));
		if (xslt == null)
			throw new IOException("XSLT transformer chain broken at '" + xsltName + "'");
		
		final ErrorListener xsltErrorListener = xslt.getErrorListener();
		xslt.setErrorListener(new ErrorListener() {
			public void warning(TransformerException exception) throws TransformerException {
				logWarning("TransformerWarning: " + exception.getMessage());
				if (xsltErrorListener != null)
					xsltErrorListener.warning(exception);
			}
			public void error(TransformerException exception) throws TransformerException {
				logError("TransformerError: " + exception.getMessage());
				if (xsltErrorListener != null)
					xsltErrorListener.error(exception);
			}
			public void fatalError(TransformerException exception) throws TransformerException {
				logError("TransformerFatalError: " + exception.getMessage());
				if (xsltErrorListener != null)
					xsltErrorListener.fatalError(exception);
			}
		});
		
		return xslt;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		
		//	get SRS
		this.srs = ((GoldenGateSRS) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateSRS.class.getName()));
		
		//	check success
		if (this.srs == null) throw new RuntimeException(GoldenGateSRS.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		
		//	listen for SRS updates
		this.srs.addDocumentEventListener(new SrsDocumentEventListener() {
			public void documentUpdated(SrsDocumentEvent dse) {
				if ((dse.document == null) || filterOut(dse.document))
					return;
				scheduleExports(false);
			}
			public void documentDeleted(SrsDocumentEvent dse) {
				scheduleExports(false);
			}
		});
		
		//	start packing scheduler
		this.collectionExporter = new CollectionExporter();
		this.collectionExporter.start();
		this.collectionExporterMonitor = new AsynchronousWorkQueue(this.collectionExporter.getName()) {
			public String getStatus() {
				return (this.name + ": collections export " + ((exportsStart < 0) ? ("due in " + (exportsDue - System.currentTimeMillis())) : ("running since " + (System.currentTimeMillis() - exportsStart))) + " ms");
			} 
		};
	}
	
	private boolean filterOut(QueriableAnnotation doc) {
		for (int f = 0; f < this.filters.length; f++) try {
			if (GPath.evaluateExpression(this.filters[f], doc, null).asBoolean().value)
				return true;
		}
		catch (GPathException gpe) {
			this.logError("SRS Feed Generator: could not apply GPath filter '" + this.filters[f].toString() + "'");
			this.logError(gpe);
		}
		return false;
	}
	
	/* 5 minutes is too short, as generation takes about 30 minutes, so let's
	 * wait just as long. Otherwise, we might well end up re-generating the
	 * dumps time and again. */
	private static final int eventTriggeredExportDelay = (1000 * 60 * 30);
	private void scheduleExports(boolean immediately) {
		this.scheduleExports(immediately ? 0 : eventTriggeredExportDelay);
	}
	private void scheduleExports(int delay) {
		this.exportsDue = Math.min(this.exportsDue, (System.currentTimeMillis() + delay));
		this.collectionExporter.wakeup();
	}
	private static final long millisPerWeek = (1000 * 60 * 60 * 24 * 7);
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		
		//	shut down packing scheduler
		this.collectionExporterMonitor.dispose();
		this.collectionExporterMonitor = null;
		this.collectionExporter.shutdown();
		this.collectionExporter = null;
	}
	
	private static final String LIST_DUMPS_COMMAND = "list";
	private static final String REPACK_INDEX_COMMAND = "reindex";
	private static final String REPACK_DUMP_COMMAND = "repack";
	private static final String REPACK_DUMP_INDEX_COMMAND = "reindexDumps";
	private static final String PACK_STATUS_COMMAND = "status";
	private static final String SCHEDULE_DUMP_COMMAND = "schedule";
	private static final String RELOAD_CONFIG_COMMAND = "reload";
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList();
		ComponentAction ca;
		
		//	provide action for listing dumps
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_DUMPS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_DUMPS_COMMAND,
						"List the collection dumps currently installed in this SCP."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					this.reportResult("There are currently " + dumps.length + " dumps installed:");
					for (int d = 0; d < dumps.length; d++)
						this.reportResult(" - " + dumps[d].name + ": " + dumps[d].dumpFile.getName());
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	provide action for triggering re-write of XML index
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return REPACK_INDEX_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						REPACK_INDEX_COMMAND,
						"Trigger repacking the XML index of the document collection."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					scheduleExports(true);
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	provide action for triggering re-pack
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return REPACK_DUMP_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						REPACK_DUMP_COMMAND + " <name>",
						"Trigger repacking one or all dumps of the document collection:",
						"- <name>: the name of the dump to repack (optional, omitting repacks all)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length <= 1) {
					long time = System.currentTimeMillis();
					boolean dumpFound = (arguments.length == 0);
					for (int d = 0; d < dumps.length; d++)
						if ((arguments.length == 0) || dumps[d].name.startsWith(arguments[0])) {
							dumps[d].exportDue = time;
							dumpFound = true;
						}
					if ((arguments.length == 0) || dumpFound)
						scheduleExports(true);
					else this.reportError(" Invalid dump name '" + arguments[0] + "', use '" + LIST_DUMPS_COMMAND + "' to list installed dumps.");
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify only the name of the dump to repack.");
			}
		};
		cal.add(ca);
		
		//	provide action for triggering re-write of XML index
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return REPACK_DUMP_INDEX_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						REPACK_DUMP_INDEX_COMMAND,
						"Trigger repacking the index page of the dumps of the document collection."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0)
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
				else try {
					writeDumpIndexPage(this);
				}
				catch (IOException ioe) {
					this.reportError("Error writing index page of collection dumps: " + ioe.getMessage());
					this.reportError(ioe);
				}
			}
		};
		cal.add(ca);
		
		//	provide action for scheduling re-pack
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return SCHEDULE_DUMP_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						SCHEDULE_DUMP_COMMAND + " <time> <name>",
						"Schedule repacking one or all dumps of the document collection:",
						"- <time>: the time to repack the dump(s) in (in seconds, or prefixed with 'd' for days, 'h' for hours, 'm' for minutes)",
						"- <name>: the name of the dump to repack (optional, omitting schedules all)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((arguments.length == 1) || (arguments.length == 2)) {
					long time = System.currentTimeMillis();
					int delay = 1;
					int delayUnitFactor = 1;
					if (arguments[0].startsWith("d") || arguments[0].startsWith("D")) {
						delayUnitFactor = (1000 * 60 * 60 * 24);
						arguments[0] = arguments[0].substring("d".length()).trim();
					}
					else if (arguments[0].startsWith("h") || arguments[0].startsWith("H")) {
						delayUnitFactor = (1000 * 60 * 60);
						arguments[0] = arguments[0].substring("h".length()).trim();
					}
					else if (arguments[0].startsWith("m") || arguments[0].startsWith("M")) {
						delayUnitFactor = (1000 * 60);
						arguments[0] = arguments[0].substring("m".length()).trim();
					}
					try {
						delay = (Integer.parseInt(arguments[0]) * delayUnitFactor);
					} catch (NumberFormatException nfe) {}
					boolean dumpFound = (arguments.length == 0);
					for (int d = 0; d < dumps.length; d++)
						if ((arguments.length == 1) || dumps[d].name.startsWith(arguments[1])) {
							dumps[d].exportDue = (time + delay);
							dumpFound = true;
						}
					if ((arguments.length == 1) || dumpFound) {
						scheduleExports(delay);
						this.reportResult(" Collection export scheduled in " + delay + " seconds.");
					}
					else this.reportError(" Invalid dump name '" + arguments[1] + "', use '" + LIST_DUMPS_COMMAND + "' to list installed dumps.");
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify only the scheduling time and the name of the dump to schedule.");
			}
		};
		cal.add(ca);
		
		//	provide action for re-loading dump and XML index definitions
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PACK_STATUS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PACK_STATUS_COMMAND,
						"Show the status of a current packing process."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					if (exportStatus == null)
						this.reportResult(" Currently, there is no packing process running.");
					else this.reportResult(exportStatus);
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	provide action for re-loading dump and XML index definitions
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return RELOAD_CONFIG_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						RELOAD_CONFIG_COMMAND,
						"Reload the configuration, i.e., the installed dumps and the XML index parameters."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					readExportConfig(this);
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private void doExport(CollectionExporter exporter, long time) throws Exception {
		this.setExportStatus("SRS Collection Packer: starting export", true);
		this.exportsStart = System.currentTimeMillis();
		long startTime;
		
		//	create index writer
		IndexWriter indexWriter = new IndexWriter(this.indexFile, this.uuidPatterns);
		this.setExportStatus(" - index writer created", true);
		
		//	get dumps due for export
		ArrayList dumps = new ArrayList();
		ArrayList dumpWriters = new ArrayList();
		startTime = System.currentTimeMillis();
		for (int d = 0; d < this.dumps.length; d++)
			if (this.dumps[d].exportDue <= time) {
				dumps.add(this.dumps[d]);
				dumpWriters.add(new DumpWriter(this.dumps[d].name, this.dumps[d].dumpFile, this.dumps[d].options, this.dumps[d].xslt, this.dumps[d].fileExtension));
			}
		this.setExportStatus((" - " + dumpWriters.size() + " dump writers created in " + (System.currentTimeMillis() - startTime) + "ms"), true);
		
		//	build collection writer
		CollectionWriter collectionWriter = new CollectionWriter(indexWriter, this.srs, ((DumpWriter[]) dumpWriters.toArray(new DumpWriter[dumpWriters.size()])));
		this.setExportStatus(" - collection writer created", true);
		
		//	get document list from SRS
		startTime = System.currentTimeMillis();
		DocumentList srsDocList = this.srs.getDocumentListFull("-" + UPDATE_TIME_ATTRIBUTE);
		
		//	cache document list locally, so we don't block database table for all too long
		LinkedList docList = new LinkedList();
		while (srsDocList.hasNextElement())
			docList.addLast(srsDocList.getNextElement());
		this.setExportStatus((" - document list retrieved in " + (System.currentTimeMillis() - startTime) + "ms, got " + docList.size() + " documents to export"), true);
		
		//	filter document list
		DocumentListElementWrapper dleWrapper = new DocumentListElementWrapper();
		int fDocCount = 0;
		startTime = System.currentTimeMillis();
		for (Iterator dleit = docList.iterator(); dleit.hasNext();) {
			dleWrapper.setDocumentListElement((DocumentListElement) dleit.next());
			if (this.filterOut(dleWrapper)) {
				dleit.remove();
				fDocCount++;
			}
		}
		this.setExportStatus((" - document list filtered in " + (System.currentTimeMillis() - startTime) + "ms, retained " + docList.size() + " documents to export, filtered out " + fDocCount), true);
		
		//	write documents
		int pDocCount = 0;
		int wDocCount = 0;
		int eDocCount = 0;
		HashSet errorMessages = new HashSet();
		while (docList.size() != 0) {
			this.setExportStatus(("   - processed " + pDocCount + " documents in " + ((System.currentTimeMillis() - time) / 1000) + " sec, exported " + wDocCount + ", got errors on " + eDocCount),  ((pDocCount != 0) && ((pDocCount % 1000) == 0)));
			pDocCount++;
			
			//	are we being shut down?
			if (!exporter.run) {
				this.logWarning("Collection export interrupted by shutdown");
				return;
			}
			
			//	write next document
			DocumentListElement dle = ((DocumentListElement) docList.removeFirst());
			String docId = ((String) dle.getAttribute(DOCUMENT_ID_ATTRIBUTE));
			try {
				long updateTime = Long.parseLong((String) dle.getAttribute(UPDATE_TIME_ATTRIBUTE));
				collectionWriter.write(docId, updateTime, dle);
				wDocCount++;
			}
			catch (RuntimeException re) {
				eDocCount++;
			}
			catch (Exception e) {
				eDocCount++;
				this.logError("Error exporting document '" + docId + "':" + e.getMessage());
				if (errorMessages.add(e.getMessage()))
					this.logError(e);
			}
		}
		
		//	close collection writer
		collectionWriter.close();
		this.setExportStatus(" - collection writer closed", true);
		this.setExportStatus((" - processed " + pDocCount + " documents in " + ((System.currentTimeMillis() - time) / 1000) + " sec, filtered " + fDocCount + ", exported " + wDocCount + ", got errors on " + eDocCount), true);
		
		//	reset dump timestamps
		for (int d = 0; d < dumps.size(); d++)
			((Dump) dumps.get(d)).exportDue = (time + millisPerWeek);
		if (dumps.size() != 0)
			this.setExportStatus(" - dump exports re-scheduled", true);
		
		//	write HTML index page
		this.writeDumpIndexPage(null);
		
		//	set status to idle
		this.setExportStatus(null, false);
	}
	
	private String exportStatus = null;
	private void setExportStatus(String status, boolean print) {
		this.exportStatus = status;
		if (print)
			this.logInfo(status);
	}
	
	private static final SimpleDateFormat lastModifiedFormat = new SimpleDateFormat("EE, dd MMM yyyy HH:mm:ss 'GMT'Z", Locale.US);
	private void writeDumpIndexPage(ComponentActionConsole cac) throws IOException {
		if (this.dumpIndexFile == null)
			return;
		if (this.dumpIndexEntries.length == 0)
			return;
		
		//	create output facilities
		File dumpFolder = this.dumpIndexFile.getParentFile();
		if (!dumpFolder.exists())
			return;
		File diGenFile = new File(dumpFolder, (this.dumpIndexFile.getName() + ".generating"));
		if (diGenFile.exists()) {
			diGenFile.delete();
			diGenFile = new File(dumpFolder, (this.dumpIndexFile.getName() + ".generating"));
		}
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(diGenFile), "UTF-8"));
		
		//	write dump index page header
		out.write("<html>");
		out.write("<head>");
		out.newLine();
		out.write("<title>Document Collection Dumps</title>");
		out.newLine();
		out.write("</head>");
		out.write("<body>");
		out.newLine();
		
		//	write table head
		out.write("<table width=\"100%\" cellspacing=\"0\" cellpadding=\"5\" align=\"center\">");
		out.newLine();
		out.write("<tr>");
		out.newLine();
		out.write("<td align=\"left\"><font size=\"+1\"><strong>Filename</strong></font></td>");
		out.newLine();
		out.write("<td align=\"center\"><font size=\"+1\"><strong>Description</strong></font></td>");
		out.newLine();
		out.write("<td align=\"center\"><font size=\"+1\"><strong>Size</strong></font></td>");
		out.newLine();
		out.write("<td align=\"right\"><font size=\"+1\"><strong>Last Modified</strong></font></td>");
		out.newLine();
		out.write("</tr>");
		out.newLine();
		if (cac != null)
			cac.reportResult(" - index table header written");
		
		//	write index table entries
		int diesWritten = 0;
		for (int e = 0; e < this.dumpIndexEntries.length; e++) {
			File dieFile = new File(dumpFolder, this.dumpIndexEntries[e].fileName);
			if (!dieFile.exists()) {
				if (cac != null)
					cac.reportResult(" - dump file " + this.dumpIndexEntries[e].fileName + " not found");
				continue;
			}
			out.write("<tr" + (((diesWritten % 2) == 0) ? "" : " bgcolor=\"#eeeeee\"") + ">");
			out.newLine();
			out.write("<td align=\"left\"><a href=\"./" + this.dumpIndexEntries[e].fileName + "\"><tt>" + this.dumpIndexEntries[e].fileName + "</tt></a></td>");
			out.newLine();
			out.write("<td align=\"center\"><tt>" + this.dumpIndexEntries[e].label + "</tt></td>");
			out.newLine();
			out.write("<td align=\"center\"><tt>" + (dieFile.length() / 1024) + " kb</tt></td>");
			out.newLine();
			out.write("<td align=\"right\"><tt>" + lastModifiedFormat.format(new Date(dieFile.lastModified())) + "</tt></td>");
			out.newLine();
			out.write("</tr>");
			out.newLine();
			if (cac != null)
				cac.reportResult(" - index row for " + this.dumpIndexEntries[e].fileName + " written");
			diesWritten++;
		}
		
		//	close table
		out.write("</table>");
		out.newLine();
		
		//	close page
		out.write("</body>");
		out.write("</html>");
		out.newLine();
		out.flush();
		out.close();
		if (cac != null)
			cac.reportResult(" - index page finished");
		
		//	switch file live
		File outFile = new File(this.dumpIndexFile.getAbsolutePath());
		if (outFile.exists()) {
			outFile.delete();
			outFile = new File(this.dumpIndexFile.getAbsolutePath());
		}
		diGenFile.renameTo(outFile);
		if (cac != null)
			cac.reportResult(" - index page activated");
		
		//	report success
		if (cac == null)
			this.setExportStatus(" - dump index page created", true);
		else cac.reportResult("Dump index page created");
	}
	
	private static class DocumentListElementWrapper extends GenericQueriableAnnotationWrapper {
		DocumentListElementWrapper() {
			super(Gamta.newDocument(Gamta.newTokenSequence("DUMMY", null)));
		}
		void setDocumentListElement(DocumentListElement dle) {
			this.clearAttributes();
			this.copyAttributes(dle);
		}
	}
	
	private static class UtcDateFormat extends SimpleDateFormat {
		UtcDateFormat(String pattern) {
			super(pattern, Locale.US);
			this.setTimeZone(TimeZone.getTimeZone("UTC")); 
		}
	}
	private static final DateFormat updateTimestampDateFormat = new UtcDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private class CollectionWriter {
		private IndexWriter indexWriter;
		private DumpWriter[] dumpWriters;
		private GoldenGateSRS srs;
		private HashSet errorMessages = new HashSet();
		CollectionWriter(IndexWriter indexWriter, GoldenGateSRS srs, DumpWriter[] dumpWriters) {
			this.indexWriter = indexWriter;
			this.srs = srs;
			this.dumpWriters = dumpWriters;
		}
		void write(String docId, long updateTime, DocumentListElement dle) throws IOException {
			
			//	write document to index file
			this.indexWriter.write(docId, updateTime, dle);
			if (this.dumpWriters.length == 0)
				return;
			
			//	get document
			QueriableAnnotation doc = this.srs.getDocument(docId);
			
			//	remove XML namespace declarations from root element so XSLTs can define their own
			String[] docAns = doc.getAttributeNames();
			for (int a = 0; a < docAns.length; a++) {
				if (docAns[a].startsWith("xmlns:"))
					doc.removeAttribute(docAns[a]);
			}
			
			//	write document to dumps (if any)
			for (int d = 0; d < this.dumpWriters.length; d++) try {
				this.dumpWriters[d].write(docId, updateTime, doc);
			}
			catch (TransformerException te) {
				logError("Error exporting document '" + docId + "' to '" + this.dumpWriters[d].name + "':" + te.getMessage());
				if (this.errorMessages.add(te.getMessage()))
					logError(te);
			}						
		}
		void close() throws IOException {
			this.indexWriter.close();
			for (int d = 0; d < this.dumpWriters.length; d++)
				this.dumpWriters[d].close();
		}
	}
	
	private static class IndexWriter {
		private String outFile;
		private File genFile;
		private BufferedWriter out;
		IndexWriter(File indexFile, Map uuidPatterns) throws IOException {
			
			//	create output facilities
			this.outFile = indexFile.getAbsolutePath();
			this.genFile = new File(this.outFile + ".generating");
			if (this.genFile.exists()) {
				this.genFile.delete();
				this.genFile = new File(this.outFile + ".generating");
			}
			this.genFile.getParentFile().mkdirs();
			this.out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.genFile), "UTF-8"));
			
			//	write index header
			this.out.write("<docCollection>");
			this.out.newLine();
			
			//	write URL patterns and respective explanations
			for (Iterator upnit = uuidPatterns.keySet().iterator(); upnit.hasNext();) {
				String upName = ((String) upnit.next());
				UrlPattern up = ((UrlPattern) uuidPatterns.get(upName));
				this.out.write("<flavor");
				this.out.write(" name=\"" + AnnotationUtils.escapeForXml(up.name) + "\"");
				this.out.write(" label=\"" + AnnotationUtils.escapeForXml(up.label) + "\"");
				this.out.write(" docIdUrlPrefix=\"" + AnnotationUtils.escapeForXml(up.urlPrefix) + "\"");
				this.out.write(">");
				this.out.write(AnnotationUtils.escapeForXml(up.description));
				this.out.write("</flavor>");
				this.out.newLine();
			}
		}
		void write(String docId, long updateTime, DocumentListElement dle) throws IOException {
			String docTitle = ((String) dle.getAttribute(DOCUMENT_TITLE_ATTRIBUTE));
			this.out.write("<doc");
			this.out.write(" id=\"" + docId + "\"");
			this.out.write(" lastUpdate=\"" + updateTimestampDateFormat.format(new Date(updateTime)) + "\"");
			if (docTitle != null)
				this.out.write(" title=\"" + AnnotationUtils.escapeForXml(docTitle, true) + "\"");
			this.out.write("/>");
			this.out.newLine();
		}
		void close() throws IOException {
			
			//	finish index
			this.out.write("</docCollection>");
			this.out.newLine();
			this.out.flush();
			this.out.close();
			
			//	switch file live
			File outFile = new File(this.outFile);
			if (outFile.exists()) {
				outFile.delete();
				outFile = new File(this.outFile);
			}
			this.genFile.renameTo(outFile);
		}
	}
	
	private static class DumpWriter {
		private String name;
		private String outFile;
		private File genFile;
		private ZipOutputStream out;
		private Transformer xslt;
		private String fileExtension;
		private XmlOutputOptions options;
		DumpWriter(String name, File dumpFile, XmlOutputOptions options, Transformer xslt, String fileExtension) throws IOException {
			this.name = name;
			
			//	create output facilities
			this.outFile = dumpFile.getAbsolutePath();
			this.genFile = new File(this.outFile + ".generating");
			if (this.genFile.exists()) {
				this.genFile.delete();
				this.genFile = new File(this.outFile + ".generating");
			}
			this.genFile.getParentFile().mkdirs();
			this.out = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(this.genFile)));
			
			//	store content parameters
			this.options = options;
			this.xslt = xslt;
			this.fileExtension = fileExtension;
		}
		void write(String docId, long updateTime, QueriableAnnotation doc) throws IOException, TransformerException {
			
			//	write data to buffer first (we might get an XSLT exception, and don't want a zip entry in that case ...)
			DumpEntryBuffer buffer = new DumpEntryBuffer();
			
			//	no XSLT transformer, write plain data
			if (this.xslt == null)
				AnnotationUtils.writeXML(doc, buffer, this.options);
			
			//	do transformation
			else this.xslt.transform(new StreamSource(new AnnotationInputStream(doc, null, "utf-8", this.options)), new StreamResult(buffer));
			
			//	prepare ZIP entry
			ZipEntry ze = new ZipEntry(docId + "." + this.fileExtension);
			ze.setTime(updateTime);
			this.out.putNextEntry(ze);
			
			//	write buffered data
			buffer.writeData(this.out);
			
			//	close ZIP entry
			this.out.closeEntry();
		}
		void close() throws IOException {
			
			//	finish ZIP
			this.out.flush();
			this.out.close();
			
			//	switch file live
			File outFile = new File(this.outFile);
			if (outFile.exists()) {
				outFile.delete();
				outFile = new File(this.outFile);
			}
			this.genFile.renameTo(outFile);
		}
	}
	
	private static class DumpEntryBuffer extends Writer {
		private char[] chars = new char[8192]; // same as buffered writer
		private int size = 0;
		public void write(char[] cbuf, int off, int len) throws IOException {
			while (this.chars.length < (this.size + len)) {
				char[] chars = new char[this.chars.length * 2];
				System.arraycopy(this.chars, 0, chars, 0, this.size);
				this.chars = chars;
			}
			System.arraycopy(cbuf, off, this.chars, this.size, len);
			this.size += len;
		}
		void writeData(OutputStream byteOut) throws IOException {
			OutputStreamWriter charOut = new OutputStreamWriter(byteOut, "UTF-8");
			charOut.write(this.chars, 0, this.size);
			charOut.flush();
		}
		public void flush() throws IOException {}
		public void close() throws IOException {}
	}
	
	private class CollectionExporter extends Thread {
		boolean run = true;
		CollectionExporter() {
			super("SrsCollectionPacker");
		}
		public void run() {
			
			//	give the others a little time to start
			try {
				Thread.sleep(1000 * 10);
			} catch (InterruptedException e) {}
			
			//	keep checking for due updates
			while (this.run) {
				
				//	export dumps if due
				if (exportsDue <= System.currentTimeMillis()) {
					try {
						long ed = exportsDue;
						doExport(this, System.currentTimeMillis());
						if (exportsDue == ed) // check if generation has been re-scheduled while it was running
							exportsDue = Long.MAX_VALUE;
					}
					catch (IOException ioe) {
						logError(ioe.getClass().getName() + " while exporting collection: " + ioe.getMessage());
						logError(ioe);
					}
					catch (Exception e) {
						logError(e.getClass().getName() + " while exporting collection: " + e.getMessage());
						logError(e);
					}
					catch (Throwable t) {
						logError(t.getClass().getName() + " while exporting collection: " + t.getMessage());
						logError(t);
					}
					finally {
						exportsStart = -1;
						try {
							Thread.sleep(1000 * 10); // whatever happens, wait at least 10 seconds before next run
						} catch (InterruptedException ie) {}
					}
				}
				
				//	wait otherwise
				else synchronized(this) {
					try {
						this.wait(exportsDue - System.currentTimeMillis());
					} catch (InterruptedException ie) {}
				}
			}
		}
		synchronized void wakeup() {
			this.notify();
		}
		synchronized void shutdown() {
			this.run = false;
			this.notify();
		}
	}
}