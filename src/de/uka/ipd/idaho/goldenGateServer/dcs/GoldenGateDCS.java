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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.CountingSet;
import de.uka.ipd.idaho.gamta.util.gPath.types.GPathObject;
import de.uka.ipd.idaho.goldenGateServer.AsynchronousWorkQueue;
import de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP;
import de.uka.ipd.idaho.goldenGateServer.util.DataObjectUpdateConstants;
import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.XsltUtils;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * Generic statistics component for the document collection hosted in a
 * GoldenGATE DIO or GoldenGATE SRS. This class indexes checkin and update
 * time, year, month, and user. All domain specific indexing remains to be
 * added by sub classes.
 * 
 * @author sautter
 */
public abstract class GoldenGateDCS extends GoldenGateEXP implements GoldenGateDcsConstants, DataObjectUpdateConstants {
	private IoProvider io;
	
	private StatFieldSet fieldSet;
	private Properties fieldLabels = new Properties();
	
	private GoldenGateDcsStatEngine statEngine;
	private CountingSet queriedFields = new CountingSet(new TreeMap(String.CASE_INSENSITIVE_ORDER));
	private Set sQueriedFields = Collections.synchronizedSet(this.queriedFields);
	private long queriedSince = System.currentTimeMillis(); // simply remember startup time
	private long queriedLastLoged = System.currentTimeMillis(); // simply remember startup time
	
	private FormattedStaticStatExport[] staticStatExports = new FormattedStaticStatExport[0];
	private long staticStatExportsDone = System.currentTimeMillis(); // let there be changes before we get going
	private long staticStatExportsDue = -1;
	private StaticStatExportThread staticStatExportThread = null;
	
	/**
	 * @param letterCode the letter code to use
	 * @param statsName the statistics exporter name to use
	 */
	protected GoldenGateDCS(String letterCode, String statsName) {
		super(letterCode, statsName);
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
			StatFieldGroup[] fieldGroups = this.fieldSet.getFieldGroups();
			for (int g = 0; g < fieldGroups.length; g++) {
				StatField[] fgFields = fieldGroups[g].getFields();
				for (int f = 0; f < fgFields.length; f++) {
					this.fieldLabels.setProperty(fgFields[f].fullName, fgFields[f].label);
					this.fieldLabels.setProperty(fgFields[f].statColName, fgFields[f].label);
				}
			}
		}
		catch (IOException ioe) {
			throw new RuntimeException("DocumentCollectionStatistics: cannot work without field set definition");
		}
		
		//	create statistics engine
		String tableNamePrefix = (this.letterCode.substring(0, 1).toUpperCase() + this.letterCode.substring(1).toLowerCase()).replaceAll("[^A-Za-z]", "");
		this.statEngine = new GoldenGateDcsStatEngine(this.getExporterName(), this.fieldSet, this.io, tableNamePrefix) {
			protected GPathObject normalizeFieldValue(StatField field, GPathObject value) {
				return normalizeValue(field, value);
			}
		};
		
		//	load static exports
		try {
			this.staticStatExports = this.loadStaticStatExports();
		}
		catch (IOException ioe) {
			System.out.println("Exception loading static exports: " + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}
	}
	
	private static final String LIST_FILED_GROUPS_COMMAND = "listGroups";
	private static final String LIST_QUERIED_FIELDS_COMMAND = "listQueried";
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
				int limit = Integer.parseInt(input.readLine());
				String[] outputFields = input.readLine().split("\\s+");
				String[] groupingFields = input.readLine().split("\\s+");
				String[] orderingFields = input.readLine().split("\\s");
				Properties fieldPredicates = new Properties();
				Properties fieldAggregates = new Properties();
				Properties aggregatePredicates = new Properties();
				StringVector customFilters = new StringVector();
				for (String line; (line = input.readLine()) != null;) {
					line = line.trim();
					if (line.length() == 0)
						break;
					if (line.indexOf(":") == -1)
						continue;
					if (line.startsWith("CF:")) {
						line = line.substring("CF:".length()).trim();
						customFilters.addElementIgnoreDuplicates(URLDecoder.decode(line, ENCODING));
					}
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
				
				DcStatistics stat = getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, customFilters.toStringArray(), limit);
				
				output.write(letterCode + GET_STATISTICS_COMMAND_SUFFIX);
				output.newLine();
				stat.writeData(output);
				output.flush();
			}
		};
		cal.add(ca);
		
		//	list field groups
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_FILED_GROUPS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_FILED_GROUPS_COMMAND,
						"List the field groups in the wrapped stats engine."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				StatFieldGroup[] fgs = fieldSet.getFieldGroups();
				this.reportResult("There are " + fgs.length + " field groups in stats '" + fieldSet.label + "':");
				for (int g = 0; g < fgs.length; g++)
					this.reportResult(" - '" + fgs[g].name + "': " + fgs[g].label + ((fgs[g].virtualTableName == null) ? "" : (" (virtual, from '" + fgs[g].virtualTableName + "')")));
			}
		};
		cal.add(ca);
		
		//	list queried fields
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_QUERIED_FIELDS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_QUERIED_FIELDS_COMMAND,
						"List the fields queried in the wrapped stats engine."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				long queriedTime = (System.currentTimeMillis() - queriedSince);
				long queriedHours = ((queriedTime + ((1000 * 60 * 60) / 2)) / (1000 * 60 * 60));
				this.reportResult("These " + queriedFields.elementCount() + " fields were queried in the past " + queriedHours + " hours (" + queriedTime + "ms):");
				for (Iterator qfnit = queriedFields.iterator(); qfnit.hasNext();) {
					String queriedFieldName = ((String) qfnit.next());
					this.reportResult(" - '" + queriedFieldName + "': " + queriedFields.getCount(queriedFieldName) + " timed");
				}
			}
		};
		cal.add(ca);
		
		//	reload static export definitions
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return RELOAD_STATIC_EXPORTS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						RELOAD_STATIC_EXPORTS_COMMAND,
						"Reload the static statistics export definitions."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0)
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
				else try {
					staticStatExports = loadStaticStatExports();
				}
				catch (IOException ioe) {
					this.reportError("Exception reloading static exports: " + ioe.getMessage());
					this.reportError(ioe);
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
				String[] explanation = {
						UPDATE_STATIC_EXPORTS_COMMAND,
						"Trigger an update for static statistics exports"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					updateStaticStatExports(true);
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	protected LinkedHashMap getUpdateParams() {
		LinkedHashMap params = new LinkedHashMap();
		params.put("fieldGroups", "- <fieldGroups>: the field groups to update (optional, for filtering)");
		return params;
	}
	
	protected long encodeUpdateParams(String[] fgnParams) throws IllegalArgumentException {
		if ((fgnParams == null) || (fgnParams.length == 0))
			return 0;
		
		//	sort parameters
		LinkedHashSet validFieldGroupNames = new LinkedHashSet(); 
		StringBuffer invalidFieldGroupNames = new StringBuffer();
		StatFieldGroup[] fieldGroups = this.fieldSet.getFieldGroups();
		for (int p = 0; p < fgnParams.length; p++) {
			if (fgnParams[p] == null)
				continue;
			if (fgnParams[p].length() == 0)
				continue;
			boolean invalid = true;
			for (int g = 0; g < fieldGroups.length; g++)
				if (fieldGroups[g].name.equalsIgnoreCase(fgnParams[p])) {
					invalid = false;
					validFieldGroupNames.add(fieldGroups[g].name);
					break;
				}
			if (invalid) {
				if (invalidFieldGroupNames.length() != 0)
					invalidFieldGroupNames.append(", ");
				invalidFieldGroupNames.append(fgnParams[p]);
			}
		}
		
		//	any invalid parameters?
		if (invalidFieldGroupNames.length() != 0) {
			if (invalidFieldGroupNames.indexOf(", ") == -1)
				throw new IllegalArgumentException("field group " + invalidFieldGroupNames + " does not exist.");
			else throw new IllegalArgumentException("field groups " + invalidFieldGroupNames + " do not exist.");
		}
		
		//	anything to sensibly filter by?
		if (validFieldGroupNames.isEmpty())
			return 0;
		if (validFieldGroupNames.size() == fieldGroups.length)
			return 0;
		if (validFieldGroupNames.size() > 7)
			throw new IllegalArgumentException("can at most filter 7 field groups at a time.");
		
		//	encode parameters
		int bitsPerFieldGroupName;
		int fieldGroupNameHashMask;
		if (validFieldGroupNames.size() < 2) {
			bitsPerFieldGroupName = 32;
			fieldGroupNameHashMask = 0xFFFFFFFF;
		}
		else if (validFieldGroupNames.size() < 3) {
			bitsPerFieldGroupName = 30;
			fieldGroupNameHashMask = 0x3FFFFFFF;
		}
		else if (validFieldGroupNames.size() < 4) {
			bitsPerFieldGroupName = 20;
			fieldGroupNameHashMask = 0x000FFFFF;
		}
		else if (validFieldGroupNames.size() < 5) {
			bitsPerFieldGroupName = 15;
			fieldGroupNameHashMask = 0x00007FFF;
		}
		else if (validFieldGroupNames.size() < 6) {
			bitsPerFieldGroupName = 12;
			fieldGroupNameHashMask = 0x00000FFF;
		}
		else if (validFieldGroupNames.size() < 7) {
			bitsPerFieldGroupName = 10;
			fieldGroupNameHashMask = 0x000003FF;
		}
		else {
			bitsPerFieldGroupName = 8;
			fieldGroupNameHashMask = 0x000000FF;
		}
		long encodedParams = 0;
		for (Iterator fgnit = validFieldGroupNames.iterator(); fgnit.hasNext();) {
			String fgn = ((String) fgnit.next());
			encodedParams |= (fgn.hashCode() & fieldGroupNameHashMask);
			if (fgnit.hasNext())
				encodedParams <<= bitsPerFieldGroupName;
		}
		encodedParams |= ((validFieldGroupNames.size() & 0x00000007) << 60); // highest 4 bit store number of field group names
		return encodedParams;
	}
	
	private HashSet decodeUpdateParams(long encodedParams) {
		if (encodedParams == 0)
			return null;
		
		//	extract decoding parameters
		int fieldGroupNameCount = ((int) ((encodedParams >>> 60) & 0x00000007));
		int bitsPerFieldGroupName;
		int fieldGroupNameHashMask;
		if (fieldGroupNameCount < 2) {
			bitsPerFieldGroupName = 32;
			fieldGroupNameHashMask = 0xFFFFFFFF;
		}
		else if (fieldGroupNameCount < 3) {
			bitsPerFieldGroupName = 30;
			fieldGroupNameHashMask = 0x3FFFFFFF;
		}
		else if (fieldGroupNameCount < 4) {
			bitsPerFieldGroupName = 20;
			fieldGroupNameHashMask = 0x000FFFFF;
		}
		else if (fieldGroupNameCount < 5) {
			bitsPerFieldGroupName = 15;
			fieldGroupNameHashMask = 0x00007FFF;
		}
		else if (fieldGroupNameCount < 6) {
			bitsPerFieldGroupName = 12;
			fieldGroupNameHashMask = 0x00000FFF;
		}
		else if (fieldGroupNameCount < 7) {
			bitsPerFieldGroupName = 10;
			fieldGroupNameHashMask = 0x000003FF;
		}
		else {
			bitsPerFieldGroupName = 8;
			fieldGroupNameHashMask = 0x000000FF;
		}
		
		//	collect matching field group names
		HashSet fieldGroupNames = new HashSet();
		StatFieldGroup[] fieldGroups = this.fieldSet.getFieldGroups();
		for (int p = 0; p < fieldGroupNameCount; p++) {
			int fieldGroupNameHashSuffix = ((int) (encodedParams & fieldGroupNameHashMask));
			StringBuffer matchFieldGroupNames = new StringBuffer();
			for (int g = 0; g < fieldGroups.length; g++)
				if ((fieldGroups[g].name.hashCode() & fieldGroupNameHashMask) == fieldGroupNameHashSuffix) {
					fieldGroupNames.add(fieldGroups[g].name);
					if (matchFieldGroupNames.length() != 0)
						matchFieldGroupNames.append(", ");
					matchFieldGroupNames.append(fieldGroups[g].name);
				}
			encodedParams >>>= bitsPerFieldGroupName;
			if (matchFieldGroupNames.length() == 0)
				this.logWarning("Invalid field group name hash suffix " + Integer.toString(fieldGroupNameHashSuffix, 16).toUpperCase());
			else if (matchFieldGroupNames.indexOf(", ") != -1)
				this.logWarning("Ambiguous field group name hash suffix " + Integer.toString(fieldGroupNameHashSuffix, 16).toUpperCase() + ", matches " + matchFieldGroupNames);
		}
		return fieldGroupNames;
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
			return ((String) doc.getAttribute(DOCUMENT_NAME_ATTRIBUTE, doc.getAttribute(DOCUMENT_ID_ATTRIBUTE)));
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
	 * @param customFilters custom filters to apply to the statistics fields
	 * @return the requested statistics, packed in a string relation
	 * @throws IOException
	 */
	public DcStatistics getStatistics(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, String[] customFilters) {
		for (Iterator qfnit = fieldPredicates.keySet().iterator(); qfnit.hasNext();)
			this.sQueriedFields.add((String) qfnit.next());
		this.logQueried();
		return this.statEngine.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, customFilters);
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
		for (Iterator qfnit = fieldPredicates.keySet().iterator(); qfnit.hasNext();)
			this.sQueriedFields.add((String) qfnit.next());
		this.logQueried();
		return this.statEngine.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, customFilters, limit);
	}
	
	private void logQueried() {
		long time = System.currentTimeMillis();
		if ((this.queriedLastLoged + (1000 * 60 * 60)) < time) try {
			long queriedTime = (time - this.queriedSince);
			long queriedHours = ((queriedTime + ((1000 * 60 * 60) / 2)) / (1000 * 60 * 60));
			ArrayList qfns = new ArrayList(this.sQueriedFields); // need to use intermediate list to avoid concurrent modification exceptions of iterator
			this.logAlways("These " + qfns.size() + " " + this.getLetterCode() + " fields were queried in the past " + queriedHours + " hours (" + queriedTime + "ms):");
			for (int f = 0; f < qfns.size(); f++) {
				String queriedFieldName = ((String) qfns.get(f));
				this.logAlways(" - '" + queriedFieldName + "': " + this.queriedFields.getCount(queriedFieldName) + " timed");
			}
			this.queriedLastLoged = time;
		}
		catch (RuntimeException re) {
			this.logError("GoldenGate" + this.getLetterCode() + ": error logging queried fields: " + re.getMessage());
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doUpdate(java.lang.String, java.lang.String, java.util.Properties, long)
	 */
	protected void doUpdate(String dataId, String user, Properties dataAttributes, long params) throws IOException {
		this.doUpdate(this.binding.getDocument(dataId), dataAttributes, params);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doUpdate(de.uka.ipd.idaho.gamta.QueriableAnnotation, java.util.Properties)
	 */
	protected void doUpdate(QueriableAnnotation doc, Properties docAttributes) throws IOException {
		this.doUpdate(doc, docAttributes, 0);
	}
	
	private void doUpdate(QueriableAnnotation doc, Properties docAttributes, long params) throws IOException {
		
		//	get field groups to update
		HashSet fieldGroups = this.decodeUpdateParams(params);
		
		//	update data tables
		this.statEngine.updateDocument(doc, fieldGroups);
		
		//	trigger update for static exports
		this.updateStaticStatExports(false);
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
		
		//	clean up data tables
		this.statEngine.deleteDocument(docId);
		
		//	trigger update for static exports
		this.updateStaticStatExports(false);
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
		private boolean xmlIncludeLabels = false;
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
		private String[] customFilters;
		
		FormattedStaticStatExport(String destination, String format, int limit) {
			super(destination);
			this.format = format;
			this.limit = limit;
		}
		
		void exportStaticStat() throws IOException {
			
			//	get statistics & fields
			DcStatistics stats = getStatistics(this.outputFields, this.groupingFields, this.orderingFields, this.fieldPredicates, this.fieldAggregates, this.aggregatePredicates, this.customFilters, this.limit);
			
			//	create export file & writer
			File destCreateFile = new File(this.destination + ".exporting");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(destCreateFile), "UTF-8"));
			
			//	write statistics, dependent on format
			if ("CSV".equals(this.format))
				stats.writeAsCSV(bw, true, this.csvSeparator);
			else if ("XML".equals(this.format))
				stats.writeAsXML(bw, this.xmlRootTag, this.xmlRowTag, this.xmlFieldTag, (this.xmlIncludeLabels ? fieldLabels : null));
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
					logError("Exception running derivative static export to '" + ((StaticStatExport) this.xmlDerivatives.get(d)).destination + "': " + ioe.getMessage());
					logError(ioe);
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
				logError("Export to '" + this.destination + "' failed: " + te.getMessage());
				logError(te);
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
		BufferedReader fsseBr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(this.dataPath, "staticExports.xml")), "UTF-8"));
		final ArrayList fsseList = new ArrayList();
		StatFieldSet.parser.stream(fsseBr, new TokenReceiver() {
			private FormattedStaticStatExport fsse = null;
			private StringVector outputFields = null;
			private StringVector groupingFields = null;
			private StringVector orderingFields = null;
			private Properties fieldPredicates = null;
			private Properties fieldAggregates = null;
			private Properties aggregatePredicates = null;
			private StringVector customFilters = null;
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
							this.fsse.customFilters = this.customFilters.toStringArray();
							fsseList.add(this.fsse);
						}
						this.fsse = null;
						this.outputFields = null;
						this.groupingFields = null;
						this.orderingFields = null;
						this.fieldPredicates = null;
						this.fieldAggregates = null;
						this.aggregatePredicates = null;
						this.customFilters = null;
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
							this.fsse.xmlFieldTag = tnas.getAttribute("fieldTag", null /* statField */); // have field tag default to null to switch it off by default
							this.fsse.xmlIncludeLabels = "true".equals(tnas.getAttribute("includeLabels", "true"));
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
						this.customFilters = new StringVector();
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
					else this.groupingFields.addElement(name);
					String aggregatePredicate = tnas.getAttribute("aggregatePredicate");
					if (aggregatePredicate != null)
						this.aggregatePredicates.setProperty(name, aggregatePredicate);
					String sortOrder = tnas.getAttribute("sort");
					if (sortOrder != null)
						this.orderingFields.addElement(("desc".equalsIgnoreCase(sortOrder) ? "-" : "") + name);
				}
				
				else if ("filter".equals(type)) {
					TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, StatFieldSet.grammar);
					String target = tnas.getAttribute("target", "raw");
					String leftField = tnas.getAttribute("left");
					String operator = tnas.getAttribute("operator");
					String rightField = tnas.getAttribute("right");
					if ((leftField != null) && (operator != null) && (rightField != null))
						this.customFilters.addElementIgnoreDuplicates(target + " " + leftField + " " + operator + " " + rightField);
				}
				
				else if ("derivative".equals(type) && "XML".equals(this.fsse.format)) {
					TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, StatFieldSet.grammar);
					String destination = tnas.getAttribute("destination");
					if (destination == null)
						return;
					String xslt = tnas.getAttribute("xslt");
					if (xslt != null) try {
						Transformer transformer = XsltUtils.getTransformer(new File(dataPath, xslt), false);
						this.fsse.xmlDerivatives.add(new DerivativeStaticStatExport(destination, this.fsse, transformer));
					}
					catch (IOException ioe) {
						logError("Exception creating export to '" + destination + "': " + ioe.getMessage());
						logError(ioe);
					}
				}
			}
			public void close() throws IOException {}
		});
		
		return ((FormattedStaticStatExport[]) fsseList.toArray(new FormattedStaticStatExport[fsseList.size()]));
	}
	
	private class StaticStatExportThread extends Thread {
		AsynchronousWorkQueue monitor = null;
		StaticStatExportThread() {
			super("DcsStaticStatisticsExportThread");
			staticStatExportThread = this;
			this.monitor = new AsynchronousWorkQueue("DcsStaticStatisticsExport") {
				public String getStatus() {
					return (this.name + ": exports due in " + (staticStatExportsDue - System.currentTimeMillis()) + " ms");
				}
			};
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
						sleep(Math.max(1, (time - staticStatExportsDue)) /* make sure there is no negative timeout (we are not synchronized, so due time might change) */);
					} catch (InterruptedException ie) {}
				}
				
				//	do exports
				for (int x = 0; x < staticStatExports.length; x++) try {
					staticStatExports[x].exportStaticStat();
				}
				catch (Exception e) {
					logError("Exception running static export to '" + staticStatExports[x].destination + "': " + e.getMessage());
					logError(e);
				}
			}
			finally {
				this.monitor.dispose();
				staticStatExportThread = null;
				staticStatExportsDone = System.currentTimeMillis();
			}
		}
	}
	
	private void updateStaticStatExports(boolean immediately) {
		if (immediately)
			this.staticStatExportsDone = -1;
		long time = System.currentTimeMillis();
		if ((time - this.staticStatExportsDone) < (1000 * 60 * 30))
			return;
		this.staticStatExportsDue = (time + (immediately ? 0 : (1000 * 60 * 2)));
		if (this.staticStatExportThread == null)
			this.staticStatExportThread = new StaticStatExportThread();
	}
//	
//	//	TEST FOR STATIC EXPORTS
//	public static void main(String[] args) throws Exception {
//		
//	}
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