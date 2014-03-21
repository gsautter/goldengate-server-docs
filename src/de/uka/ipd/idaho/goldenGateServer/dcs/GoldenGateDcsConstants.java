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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Iterator;
import java.util.LinkedList;

import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.gamta.util.gPath.GPath;
import de.uka.ipd.idaho.gamta.util.gPath.GPathExpression;
import de.uka.ipd.idaho.gamta.util.gPath.GPathParser;
import de.uka.ipd.idaho.gamta.util.gPath.GPathVariableResolver;
import de.uka.ipd.idaho.gamta.util.gPath.exceptions.GPathSyntaxException;
import de.uka.ipd.idaho.gamta.util.gPath.types.GPathNumber;
import de.uka.ipd.idaho.gamta.util.gPath.types.GPathObject;
import de.uka.ipd.idaho.gamta.util.gPath.types.GPathString;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants;
import de.uka.ipd.idaho.htmlXmlUtil.Parser;
import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.Grammar;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.StandardGrammar;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringRelation;

/**
 * Constant bearer for GoldenGATE DCS
 * 
 * @author sautter
 */
public interface GoldenGateDcsConstants extends GoldenGateServerConstants, LiteratureConstants {
	
	/** the command for retrieving the field definitions */
	public static final String GET_FIELDS_COMMAND_SUFFIX = "_GET_FIELDS";
	
	/** the command for compiling and retrieving a statistics */
	public static final String GET_STATISTICS_COMMAND_SUFFIX = "_GET_STATISTICS";
	
	//	TODO document this, all !!!
	
	public static class StatFieldSet {
		public final String label;
		public final String docCountLabel;
		private LinkedList variables = new LinkedList();
		private LinkedList fieldGroups = new LinkedList();
		StatFieldSet(String label, String docCountLabel) {
			this.label = label;
			this.docCountLabel = ((docCountLabel == null) ? "Number of Documents" : docCountLabel);
		}
		public StatVariable[] getVariables() {
			return ((StatVariable[]) this.variables.toArray(new StatVariable[this.variables.size()]));
		}
		public StatFieldGroup[] getFieldGroups() {
			return ((StatFieldGroup[]) this.fieldGroups.toArray(new StatFieldGroup[this.fieldGroups.size()]));
		}
		public void writeXml(BufferedWriter bw) throws IOException {
			bw.write("<fieldSet" +
					" label=\"" + StatFieldSet.grammar.escape(this.label) + "\"" +
					" docCountLabel=\"" + StatFieldSet.grammar.escape(this.docCountLabel) + "\"" +
					">");
			bw.newLine();
			for (Iterator vit = this.variables.iterator(); vit.hasNext();)
				((StatVariable) vit.next()).writeXml(bw);
			for (Iterator fgit = this.fieldGroups.iterator(); fgit.hasNext();)
				((StatFieldGroup) fgit.next()).writeXml(bw);
			bw.write("</fieldSet>");
			bw.newLine();
		}
		public static StatFieldSet readFieldSet(Reader fsr) throws IOException {
			if (!(fsr instanceof BufferedReader))
				fsr = new BufferedReader(fsr);
			final StatFieldSet[] sfs = {null};
			parser.stream(fsr, new TokenReceiver() {
				private StatFieldGroup sfg = null;
				private StatField sf = null;
				private StatVariable sv = null;
				public void storeToken(String token, int treeDepth) throws IOException {
					if (!grammar.isTag(token))
						return;
					String type = grammar.getType(token);
					TreeNodeAttributeSet tnas = (grammar.isEndTag(token) ? null : TreeNodeAttributeSet.getTagAttributes(token, grammar));
					if ("selector".equals(type) && (tnas != null)) {
						String extractorStr = tnas.getAttribute("extractor");
						if (extractorStr == null)
							return;
						String value = tnas.getAttribute("value");
						String contextStr = tnas.getAttribute("context");
						try {
							GPathExpression extractor = GPathParser.parseExpression(extractorStr);
							GPath context = ((contextStr == null) ? null : (GPathParser.parsePath(contextStr)));
							StatDataSelector sds = new StatDataSelector(extractor, ((value == null) ? null : new GPathString(value)), context);
							if (this.sf != null)
								this.sf.selectors.add(sds);
							else if (this.sv != null)
								this.sv.selectors.add(sds);
						}
						catch (GPathSyntaxException gpse) {
							System.out.println("StatFieldSet: Could not parse selector from '" + token.trim() + "'");
							gpse.printStackTrace(System.out);
						}
						return;
					}
					if ("field".equals(type) && (this.sfg != null)) {
						if (tnas == null) {
							if ((this.sf != null) && (this.sf.selectors.size() != 0))
								this.sfg.fields.add(this.sf);
							this.sf = null;
						}
						else {
							String name = tnas.getAttribute("name");
							if (name == null)
								return;
							String label = tnas.getAttribute("label");
							String dataType = tnas.getAttribute("type");
							if (dataType == null)
								return;
							try {
								int dataLength = (StatField.STRING_TYPE.equals(dataType) ? Integer.parseInt(tnas.getAttribute("length")) : 0);
								GPathObject defValue = new GPathString(tnas.getAttribute("default", ""));
								String defAggregate = "count-distinct";
								if (StatField.BOOLEAN_TYPE.equals(dataType))
									defValue = defValue.asBoolean();
								else if (StatField.REAL_TYPE.equals(dataType)) {
									defValue = defValue.asNumber();
									defAggregate = "sum";
								}
								else if (StatField.INTEGER_TYPE.equals(dataType)) {
									defValue = new GPathNumber(Math.round(defValue.asNumber().value));
									defAggregate = "sum";
								}
								this.sf = new StatField(this.sfg, name, label, dataType, dataLength, defValue, tnas.getAttribute("aggregate", defAggregate), tnas.getAttribute("statName"));
							}
							catch (NumberFormatException nfe) {
								System.out.println("StatFieldSet: Could not parse field from '" + token.trim() + "'");
								nfe.printStackTrace(System.out);
							}
						}
						return;
					}
					if ("fieldGroup".equals(type) && (sfs[0] != null)) {
						if (tnas == null) {
							if ((this.sfg != null) && (this.sfg.fields.size() != 0))
								sfs[0].fieldGroups.add(this.sfg);
							this.sfg = null;
						}
						else {
							String name = tnas.getAttribute("name");
							if (name == null)
								return;
							String label = tnas.getAttribute("label");
							String contextStr = tnas.getAttribute("context");
							if (contextStr == null)
								return;
							try {
								GPath context = ((contextStr == null) ? null : (GPathParser.parsePath(contextStr)));
								this.sfg = new StatFieldGroup(name, label, context);
							}
							catch (GPathSyntaxException gpse) {
								System.out.println("StatFieldSet: Could not parse selector from '" + token.trim() + "'");
								gpse.printStackTrace(System.out);
							}
						}
						return;
					}
					if ("fieldSet".equals(type) && (tnas != null)) {
						String label = tnas.getAttribute("label");
						if (label == null)
							return;
						sfs[0] = new StatFieldSet(label, tnas.getAttribute("docCountLabel"));
						return;
					}
					if ("variable".equals(type) && (sfs[0] != null)) {
						if (tnas == null) {
							if ((this.sv != null) && (this.sv.selectors.size() != 0))
								sfs[0].variables.add(this.sv);
							this.sv = null;
						}
						else {
							String name = tnas.getAttribute("name");
							if (name != null)
								this.sv = new StatVariable(name);
						}
						return;
					}
				}
				public void close() throws IOException {}
			});
			return sfs[0];
		}
		public static final Grammar grammar = new StandardGrammar();
		public static final Parser parser = new Parser(grammar);
	}
	
	public static class StatFieldGroup {
		public final String name;
		public final String label;
		public final GPath defContext;
		private LinkedList fields = new LinkedList();
		StatFieldGroup(String name, String label, GPath defContext) {
			this.name = name;
			this.label = ((label == null) ? name : label);
			this.defContext = defContext;
		}
		public StatField[] getFields() {
			return ((StatField[]) this.fields.toArray(new StatField[this.fields.size()]));
		}
		public void writeXml(BufferedWriter bw) throws IOException {
			bw.write("<fieldGroup" +
					" name=\"" + StatFieldSet.grammar.escape(this.name) + "\"" +
					" label=\"" + StatFieldSet.grammar.escape(this.label) + "\"" +
					" context=\"" + StatFieldSet.grammar.escape(this.defContext.toString()) + "\"" +
					">");
			bw.newLine();
			for (Iterator fit = this.fields.iterator(); fit.hasNext();)
				((StatField) fit.next()).writeXml(bw);
			bw.write("</fieldGroup>");
			bw.newLine();
		}
	}
	
	public static class StatField {
		static final boolean DEBUG = false;
		
		/** the type constant indicating a string field */
		public static final String STRING_TYPE = "string";
		
		/** the type constant indicating a floating point number field */
		public static final String REAL_TYPE = "real";
		
		/** the type constant indicating an integer number field */
		public static final String INTEGER_TYPE = "integer";
		
		/** the type constant indicating a boolean field */
		public static final String BOOLEAN_TYPE = "boolean";
		
		public final StatFieldGroup group;
		public final String name;
		public final String fullName;
		public final String label;
		public final String dataType;
		public final int dataLength;
		public final GPathObject defValue;
		private LinkedList selectors = new LinkedList();
		public final String defAggregate;
		public final String statColName;
		StatField(StatFieldGroup fieldGroup, String name, String label, String dataType, int dataLength, GPathObject defValue, String defAggregate, String statColName) {
			this.group = fieldGroup;
			this.name = name;
			this.fullName = (this.group.name + "." + this.name);
			this.label = ((label == null) ? (this.name.substring(0, 1).toUpperCase() + this.name.substring(1)) : label);
			this.dataType = dataType;
			this.dataLength = dataLength;
			this.defValue = defValue;
			this.defAggregate = defAggregate;
			this.statColName = ((statColName == null) ? (this.group.name.substring(0, 1).toUpperCase() + this.group.name.substring(1) + this.name.substring(0, 1).toUpperCase() + this.name.substring(1)) : statColName);
		}
		public GPathObject getFieldValue(QueriableAnnotation doc, QueriableAnnotation context, GPathVariableResolver variables) {
			if (DEBUG) System.out.println("Extracting field " + this.fullName);
			for (Iterator sit = this.selectors.iterator(); sit.hasNext();) {
				StatDataSelector sds = ((StatDataSelector) sit.next());
				GPathObject sdsValue = sds.getFieldValue(doc, context, variables);
				if ((sdsValue != null) && (BOOLEAN_TYPE.equals(this.dataType) || sdsValue.asBoolean().value) && ((!REAL_TYPE.equals(this.dataType) && !INTEGER_TYPE.equals(this.dataType)) || !"NaN".equals(sdsValue.asString().value))) {
					if (DEBUG) System.out.println(" ==> " + sdsValue.toString() + " (" + sdsValue.asString().value + ")");
					return sdsValue;
				}
			}
			if (DEBUG) System.out.println(" =D=> " + this.defValue.toString() + " (" + this.defValue.asString().value + ")");
			return this.defValue;
		}
		public void writeXml(BufferedWriter bw) throws IOException {
			bw.write("<field" +
					" name=\"" + StatFieldSet.grammar.escape(this.name) + "\"" +
					" label=\"" + StatFieldSet.grammar.escape(this.label) + "\"" +
					" type=\"" + StatFieldSet.grammar.escape(this.dataType) + "\"" +
					((this.dataLength < 1) ? "" : (" length=\"" + this.dataLength + "\"")) +
					" default=\"" + StatFieldSet.grammar.escape(this.defValue.asString().value) + "\"" +
					" aggregate=\"" + StatFieldSet.grammar.escape(this.defAggregate) + "\"" +
					" statName=\"" + StatFieldSet.grammar.escape(this.statColName) + "\"" +
					">");
			bw.newLine();
			for (Iterator sit = this.selectors.iterator(); sit.hasNext();)
				((StatDataSelector) sit.next()).writeXml(bw);
			bw.write("</field>");
			bw.newLine();
		}
	}
	
	public static class StatVariable {
		public final String name;
		private LinkedList selectors = new LinkedList();
		StatVariable(String name) {
			this.name = (name.startsWith("$") ? name : ("$" + name));
		}
		public GPathObject getValue(QueriableAnnotation doc, GPathVariableResolver variables) {
			if (StatField.DEBUG) System.out.println("Extracting variable " + this.name);
			for (Iterator sit = this.selectors.iterator(); sit.hasNext();) {
				StatDataSelector sds = ((StatDataSelector) sit.next());
				GPathObject sdsValue = sds.getFieldValue(doc, doc, variables);
				if ((sdsValue != null) && sdsValue.asBoolean().value && !"NaN".equals(sdsValue.asString().value)) {
					if (StatField.DEBUG) System.out.println(" ==> " + sdsValue.toString() + " (" + sdsValue.asString().value + ")");
					return sdsValue;
				}
			}
			return null;
		}
		public void writeXml(BufferedWriter bw) throws IOException {
			bw.write("<variable" +
					" name=\"" + StatFieldSet.grammar.escape(this.name) + "\"" +
					">");
			bw.newLine();
			for (Iterator sit = this.selectors.iterator(); sit.hasNext();)
				((StatDataSelector) sit.next()).writeXml(bw);
			bw.write("</variable>");
			bw.newLine();
		}
	}
	
	public static class StatDataSelector {
		public final GPathExpression extractor;
		public final GPathString value;
		public final GPath context;
		StatDataSelector(GPathExpression extractor, GPathString value, GPath context) {
			this.extractor = extractor;
			this.value = value;
			this.context = context;
		}
		public GPathObject getFieldValue(QueriableAnnotation doc, QueriableAnnotation context, GPathVariableResolver variables) {
			if (this.context != null) {
				if (StatField.DEBUG) System.out.println(" - applying custom context " + this.context.toString());
				QueriableAnnotation[] contexts = this.context.evaluate(doc, variables);
				if (contexts.length == 0)
					return null;
				context = contexts[0];
			}
			if (StatField.DEBUG) System.out.println(" - extracting " + this.extractor.toString());
			if (StatField.DEBUG) System.out.println("   - context is " + AnnotationUtils.produceStartTag(context));
			GPathObject value = GPath.evaluateExpression(this.extractor, context, variables);
			if (StatField.DEBUG) System.out.println("   ==> " + value + " (" + value.asString().value + ")");
			if (this.value == null)
				return (value.asBoolean().value ? value : null);
			else return (value.asBoolean().value ? this.value : null);
		}
		public void writeXml(BufferedWriter bw) throws IOException {
			bw.write("<selector" +
					" extractor=\"" + StatFieldSet.grammar.escape(this.extractor.toString()) + "\"" +
					((this.value == null) ? "" : (" value=\"" + StatFieldSet.grammar.escape(this.value.toString()) + "\"")) +
					((this.context == null) ? "" : (" context=\"" + StatFieldSet.grammar.escape(this.context.toString()) + "\"")) +
					"/>");
			bw.newLine();
		}
	}
	
	/**
	 * A statistics generated from the data stored in a DCS, in the form of a
	 * string relation, plus associated meta data.
	 * 
	 * @author sautter
	 */
	public static final class DcStatistics extends StringRelation {
		private final String[] fields;
		
		/** Constructor
		 * @param fields the query fields the statistics was created for
		 */
		public DcStatistics(String[] fields) {
			this.fields = fields;
		}
		
		/**
		 * Retrieve the data fields contained in the statistics. This method
		 * clones the internal array, so changing the returned array has no
		 * effect on the future behavior of this object.
		 * @return an array holding the data field names
		 */
		public String[] getFields() {
			String[] fields = new String[this.fields.length];
			System.arraycopy(this.fields, 0, fields, 0, fields.length);
			return fields;
		}
		
		/**
		 * Write a description of this statistics to an output stream.
		 * @param os the OutputStream to write to
		 * @throws IOException
		 */
		public void writeData(OutputStream os) throws IOException {
			this.writeData(new OutputStreamWriter(os, "UTF-8"));
		}
		
		/**
		 * Write an XML description of this query field to a writer.
		 * @param w the Writer to write to
		 * @throws IOException
		 */
		public void writeData(Writer w) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			
			StringVector writeKeys = new StringVector();
			writeKeys.addContent(this.fields);
			writeCsvData(bw, this, '"', writeKeys);
			
			if (bw != w)
				bw.flush();
		}
		
		/**
		 * Create a statistics from the data provided by some InputStream.
		 * @param is the InputStream to read from
		 * @return a statistics created from the data provided by the specified
		 *         InputStream
		 * @throws IOException
		 */
		public static DcStatistics readStatistics(InputStream is) throws IOException {
			return readStatistics(new InputStreamReader(is, "UTF-8"));
		}
		
		/**
		 * Create a statistics from the data provided by some Reader.
		 * @param r the Reader to read from
		 * @return a statistics created from the data provided by the specified
		 *         Reader
		 * @throws IOException
		 */
		public static DcStatistics readStatistics(Reader r) throws IOException {
			BufferedReader br = ((r instanceof BufferedReader) ? ((BufferedReader) r) : new BufferedReader(r));
			
			String fieldLine = br.readLine();
			String[] fields = fieldLine.split("\\\"\\,\\\"");
			fields[0] = fields[0].substring(1);
			fields[fields.length-1] = fields[fields.length-1].substring(0, (fields[fields.length-1].length()-1));
			
			DcStatistics stat = new DcStatistics(fields);
			
			StringVector keys = new StringVector();
			keys.addContent(fields);
			addCsvData(stat, br, '"', false, keys);
			
			return stat;
		}
	}
}