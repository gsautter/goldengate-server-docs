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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.gamta.util.gPath.GPath;
import de.uka.ipd.idaho.gamta.util.gPath.GPathExpression;
import de.uka.ipd.idaho.gamta.util.gPath.GPathParser;
import de.uka.ipd.idaho.gamta.util.gPath.GPathVariableResolver;
import de.uka.ipd.idaho.gamta.util.gPath.exceptions.GPathException;
import de.uka.ipd.idaho.gamta.util.gPath.exceptions.GPathSyntaxException;
import de.uka.ipd.idaho.gamta.util.gPath.exceptions.VariableNotBoundException;
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
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

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
	
	/**
	 * A set of statistics fields, with the fields organized in groups. Every
	 * GoldenGATE DCS instance works on exactly one field set, which defines
	 * the whole of the statistics gathered by that instance
	 * 
	 * @author sautter
	 */
	public static class StatFieldSet {
		
		/** the label of the field set as a whole */
		public final String label;
		
		/** a custom label for the built-in DocCount field */
		public final String docCountLabel;
		
		private LinkedList variables = new LinkedList();
		private LinkedList fieldGroups = new LinkedList();
		StatFieldSet(String label, String docCountLabel) {
			this.label = label;
			this.docCountLabel = ((docCountLabel == null) ? "Number of Documents" : docCountLabel);
		}
		
		/**
		 * Retrieve the variables defined for the field set. Variables select
		 * node sets that can be referenced by data selectors for statistics
		 * fields. This is a means of caching the result of path expressions.
		 * @return an array holding the variables
		 */
		public StatVariable[] getVariables() {
			return ((StatVariable[]) this.variables.toArray(new StatVariable[this.variables.size()]));
		}
		
		/**
		 * Retrieve the field groups of the field set. Field groups bundle
		 * related fields, with each field group residing in its own database
		 * table.
		 * @return an array holding the field groups
		 */
		public StatFieldGroup[] getFieldGroups() {
			return ((StatFieldGroup[]) this.fieldGroups.toArray(new StatFieldGroup[this.fieldGroups.size()]));
		}
		
		/**
		 * Serialize the field set as XML. If the argument writer is not a
		 * <code>BufferedWriter</code>, this method wraps it in one, and calls
		 * its <code>flush()</code> method before returning.
		 * @param out the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer out) throws IOException {
			BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
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
			if (bw != out)
				bw.flush();
		}
		
		/**
		 * De-serialize a field set from its XML representation.
		 * @param fsr the reader to read from
		 * @return the de-serialized field set
		 * @throws IOException
		 */
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
					if ("link".equals(type) && (tnas != null)) {
						String link = tnas.getAttribute("link");
						if (link == null)
							return;
						String conditionStr = tnas.getAttribute("condition");
						GPathExpression condition = null;
						if (conditionStr != null) try {
							condition = GPathParser.parseExpression(conditionStr);
						}
						catch (GPathSyntaxException gpse) {
							System.out.println("StatFieldSet: Could not parse condition from '" + token.trim() + "'");
							gpse.printStackTrace(System.out);
							return;
						}
						StatFieldLink sfl = new StatFieldLink(link, condition);
						if (this.sf != null)
							this.sf.links.add(sfl);
						return;
					}
					if ("join".equals(type) && (this.sfg != null)) {
						if (tnas == null)
							return;
						String fieldName = tnas.getAttribute("fieldName");
						if (fieldName == null)
							return;
						String joinFieldName = tnas.getAttribute("joinFieldName");
						if (joinFieldName == null)
							return;
						if (joinFieldName.indexOf(".") == -1)
							return;
						this.sfg.joins.add(new StatFieldGroupJoin(fieldName, joinFieldName));
					}
					if ("field".equals(type) && (this.sfg != null)) {
						if (tnas == null) {
							if ((this.sf != null) && ((this.sf.selectors.size() != 0) || (this.sfg.virtualTableName != null)))
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
								else if (StatField.LONG_TYPE.equals(dataType)) {
									defValue = new GPathNumber(Math.round(defValue.asNumber().value));
									defAggregate = "sum";
								}
								this.sf = new StatField(this.sfg, name, label, dataType, dataLength, defValue, tnas.getAttribute("aggregate", defAggregate), tnas.getAttribute("statName"), tnas.containsAttribute("indexed"), tnas.getAttribute("colName"), tnas.containsAttribute("isRefId"));
								if (grammar.isSingularTag(token) && (this.sfg != null) && (this.sfg.virtualTableName != null)) {
									this.sfg.fields.add(this.sf);
									this.sf = null;
								}
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
							String virtualTableName = tnas.getAttribute("tableName");
							String virtualColumnPrefix = tnas.getAttribute("columnPrefix");
							String refIdFieldName = tnas.getAttribute("refId");
							if ((contextStr == null) && (virtualTableName == null))
								return;
							try {
								GPath context = ((contextStr == null) ? null : (GPathParser.parsePath(contextStr)));
								this.sfg = new StatFieldGroup(name, label, "true".equalsIgnoreCase(tnas.getAttribute("supplementary")), context, virtualTableName, virtualColumnPrefix, refIdFieldName);
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
							if ((this.sv != null) && (this.sv.selectors.size() != 0)) {
								if (this.sfg == null)
									sfs[0].variables.add(this.sv);
								else this.sfg.variables.add(this.sv);
							}
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
	
	/**
	 * A group of related fields in a statistics, e.g. the author, year, and
	 * title of a document. Each field group resides in its own database table
	 * in the server component.
	 * 
	 * @author sautter
	 */
	public static class StatFieldGroup {
		
		/** the name of the field group (has to consist of letters only, no spaces, and must not be an SQL key word; has to be unique within a field set) */
		public final String name;
		
		/** the label of the field group, i.e., a nice name for use in a UI */
		public final String label;
		
		/** is the field group supplementary, i.e., infrequently used in a UI? */
		public final boolean isSupplementary;
		
		/** the default context of the field group, i.e., a selector for the document annotations to to extract data values from */
		public final GPath defContext;
		
		/** the name of the underlying virtual table, i.e., one that can only be read, but not updated */
		public final String virtualTableName;
		
		/** the field group name to use for prefixing field names in the underlying virtual table (in case it is linked in from another statistics engine and the group name differs) */
		public final String virtualColumnPrefix;
		
		/** the fully qualified name of the field referenced by the document ID column in the underlying virtual table */
		public final String refIdFieldName;
		
		private LinkedList variables = new LinkedList();
		private LinkedList fields = new LinkedList();
		private LinkedList joins = new LinkedList();
		StatFieldGroup(String name, String label, boolean isSupplementary, GPath defContext, String virtualTableName, String virtualColumnPrefix, String refIdFieldName) {
			this.name = name;
			this.label = ((label == null) ? name : label);
			this.isSupplementary = isSupplementary;
			this.defContext = defContext;
			this.virtualTableName = ("".equals(virtualTableName) ? null : virtualTableName);
			this.virtualColumnPrefix = (("".equals(virtualColumnPrefix) || (this.virtualTableName == null)) ? null : virtualColumnPrefix);
			this.refIdFieldName = (("".equals(refIdFieldName) || (this.virtualTableName == null)) ? null : refIdFieldName);
		}
		
		/**
		 * Retrieve the custom joins of the field group.
		 * @return an array holding the joins
		 */
		public StatFieldGroupJoin[] getJoins() {
			return ((StatFieldGroupJoin[]) this.joins.toArray(new StatFieldGroupJoin[this.joins.size()]));
		}
		
		/**
		 * Retrieve the variables defined for the field group. Variables select
		 * node sets that can be referenced by data selectors for statistics
		 * fields. This is a means of caching the result of path expressions.
		 * @return an array holding the variables
		 */
		public StatVariable[] getVariables() {
			return ((StatVariable[]) this.variables.toArray(new StatVariable[this.variables.size()]));
		}
		
		/**
		 * Retrieve the fields of the group.
		 * @return an array holding the fields
		 */
		public StatField[] getFields() {
			return ((StatField[]) this.fields.toArray(new StatField[this.fields.size()]));
		}
		
		/**
		 * Serialize the field group as XML. If the argument writer is not a
		 * <code>BufferedWriter</code>, this method wraps it in one, and calls
		 * its <code>flush()</code> method before returning.
		 * @param out the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer out) throws IOException {
			BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
			bw.write("<fieldGroup" +
					" name=\"" + StatFieldSet.grammar.escape(this.name) + "\"" +
					" label=\"" + StatFieldSet.grammar.escape(this.label) + "\"" +
					(this.isSupplementary ? " supplementary=\"true\"" : "") +
					((this.defContext == null) ? "" : (" context=\"" + StatFieldSet.grammar.escape(this.defContext.toString()) + "\"")) +
					((this.virtualTableName == null) ? "" : (" tableName=\"" + StatFieldSet.grammar.escape(this.virtualTableName) + "\"")) +
					((this.virtualColumnPrefix == null) ? "" : (" columnPrefix=\"" + StatFieldSet.grammar.escape(this.virtualColumnPrefix) + "\"")) +
					((this.refIdFieldName == null) ? "" : (" refId=\"" + StatFieldSet.grammar.escape(this.refIdFieldName) + "\"")) +
					">");
			bw.newLine();
			for (Iterator jit = this.joins.iterator(); jit.hasNext();)
				((StatFieldGroupJoin) jit.next()).writeXml(bw);
			for (Iterator vit = this.variables.iterator(); vit.hasNext();)
				((StatVariable) vit.next()).writeXml(bw);
			for (Iterator fit = this.fields.iterator(); fit.hasNext();)
				((StatField) fit.next()).writeXml(bw);
			bw.write("</fieldGroup>");
			bw.newLine();
			if (bw != out)
				bw.flush();
		}
	}
	
	/**
	 * An inherit join between two groups of related fields in a statistics,
	 * forcing a join on some other criteria if both field groups are present
	 * in a query.
	 * 
	 * @author sautter
	 */
	public static class StatFieldGroupJoin {
		
		/** the local name of the field to join */
		public final String fieldName;
		
		/** the fully qualified name of the field to join with */
		public final String joinFieldName;
		
		StatFieldGroupJoin(String fieldName, String joinFieldName) {
			this.fieldName = fieldName;
			this.joinFieldName = joinFieldName;
		}
		
		/**
		 * Serialize the field group join as XML. If the argument writer is not
		 * a <code>BufferedWriter</code>, this method wraps it in one, and
		 * calls its <code>flush()</code> method before returning.
		 * @param out the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer out) throws IOException {
			BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
			bw.write("<join" +
					" fieldName=\"" + StatFieldSet.grammar.escape(this.fieldName) + "\"" +
					" joinFieldName=\"" + StatFieldSet.grammar.escape(this.joinFieldName) + "\"" +
					"/>");
			bw.newLine();
			if (bw != out)
				bw.flush();
		}
	}
	
	/**
	 * An individual statistics field, representing a single value. The value
	 * is extracted from documents by means of XPath based selectors. The first
	 * selector to return a non-false, non-0, non-empty value for a given
	 * document determines the value of the field.
	 * 
	 * @author sautter
	 */
	public static class StatField {
		static final boolean DEBUG = false;
		
		/** the type constant indicating a string field */
		public static final String STRING_TYPE = "string";
		
		/** the type constant indicating a floating point number field */
		public static final String REAL_TYPE = "real";
		
		/** the type constant indicating an integer number field */
		public static final String INTEGER_TYPE = "integer";
		
		/** the type constant indicating a long integer number field */
		public static final String LONG_TYPE = "long";
		
		/** the type constant indicating a boolean field */
		public static final String BOOLEAN_TYPE = "boolean";
		
		/** the field group the field belongs to */
		public final StatFieldGroup group;
		
		/** the name of the field group (has to consist of letters only, no spaces, and must not be an SQL key word; has to be unique within a field group) */
		public final String name;
		
		/** the full name of the field, i.e., <code>&lt;groupName&gt;.&lt;fieldName&gt;</code> */
		public final String fullName;
		
		/** the label of the field group, i.e., a nice name for use in a UI */
		public final String label;
		
		/** the data type of the field, one of <code>string</code>, <code>integer</code>, <code>real</code>, and <code>boolean</code> */
		public final String dataType;
		
		/** the length of the field (relevant only if the data type is <code>string</code>) */
		public final int dataLength;
		
		/** the default value */
		public final GPathObject defValue;
		
		private LinkedList selectors = new LinkedList();
		private LinkedList links = new LinkedList();
		
		/** the aggregation function to use by default (<code>count-distinct</code> for <code>string</code> fields, <code>sum</code> for <code>integer</code> and <code>real</code> fields) */
		public final String defAggregate;
		
		/** the name of the field in a statistics (defaults to <code>&lt;groupName&gt;&lt;fieldName&gt;</code> if not specified) */
		public final String statColName;
		
		/** index the field in the database? */
		public final boolean indexed;
		
		/** the name of the field as a column in a database table (defaults to the <code>statColName</code>) */
		public final String columnName;
		
		/** use the field as the document ID in a virtual table? */
		public final boolean isVirtualRefId;
		
		StatField(StatFieldGroup fieldGroup, String name, String label, String dataType, int dataLength, GPathObject defValue, String defAggregate, String statColName, boolean indexed, String columnName, boolean isVirtualRefId) {
			this.group = fieldGroup;
			this.name = name;
			this.fullName = (this.group.name + "." + this.name);
			this.label = ((label == null) ? (this.name.substring(0, 1).toUpperCase() + this.name.substring(1)) : label);
			this.dataType = dataType;
			this.dataLength = dataLength;
			this.defValue = defValue;
			this.defAggregate = defAggregate;
			this.statColName = ((statColName == null) ? (this.group.name.substring(0, 1).toUpperCase() + this.group.name.substring(1) + this.name.substring(0, 1).toUpperCase() + this.name.substring(1)) : statColName);
			this.indexed = indexed;
			if (columnName != null)
				this.columnName = columnName;
			else if (this.group.virtualColumnPrefix != null)
				this.columnName = (((this.group.virtualTableName == null) || (this.group.virtualColumnPrefix == null)) ? this.statColName : (("".equals(this.group.virtualColumnPrefix) ? "" : (this.group.virtualColumnPrefix.substring(0, 1).toUpperCase() + this.group.virtualColumnPrefix.substring(1))) + this.name.substring(0, 1).toUpperCase() + this.name.substring(1)));
			else this.columnName = this.statColName;
			this.isVirtualRefId = isVirtualRefId;
		}
		
		/**
		 * Extract the field value for a given document and context annotation.
		 * The context annotation is one of those selected by the parent field
		 * group's default context. This method iterates through the data
		 * selectors defined for the field. It returns the first value that (1)
		 * is non-empty for <code>string</code> fields and (2) is non-0 and not
		 * <code>NaN</code> for <code>integer</code> and <code>real</code>
		 * fields. If that is not the case for either of the data selectors,
		 * this method returns the default value.
		 * @param doc the whole document
		 * @param context the context annotation
		 * @param variables the field set variables
		 * @return the value for the field
		 */
		public GPathObject getFieldValue(QueriableAnnotation doc, QueriableAnnotation context, GPathVariableResolver variables) {
			if (DEBUG) System.out.println("Extracting field " + this.fullName);
			for (Iterator sit = this.selectors.iterator(); sit.hasNext();) try {
				StatDataSelector sds = ((StatDataSelector) sit.next());
				GPathObject sdsValue = sds.getFieldValue(doc, context, variables);
				if ((sdsValue != null) && (BOOLEAN_TYPE.equals(this.dataType) || sdsValue.asBoolean().value) && ((!REAL_TYPE.equals(this.dataType) && !INTEGER_TYPE.equals(this.dataType) && !LONG_TYPE.equals(this.dataType)) || !"NaN".equals(sdsValue.asString().value))) {
					if (DEBUG) System.out.println(" ==> " + sdsValue.toString() + " (" + sdsValue.asString().value + ")");
					return sdsValue;
				}
			}
			catch (VariableNotBoundException vnbe) { /* we just ignore this one, as it can always happen at runtime */ }
			catch (GPathException gpe) {
				System.out.println("Exception extracting field '" + this.fullName + "': " + gpe.getMessage());
			}
			if (DEBUG) System.out.println(" =D=> " + this.defValue.toString() + " (" + this.defValue.asString().value + ")");
			return this.defValue;
		}
		
		/**
		 * Find a link that is applicable to the values of the field in the
		 * result of a given query.
		 * @param query the query
		 * @return the (first) applicable link
		 */
		public StatFieldLink getFieldLink(GPathVariableResolver query) {
			for (Iterator lit = this.links.iterator(); lit.hasNext();) try {
				StatFieldLink sfl = ((StatFieldLink) lit.next());
				if (sfl.isApplicable(query))
					return sfl;
			}
			catch (VariableNotBoundException vnbe) { /* we just ignore this one, as it can always happen at runtime */ }
			catch (GPathException gpe) {
				System.out.println("Exception finding link for field '" + this.fullName + "': " + gpe.getMessage());
			}
			return null;
		}
		
		/**
		 * Serialize the field as XML. If the argument writer is not a
		 * <code>BufferedWriter</code>, this method wraps it in one, and calls
		 * its <code>flush()</code> method before returning.
		 * @param out the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer out) throws IOException {
			BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
			bw.write("<field" +
					" name=\"" + StatFieldSet.grammar.escape(this.name) + "\"" +
					" label=\"" + StatFieldSet.grammar.escape(this.label) + "\"" +
					" type=\"" + StatFieldSet.grammar.escape(this.dataType) + "\"" +
					((this.dataLength < 1) ? "" : (" length=\"" + this.dataLength + "\"")) +
					" default=\"" + StatFieldSet.grammar.escape(this.defValue.asString().value) + "\"" +
					" aggregate=\"" + StatFieldSet.grammar.escape(this.defAggregate) + "\"" +
					" statName=\"" + StatFieldSet.grammar.escape(this.statColName) + "\"" +
					(this.indexed ? " indexed=\"true\"" : "") +
					(this.statColName.equals(this.columnName) ? "" : (" colName=\"" + StatFieldSet.grammar.escape(this.columnName) + "\"")) +
					(this.isVirtualRefId ? " isRefId=\"true\"" : "") +
					">");
			bw.newLine();
			for (Iterator sit = this.selectors.iterator(); sit.hasNext();)
				((StatDataSelector) sit.next()).writeXml(bw);
			for (Iterator lit = this.links.iterator(); lit.hasNext();)
				((StatFieldLink) lit.next()).writeXml(bw);
			bw.write("</field>");
			bw.newLine();
			if (bw != out)
				bw.flush();
		}
	}
	
	/**
	 * A statistics field link. Links decorate field values with interactive
	 * functionality in an HTML based web UI.
	 * 
	 * @author sautter
	 */
	public static class StatFieldLink {
		
		/** the link pattern in which to insert a given field value to create a functional link (can either generate a link URL or a JavaScript function call, in which latter case it has to start with 'return ') */
		public final String link;
		
		/** an optional condition deciding whether or not the link is applicable to the result of a given query */
		public final GPathExpression condition;
		
		StatFieldLink(String link, GPathExpression condition) {
			this.link = link;
			this.condition = condition;
		}
		
		/**
		 * Check whether or not the link is applicable to the result of a given
		 * query.
		 * @param query the query
		 * @return true if the link is applicable
		 */
		public boolean isApplicable(GPathVariableResolver query) {
			if (this.condition == null)
				return true;
			else return GPath.evaluateExpression(this.condition, isApplicableDummyDoc, query).asBoolean().value;
		}
		private static final QueriableAnnotation isApplicableDummyDoc = Gamta.newDocument(Gamta.newTokenSequence("DUMMY", null));
		
		/**
		 * Create a link for a given field value. If the link is not applicable
		 * to the argument field value or in the context of the argument data,
		 * this method returns null.
		 * @param fieldValue the field value
		 * @param values the tupel of values the given field value belongs to 
		 * @return a link string
		 */
		public String getFieldLink(String fieldValue, StringTupel values) {
			if (fieldValue == null)
				return null;
			fieldValue = fieldValue.trim();
			if (fieldValue.length() == 0)
				return null;
			//	TODO replace other values as well
			return this.link.replaceAll("\\$value", fieldValue);
		}
		
		/**
		 * Serialize the field link as XML. If the argument writer is not a
		 * <code>BufferedWriter</code>, this method wraps it in one, and calls
		 * its <code>flush()</code> method before returning.
		 * @param out the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer out) throws IOException {
			BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
			bw.write("<link" +
					" link=\"" + StatFieldSet.grammar.escape(this.link) + "\"" +
					((this.condition == null) ? "" : (" condition=\"" + StatFieldSet.grammar.escape(this.condition.toString()) + "\"")) +
					"/>");
			bw.newLine();
			if (bw != out)
				bw.flush();
		}
	}
	
	/**
	 * A statistics variable. Variables are populated at the start of data
	 * extraction from a given document. They are then available to reference
	 * in the data selectors associated with individual statistics fields.
	 * This is a means of caching the results of sets of data selectors, most
	 * useful for annotations that (1) can be in multiple different paths and
	 * (b) are referenced in multiple field data selectors.
	 * 
	 * @author sautter
	 */
	public static class StatVariable {
		
		/** the name of the variable (has to consist of letters only, no spaces) */
		public final String name;
		
		private LinkedList selectors = new LinkedList();
		StatVariable(String name) {
			this.name = (name.startsWith("$") ? name : ("$" + name));
		}
		
		/**
		 * Extract the variable value for a given document. The context
		 * annotation is one of those selected by the parent field group's
		 * default context. This method iterates through the data selectors
		 * defined for the field. It returns the first value that (1) is
		 * non-empty for <code>string</code> fields and (2) is non-0 and not
		 * <code>NaN</code> for <code>integer</code> and <code>real</code>
		 * fields. If that is not the case for either of the data selectors,
		 * this method returns the default value.
		 * @param doc the whole document
		 * @param context the context annotation
		 * @param variables the field set variables defined so far
		 * @return the value for the variable
		 */
		public GPathObject getValue(QueriableAnnotation doc, QueriableAnnotation context, GPathVariableResolver variables) {
			if (StatField.DEBUG) System.out.println("Extracting variable " + this.name);
			for (Iterator sit = this.selectors.iterator(); sit.hasNext();) try {
				StatDataSelector sds = ((StatDataSelector) sit.next());
				GPathObject sdsValue = sds.getFieldValue(doc, context, variables);
				if ((sdsValue != null) && sdsValue.asBoolean().value && !"NaN".equals(sdsValue.asString().value)) {
					if (StatField.DEBUG) System.out.println(" ==> " + sdsValue.toString() + " (" + sdsValue.asString().value + ")");
					return sdsValue;
				}
			}
			catch (VariableNotBoundException vnbe) { /* we just ignore this one, as it can always happen at runtime */ }
			catch (GPathException gpe) {
				System.out.println("Exception extracting variable '" + this.name + "': " + gpe.getMessage());
			}
			return null;
		}
		
		/**
		 * Serialize the variable as XML. If the argument writer is not a
		 * <code>BufferedWriter</code>, this method wraps it in one, and calls
		 * its <code>flush()</code> method before returning.
		 * @param out the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer out) throws IOException {
			BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
			bw.write("<variable" +
					" name=\"" + StatFieldSet.grammar.escape(this.name) + "\"" +
					">");
			bw.newLine();
			for (Iterator sit = this.selectors.iterator(); sit.hasNext();)
				((StatDataSelector) sit.next()).writeXml(bw);
			bw.write("</variable>");
			bw.newLine();
			if (bw != out)
				bw.flush();
		}
	}
	
	/**
	 * A data selector, i.e., an individual XPath expression selecting some
	 * data value from a document or subordinate annotation.
	 * 
	 * @author sautter
	 */
	public static class StatDataSelector {
		
		/** the extractor expression (if this actually is a path, put it in parentheses to make it an expression) */
		public final GPathExpression extractor;
		
		/** a categorization value */
		public final GPathString value;
		
		/** a custom context, overwriting the one of the field group a parent field belongs to */
		public final GPath context;
		
		StatDataSelector(GPathExpression extractor, GPathString value, GPath context) {
			this.extractor = extractor;
			this.value = value;
			this.context = context;
		}
		
		/**
		 * Extract a data value from a document or subordinate annotation. If
		 * the data selector defines a custom context, this context is evaluated
		 * first on the argument document, overwriting the argument context. If
		 * the data selector has a value set, this value is returned instead of
		 * the extracted one if the result of the extractor expression corresponds
		 * to <code>true</code> in the semantics of XPath. 
		 * @param doc the whole document
		 * @param context the context to work on
		 * @param variables the field set variables
		 * @return the extracted value
		 */
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
		
		/**
		 * Serialize the data selector as XML. If the argument writer is not a
		 * <code>BufferedWriter</code>, this method wraps it in one, and calls
		 * its <code>flush()</code> method before returning.
		 * @param out the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer out) throws IOException {
			BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
			bw.write("<selector" +
					" extractor=\"" + StatFieldSet.grammar.escape(this.extractor.toString()) + "\"" +
					((this.value == null) ? "" : (" value=\"" + StatFieldSet.grammar.escape(this.value.toString()) + "\"")) +
					((this.context == null) ? "" : (" context=\"" + StatFieldSet.grammar.escape(this.context.toString()) + "\"")) +
					"/>");
			bw.newLine();
			if (bw != out)
				bw.flush();
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
		private boolean readOnly = false;
		
		/** the timestamp of the last update to the underlying statistics tables */
		public final long lastUpdated;
		
		/** Constructor
		 * @param fields the query fields the statistics was created for
		 */
		public DcStatistics(String[] fields, long lastUpdated) {
			this.fields = fields;
			this.lastUpdated = lastUpdated;
		}
		
		/**
		 * Test whether or not the statistics is read-only. If this method
		 * returns true, a call to any modification method will result in an
		 * throw an <code>IllegalStateException</code>. In particular, 
		 * statistics are read-only if they are cached in some kind of way.
		 * @return the true if the statistics is read-only
		 */
		public boolean isReadOnly() {
			return this.readOnly;
		}
		
		/**
		 * Put the statistics in read-only state. Client code that caches
		 * statistics in any way should call this method to lock instances of
		 * this class after populating them, to prevent accidental modification
		 * of cached data by their own client code. There is no way back, so
		 * once this method has been called, a statistics cannot be modified
		 * any longer.
		 */
		public void setReadOnly() {
			this.readOnly = true;
		}
		
		/**
		 * Retrieve the number of fields in the statistics.
		 * @return the number of fields
		 */
		public int getFieldCount() {
			return this.fields.length;
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
		
		public void renameKey(String key, String newKey) {
			if (this.readOnly)
				throw new IllegalStateException("Cannot rename key in read-only mode");
			super.renameKey(key, newKey);
			for (int f = 0; f < this.fields.length; f++) {
				if (key.equals(this.fields[f]))
					this.fields[f] = newKey;
			}
		}
		public void add(int index, StringTupel s) {
			if (this.readOnly)
				throw new IllegalStateException("Cannot add element in read-only mode");
			super.add(index, s);
		}
		public void addElement(StringTupel s) {
			if (this.readOnly)
				throw new IllegalStateException("Cannot add element in read-only mode");
			super.addElement(s);
		}
		public void insertElementAt(StringTupel s, int index) {
			if (this.readOnly)
				throw new IllegalStateException("Cannot insert element in read-only mode");
			super.insertElementAt(s, index);
		}
		public StringTupel remove(int index) {
			if (this.readOnly)
				throw new IllegalStateException("Cannot remove element in read-only mode");
			return super.remove(index);
		}
		public void remove(StringTupel s) {
			if (this.readOnly)
				throw new IllegalStateException("Cannot remove element in read-only mode");
			super.remove(s);
		}
		public void removeElementAt(int index) {
			if (this.readOnly)
				throw new IllegalStateException("Cannot remove element in read-only mode");
			super.removeElementAt(index);
		}
		public StringTupel set(int index, StringTupel s) {
			if (this.readOnly)
				throw new IllegalStateException("Cannot replace element in read-only mode");
			return super.set(index, s);
		}
		public void setElementAt(StringTupel s, int index) {
			if (this.readOnly)
				throw new IllegalStateException("Cannot replace element in read-only mode");
			super.setElementAt(s, index);
		}
		public void removeValue(String key) {
			if (this.readOnly)
				throw new IllegalStateException("Cannot remove key in read-only mode");
			super.removeValue(key);
		}
		public void removeDuplicates() {
			if (this.readOnly)
				throw new IllegalStateException("Cannot remove duplicates in read-only mode");
			super.removeDuplicates();
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
			
			bw.write("" + this.lastUpdated);
			bw.newLine();
			
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
			
			String lastUpdateStr = br.readLine();
			long lastUpdate = Long.parseLong(lastUpdateStr);
			
			String fieldLine = br.readLine();
			String[] fields = fieldLine.split("\\\"\\,\\\"");
			fields[0] = fields[0].substring(1);
			fields[fields.length-1] = fields[fields.length-1].substring(0, (fields[fields.length-1].length()-1));
			
			DcStatistics stat = new DcStatistics(fields, lastUpdate);
			
			StringVector keys = new StringVector();
			keys.addContent(fields);
			addCsvData(stat, br, '"', false, keys);
			
			return stat;
		}
		
		/**
		 * Write this statistics to some writer in CSV format.
		 * @param w the Writer to write to
		 * @param writeHeaderRow add the header row?
		 * @param separator the separator to use (defaults to ',')
		 * @throws IOException
		 */
		public void writeAsCSV(Writer w, boolean writeHeaderRow, String separator) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			if ((separator == null) || (separator.length() != 1))
				separator = ",";
			StringRelation.writeCsvData(bw, this, separator.charAt(0), '"', writeHeaderRow);
			if (bw != w)
				bw.flush();
		}
		
		/**
		 * Write this statistics to some writer in tab separated text format.
		 * @param w the Writer to write to
		 * @param writeHeaderRow add the header row?
		 * @throws IOException
		 */
		public void writeAsTabText(Writer w, boolean writeHeaderRow) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			String[] fields = this.getFields();
			
			if (writeHeaderRow) {
				for (int f = 0; f < fields.length; f++) {
					if (f != 0)
						bw.write('\t');
					bw.write(fields[f]);
				}
				bw.newLine();
			}
			
			for (int t = 0; t < this.size(); t++) {
				StringTupel st = this.get(t);
				for (int f = 0; f < fields.length; f++) {
					if (f != 0)
						bw.write('\t');
					bw.write(st.getValue(fields[f], ""));
				}
				bw.newLine();
			}
			
			if (bw != w)
				bw.flush();
		}
		
		/**
		 * Write this statistics to some writer in XML format. The exact format
		 * of the XML depends on the arguments: If the field tag name is null,
		 * the data resides in the attributes of otherwise empty row tags, with
		 * the field names as the attribute names. Otherwise, each field is a
		 * separate element with the same tag name, and the data field names
		 * are in a 'name' attribute to each field tag.
		 * @param w the Writer to write to
		 * @param rootTag the root tag name
		 * @param rowTag the row tag name
		 * @param fieldTag the field tag name
		 * @param fieldsToLabels mapping of field names to field labels (null
		 *        omits labels altogether)
		 * @throws IOException
		 */
		public void writeAsXML(Writer w, String rootTag, String rowTag, String fieldTag, Properties fieldsToLabels) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			String[] fields = this.getFields();
			
			bw.write("<" + rootTag);
			if (fieldsToLabels != null) {
				for (int f = 0; f < fields.length; f++)
					bw.write(" " + fields[f] + "=\"" + fieldsToLabels.getProperty(fields[f], fields[f]) + "\"");
			}
			bw.write(">");
			bw.newLine();
			
			for (int t = 0; t < this.size(); t++) {
				StringTupel st = this.get(t);
				if (fieldTag == null) {
					bw.write("<" + rowTag);
					for (int f = 0; f < fields.length; f++)
						bw.write(" " + fields[f] + "=\"" + StatFieldSet.grammar.escape(st.getValue(fields[f], "")) + "\"");
					bw.write("/>");
					bw.newLine();
				}
				else {
					bw.write("<" + rowTag + ">");
					bw.newLine();
					for (int f = 0; f < fields.length; f++) {
						bw.write("<" + fieldTag + " name=\"" + fields[f] + "\">");
						bw.write(StatFieldSet.grammar.escape(st.getValue(fields[f], "")));
						bw.write("</" + fieldTag + ">");
						bw.newLine();
					}
					bw.write("</" + rowTag + ">");
					bw.newLine();
				}
			}
			
			bw.write("</" + rootTag + ">");
			bw.newLine();
			
			if (bw != w)
				bw.flush();
		}
		
		/**
		 * Write this statistics to some writer in JSON format. The exact
		 * format of the JSON depends on the arguments: If either of fields
		 * array or labels object are included, the output is an object with at
		 * most 3 properties: fields, labels, and data. If neither are
		 * included, the output is either a plain array, or even a plain object
		 * if the statistics only comprises a single row and the respective
		 * flag is set to false. Further, if the variable name is not null or
		 * empty, the JSON object (whichever is created) will be assigned to
		 * a JavaScript variable with the argument name. This is to simplify
		 * access in JavaScript environments.
		 * @param w the Writer to write to
		 * @param assignToVariable the JavaScript variable to assign the JSON
		 *        object to, for easier access from scripts
		 * @param includeFields include an array with the field names?
		 * @param fieldsToLabels mapping of field names to field labels (null
		 *        omits labels altogether)
		 * @param singleRecordAsArray write a single record as a single array
		 *        element, or as a plain object?
		 * @throws IOException
		 */
		public void writeAsJSON(Writer w, String assignToVariable, boolean includeFields, Properties fieldsToLabels, boolean singleRecordAsArray) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			String[] fields = this.getFields();
			
			if (assignToVariable != null)
				bw.write("var " + assignToVariable + " = ");
			
			if (includeFields || (fieldsToLabels != null))
				bw.write("{"); // open container object
			
			if (includeFields) {
				bw.write("\"fields\": [");
				for (int f = 0; f < fields.length; f++)
					bw.write(((f == 0) ? "" : ", ") + "\"" + fields[f] + "\"");
				bw.write("],");
				bw.newLine();
			}
			if (fieldsToLabels != null) {
				bw.write("\"labels\": {"); bw.newLine();
				for (int f = 0; f < fields.length; f++) {
					bw.write("\"" + fields[f] + "\": \"" + fieldsToLabels.getProperty(fields[f], fields[f]) + "\"" + (((f+1) < fields.length) ? "," : ""));
					bw.newLine();
				}
				bw.write("},"); bw.newLine();
			}
			
			if (includeFields || (fieldsToLabels != null))
				bw.write("\"data\": ["); // open array in 'data' object property
			else if (singleRecordAsArray || (this.size() > 1))
				bw.write("["); // open standalone array
			bw.newLine();
			
			for (int t = 0; t < this.size(); t++) {
				StringTupel st = this.get(t);
				bw.write("{"); bw.newLine();
				for (int f = 0; f < fields.length; f++) {
					bw.write("\"" + fields[f] + "\": \"" + this.escapeForJson(st.getValue(fields[f], "")) + "\"" + (((f+1) < fields.length) ? "," : ""));
					bw.newLine();
				}
				bw.write("}" + (((t+1) < this.size()) ? "," : ""));
				bw.newLine();
			}
			
			if (includeFields || (fieldsToLabels != null)) {
				bw.write("]"); // close array in object property
				bw.newLine();
				bw.write("}"); // close container object
				bw.newLine();
			}
			else if (singleRecordAsArray || (this.size() > 1)) {
				bw.write("]"); // close standalone array
				bw.newLine();
			}
			
			if (assignToVariable != null)
				bw.write(";"); // finish variable assignment
			
			if (bw != w)
				bw.flush();
		}
		private String escapeForJson(String str) {
			if (str == null)
				return null;
			StringBuffer escaped = new StringBuffer();
			char ch;
			for (int c = 0; c < str.length(); c++) {
				ch = str.charAt(c);
				if ((ch == '\\') || (ch == '"'))
					escaped.append('\\');
				if (ch < 32)
					escaped.append(' ');
				else escaped.append(ch);
			}
			return escaped.toString();
		}
	}
}