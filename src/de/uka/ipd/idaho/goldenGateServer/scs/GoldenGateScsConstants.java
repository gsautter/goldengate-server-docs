/*
 * Copyright (c) 2006-2008, IPD Boehm, Universitaet Karlsruhe (TH)
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
 * THIS SOFTWARE IS PROVIDED BY UNIVERSITÄT KARLSRUHE (TH) AND CONTRIBUTORS 
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

package de.uka.ipd.idaho.goldenGateServer.scs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.util.ArrayList;

import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.gPath.GPath;
import de.uka.ipd.idaho.gamta.util.gPath.GPathExpression;
import de.uka.ipd.idaho.gamta.util.gPath.GPathParser;
import de.uka.ipd.idaho.gamta.util.gPath.types.GPathObject;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants;
import de.uka.ipd.idaho.htmlXmlUtil.Parser;
import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.Grammar;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.StandardGrammar;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringRelation;

/**
 * Constant bearer for GoldenGATE SRS Collection Statistics Server.
 * 
 * @author sautter
 */
public interface GoldenGateScsConstants extends GoldenGateServerConstants {
	
	/** The whole set of statistics fields*/
	public static final String statisticsFields_NODE_TYPE = "statisticsFields";
	
	/** One set of related statistics fields*/
	public static final String fieldSet_NODE_TYPE = "fieldSet";
	
	/** The name of the field set. Has to be unique. Has to consist of letters only, no whitespaces, numbers or punctuation.*/
	public static final String fieldSet_name_ATTRIBUTE = "name";
	
	/** The label of the field set, to display in the UI and to use in statistics output.*/
	public static final String fieldSet_label_ATTRIBUTE = "label";
	
	/** A single statistics field*/
	public static final String field_NODE_TYPE = "field";
	
	/** The aggregate type, i.e., count, sum, min, max, average, or ignore. Note that sum and average only work for fields of type number. Ignore will exclude the field from the statistics if it is not used as a grouping field.*/
	public static final String aggregate_NODE_TYPE = "aggregate";
	
	/** The aggregate type, i.e., count, sum, min, max, average, or ignore. Note that sum and average only work for fields of type number. Ignore will exclude the field from the statistics if it is not used as a grouping field.*/
	public static final String aggregate_type_ATTRIBUTE = "type";
	
	/** Marks an aggreagte as the one to be used if a field preceeding this field in the same field set is used for grouping. For each field, the first aggregate with this attribute set to true will be used.*/
	public static final String defHighGroup_ATTRIBUTE = "defHighGroup";
	
	/** Marks an aggreagte as the one to be used if a field following this field in the same field set is used for grouping. For each field, the first aggregate with this attribute set to true will be used.*/
	public static final String defLowGroup_ATTRIBUTE = "defLowGroup";
	
	/** An XPath predicate for extracting the field value from an SRS document. If multiple predicates exist for one field, they have to be ordered in descending restrictiveness of their 'test' expessions, as the first predicate whose test expression evaluates to true will be used.*/
	public static final String predicate_NODE_TYPE = "predicate";
	
	/** The actual XPath expresion for extracting the field value from an SRS document.*/
	public static final String predicate_expression_ATTRIBUTE = "expression";
	
	/** An boolean XPath expession for testing whether or not to use this predicate for filling the field. Defaults to true.*/
	public static final String predicate_test_ATTRIBUTE = "test";
	
	/** The field name. Has to be unique throughout the field set. Has to consist of letters only, no whitespaces, numbers or punctuation. The field name 'docId' is preserved for internal use, in whichever capitalization.*/
	public static final String field_name_ATTRIBUTE = "name";
	
	/** The field label, to display in the UI and to use in statistics output.*/
	public static final String field_label_ATTRIBUTE = "label";
	
	/** The field type, i.e., boolean, number, or string.*/
	public static final String field_type_ATTRIBUTE = "type";
	
	/** The maximum lenght of field values. Has an effect only if type is string.*/
	public static final String field_length_ATTRIBUTE = "length";
	
	/**
	 * Container for a set of statistics fields
	 * 
	 * @author sautter
	 */
	public static class FieldSet {
		
		/** the name of the field set */
		public final String name;
		
		/** the label (nice name) of the field set */
		public final String label;
		
		/**
		 * Constructor
		 * @param name the name of the field set
		 * @param label the label of the field set
		 */
		public FieldSet(String name, String label) {
			this.name = name;
			this.label = label;
		}
		
		private ArrayList fields = new ArrayList();
		
		void addField(Field field) {
			this.fields.add(field);
		}
		public Field[] getFields() {
			return ((Field[]) this.fields.toArray(new Field[this.fields.size()]));
		}
		
		/**
		 * Write an XML description of this field group to a writer.
		 * @param os the OutputStream to write to
		 * @throws IOException
		 */
		public void writeXml(OutputStream os) throws IOException {
			this.writeXml(new OutputStreamWriter(os, "UTF-8"));
		}

		/**
		 * Write an XML description of this field set to a writer.
		 * @param w the Writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer w) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			
			bw.write("<" + fieldSet_NODE_TYPE + 
					" " + fieldSet_name_ATTRIBUTE + "=\"" + this.name + "\"" +
					" " + fieldSet_label_ATTRIBUTE + "=\"" + fieldSetGrammar.escape(this.label) + "\"" +
					">");
			bw.newLine();
			
			Field[] fields = this.getFields();
			for (int f = 0; f < fields.length; f++)
				fields[f].writeXml(bw);
			
			bw.write("</" + fieldSet_NODE_TYPE + ">");
			bw.newLine();
			
			if (bw != w)
				bw.flush();
		}
		
		/**
		 * Create one or more field sets from the XML data provided by some
		 * InputStream.
		 * @param is the InputStream to read from
		 * @return one or more FieldSet objects created from the XML data
		 *         provided by the specified InputStream
		 * @throws IOException
		 */
		public static FieldSet[] readFieldSets(InputStream is) throws IOException {
			return readFieldSets(new InputStreamReader(is, "UTF-8"));
		}

		/**
		 * Create one or more field sets from the XML data provided by some
		 * Reader.
		 * @param r the Reader to read from
		 * @return one or more FieldSet objects created from the XML data
		 *         provided by the specified Reader
		 * @throws IOException
		 */
		public static FieldSet[] readFieldSets(Reader r) throws IOException {
			final ArrayList fieldSets = new ArrayList();
			fieldSetParser.stream(r, new TokenReceiver() {
				
				private FieldSet fieldSet = null;
				private Field field = null;
				
				public void close() throws IOException {
					if ((this.field != null) && (this.fieldSet != null))
						this.fieldSet.addField(this.field);
					this.field = null;
					
					if (this.fieldSet != null)
						fieldSets.add(this.fieldSet);
					this.fieldSet = null;
				}
				public void storeToken(String token, int treeDepth) throws IOException {
					if (fieldSetGrammar.isTag(token)) {
						String tokenType = fieldSetGrammar.getType(token); 
						
						if (fieldSet_NODE_TYPE.equals(tokenType)) {
							if (fieldSetGrammar.isEndTag(token)) {
								if (this.fieldSet != null)
									fieldSets.add(this.fieldSet);
								this.fieldSet = null;
							}
							else {
								TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, fieldSetGrammar);
								String name = tnas.getAttribute(fieldSet_name_ATTRIBUTE);
								if (name != null) {
									String label = tnas.getAttribute(fieldSet_label_ATTRIBUTE, "");
									this.fieldSet = new FieldSet(name, label);
								}
							}
						}
						else if (field_NODE_TYPE.equals(tokenType)) {
							
							if (fieldSetGrammar.isEndTag(token)) {
								if ((this.fieldSet != null) && (this.field != null))
									this.fieldSet.addField(this.field);
								this.field = null;
							}
							else {
								TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, fieldSetGrammar);
								String name = tnas.getAttribute(field_name_ATTRIBUTE);
								if ((name != null) && (this.fieldSet != null)) {
									String label = tnas.getAttribute(field_label_ATTRIBUTE, "");
									String type = tnas.getAttribute(field_type_ATTRIBUTE, Field.STRING_TYPE);
									int length = 1;
									try {
										length = Integer.parseInt(tnas.getAttribute(field_length_ATTRIBUTE, "1"));
									} catch (NumberFormatException e) {}
									this.field = new Field(name, this.fieldSet, label, type, length);
								}
							}
						}
						else if (aggregate_NODE_TYPE.equals(tokenType)) {
							if (fieldSetGrammar.isSingularTag(token) && (this.field != null)) {
								TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, fieldSetGrammar);
								String type = tnas.getAttribute(aggregate_type_ATTRIBUTE);
								if (type != null)
									this.field.addAggregate(new Aggregate(
										type,
										"true".equals(tnas.getAttribute(defHighGroup_ATTRIBUTE, "false")),
										"true".equals(tnas.getAttribute(defLowGroup_ATTRIBUTE, "false"))
									));
							}
						}
						else if (predicate_NODE_TYPE.equals(tokenType)) {
							if (fieldSetGrammar.isSingularTag(token) && (this.field != null)) {
								TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, fieldSetGrammar);
								String expression = tnas.getAttribute(predicate_expression_ATTRIBUTE);
								if (expression != null)
									this.field.addPredicate(new Predicate(
										expression,
										tnas.getAttribute(predicate_test_ATTRIBUTE)
									));
							}
						}
					}
				}
			});
			
			return ((FieldSet[]) fieldSets.toArray(new FieldSet[fieldSets.size()]));
		}
		
		static final Grammar fieldSetGrammar = new StandardGrammar();
		static final Parser fieldSetParser = new Parser(fieldSetGrammar);
	}
	
	/**
	 * A single statistics field
	 * 
	 * @author sautter
	 */
	public static class Field {
		
		/** the type constant indicating a string field */
		public static final String STRING_TYPE = "string";
		
		/** the type constant indicating a number field */
		public static final String NUMBER_TYPE = "number";
		
		/** the type constant indicating a boolean field */
		public static final String BOOLEAN_TYPE = "boolean";
		
		/** constant field containing the document count */
		public static final Field COUNT_FIELD = new Field("count", "Count", NUMBER_TYPE, 0);
		
		/** the name of this field */
		public final String name;
		
		/** the full name of this field, i.e., the name prefixed with the name of the field set the field belongs to */
		public final String fullName;
		
		/** the label of the field */
		public final String label;
		
		/**
		 * the type of the field, one of STRING_TYPE (the default),
		 * NUMBER_TYPE, or BOOLEAN_TYPE
		 */
		public final String type;
		
		/** the field length (relevant only for string type fields) */
		public final int length;
		
		private ArrayList aggregates = new ArrayList();
		private ArrayList predicates = new ArrayList();
		
		Field(String name, FieldSet fieldSet, String label, String type, int length) {
			this.name = name;
			this.fullName = (fieldSet.name + "_" + this.name);
			this.label = label;
			this.type = type;
			this.length = length;
		}
		
		private Field(String name, String label, String type, int length) {
			this.name = name;
			this.fullName = this.name;
			this.label = label;
			this.type = type;
			this.length = length;
		}
		
		public Aggregate[] getAggregates() {
			return ((Aggregate[]) this.aggregates.toArray(new Aggregate[this.aggregates.size()]));
		}
		void addAggregate(Aggregate aggregate) {
			this.aggregates.add(aggregate);
		}
		
		public Predicate[] getPredicates() {
			return ((Predicate[]) this.predicates.toArray(new Predicate[this.predicates.size()]));
		}
		void addPredicate(Predicate predicate) {
			this.predicates.add(predicate);
		}
		
		void writeXml(BufferedWriter bw) throws IOException {
			
			bw.write("<" + field_NODE_TYPE + 
					" " + field_name_ATTRIBUTE + "=\"" + this.name + "\"" +
					" " + field_label_ATTRIBUTE + "=\"" + FieldSet.fieldSetGrammar.escape(this.label) + "\"" +
					" " + field_type_ATTRIBUTE + "=\"" + this.type + "\"" +
					" " + field_length_ATTRIBUTE + "=\"" + this.length + "\"" +
					">");
			bw.newLine();
			
			Aggregate[] aggregates = this.getAggregates();
			for (int a = 0; a < aggregates.length; a++)
				aggregates[a].writeXml(bw);
			
			Predicate[] predicates = this.getPredicates();
			for (int p = 0; p < predicates.length; p++)
				predicates[p].writeXml(bw);
			
			bw.write("</" + field_NODE_TYPE + ">");
			bw.newLine();
		}
	}
	
	public static class Aggregate {
		
		/** the type constant indicating count aggregation */
		public static final String COUNT_TYPE = "count";
		
		/** the type constant indicating sum aggregation */
		public static final String SUM_TYPE = "sum";
		
		/** the type constant indicating min aggregation */
		public static final String MIN_TYPE = "min";
		
		/** the type constant indicating max aggregation */
		public static final String MAX_TYPE = "max";
		
		/** the type constant indicating average aggregation (applicable only to number fields) */
		public static final String AVERAGE_TYPE = "average";
		
		/** the type constant indicating ignore aggregation, i.e., ignoring a field if not used for grouping */
		public static final String IGNORE_TYPE = "ignore";
		
		public final String type;
		public final boolean defHighGroup;
		public final boolean defLowGroup;
		
		Aggregate(String type) {
			this(type, false, false);
		}
		Aggregate(String type, boolean defHighGroup, boolean defLowGroup) {
			this.type = type;
			this.defHighGroup = defHighGroup;
			this.defLowGroup = defLowGroup;
		}
		
		void writeXml(BufferedWriter bw) throws IOException {
			bw.write("<" + aggregate_NODE_TYPE + 
					" " + aggregate_type_ATTRIBUTE + "=\"" + this.type + "\"" +
					(this.defHighGroup ? (" " + defHighGroup_ATTRIBUTE + "=\"true\"") : "") +
					(this.defLowGroup ? (" " + defLowGroup_ATTRIBUTE + "=\"true\"") : "") +
					"/>");
			bw.newLine();
		}
	}
	
	public static class Predicate {
		private GPathExpression test;
		private GPathExpression predicate;
		Predicate(String predicate, String test) {
			this.predicate = GPathParser.parseExpression(predicate);
			this.test = ((test == null) ? null : GPathParser.parseExpression(test));
		}
		
		void writeXml(BufferedWriter bw) throws IOException {
			bw.write("<" + predicate_NODE_TYPE + 
					" " + predicate_expression_ATTRIBUTE + "=\"" + FieldSet.fieldSetGrammar.escape(this.predicate.toString()) + "\"" +
					((this.test == null) ? "" : (" " + predicate_test_ATTRIBUTE + "=\"" + FieldSet.fieldSetGrammar.escape(this.test.toString()) + "\"")) +
					"/>");
			bw.newLine();
		}
		
		public final boolean isApplicable(QueriableAnnotation doc) {
			return ((this.test == null) || GPath.evaluateExpression(this.test, doc, null).asBoolean().value);
		}
		
		public final GPathObject extractData(QueriableAnnotation doc) {
			return GPath.evaluateExpression(this.predicate, doc, null);
		}
	}
	
	/** the command for retrieving the field definitions */
	public static final String GET_FIELDS = "SCS_GET_FIELDS";
	
	/** the command for compiling and retrieving a statistics */
	public static final String GET_STATISTICS = "SCS_GET_STATISTICS";
	
	/**
	 * Representation of a field and the way to handle it in generating a
	 * statistics.
	 * 
	 * @author sautter
	 */
	public static class QueryField {
		private static final String operation_ATTRIBUTE = "operation";
		private static final String filter_ATTRIBUTE = "filter";
		private static final String sortPriority_ATTRIBUTE = "sortPriority";
		
		/** the operation indidation to use a field for grouping */
		public static final String GROUP_OPERATION = "group";
		
		/** the full name of the field, i.e., the field name prefixed with the name of the field group the field belongs to */
		public final String name;
		
		/** the operation to apply to the field, either grouping, or the name of an aggregate */
		public final String operation;
		
		/** the filter value to use for the field, as a string (will be converted to boolean or number according to XPath rules if the field type requires it) */
		public final String filter;
		
		/** the priority of the field in sorting the result statistics */
		public final int sortPriority;
		
		/**
		 * Constructor
		 * @param field the field the query field refers to
		 * @param operation the operation to use for the field, i.e., either an
		 *            aggregate function, or 'group'
		 * @param filter the filter value to use for this field (specify null
		 *            for not using the field for filtering)
		 * @param sortPriority the priority of the field in sorting the result
		 *            statistics (specifying a negative value excludes the field
		 *            from sorting)
		 */
		public QueryField(Field field, String operation, String filter, int sortPriority) {
			this.name = field.fullName;
			this.operation = operation;
			this.filter = filter;
			this.sortPriority = sortPriority;
		}
		
		private QueryField(String name, String operation, String filter, int sortPriority) {
			this.name = name;
			this.operation = operation;
			this.filter = filter;
			this.sortPriority = sortPriority;
		}
		
		/**
		 * Write an XML description of this query field to an output stream.
		 * @param os the OutputStream to write to
		 * @throws IOException
		 */
		public void writeXml(OutputStream os) throws IOException {
			this.writeXml(new OutputStreamWriter(os, "UTF-8"));
		}
		
		/**
		 * Write an XML description of this query field to a writer.
		 * @param w the Writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer w) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			
			bw.write("<" + field_NODE_TYPE + 
					" " + field_name_ATTRIBUTE + "=\"" + this.name + "\"" +
					" " + operation_ATTRIBUTE + "=\"" + this.operation + "\"" +
					((this.filter == null) ? "" : (" " + filter_ATTRIBUTE + "=\"" + fieldGrammar.escape(this.filter) + "\"")) +
					((this.sortPriority < 0) ? "" : (" " + sortPriority_ATTRIBUTE + "=\"" + this.sortPriority + "\"")) +
					"/>");
			bw.newLine();
			
			if (bw != w)
				bw.flush();
		}
		
		/**
		 * Create one or more query field from the XML data provided by some
		 * InputStream.
		 * @param is the InputStream to read from
		 * @return one or more QueryField objects created from the XML data
		 *         provided by the specified InputStream
		 * @throws IOException
		 */
		public static QueryField[] readQueryFields(InputStream is) throws IOException {
			return readQueryFields(new InputStreamReader(is, "UTF-8"));
		}
		
		/**
		 * Create one or more field sets from the XML data provided by some
		 * Reader.
		 * @param r the Reader to read from
		 * @return one or more QueryField objects created from the XML data
		 *         provided by the specified Reader
		 * @throws IOException
		 */
		public static QueryField[] readQueryFields(Reader r) throws IOException {
			final ArrayList fields = new ArrayList();
			fieldParser.stream(r, new TokenReceiver() {
				public void close() throws IOException {}
				public void storeToken(String token, int treeDepth) throws IOException {
					if (field_NODE_TYPE.equals(fieldGrammar.getType(token)) && fieldGrammar.isSingularTag(token)) {
						TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, fieldGrammar);
						String name = tnas.getAttribute(field_name_ATTRIBUTE);
						if (name != null) {
							String operation = tnas.getAttribute(operation_ATTRIBUTE, Aggregate.IGNORE_TYPE);
							String filter = tnas.getAttribute(filter_ATTRIBUTE);
							int sortPriority = -1;
							try {
								sortPriority = Integer.parseInt(tnas.getAttribute(sortPriority_ATTRIBUTE, "-1"));
							} catch (NumberFormatException nfe) {}
							fields.add(new QueryField(name, operation, filter, sortPriority));
						}
					}
				}
			});
			
			return ((QueryField[]) fields.toArray(new QueryField[fields.size()]));
		}
		
		private static final Grammar fieldGrammar = new StandardGrammar();
		private static final Parser fieldParser = new Parser(fieldGrammar);
	}
	
	/**
	 * A statistics generated from the data stored in SCS, in the form of a
	 * string relation, plus associated meta data.
	 * 
	 * @author sautter
	 */
	public static final class Statistics extends StringRelation {
		private final QueryField[] fields;
		
		/** Constructor
		 * @param fields the query fields the statistics was created for
		 */
		public Statistics(QueryField[] fields) {
			this.fields = fields;
		}
		
		/**
		 * Retrieve the query fields the statistics was created for. This method
		 * clones the internal array, so changing the returned array has no
		 * effect on the future behavior of this object.
		 * @return an array holding the query fields the statistics was created
		 *         for
		 */
		public QueryField[] getQueryFields() {
			QueryField[] fields = new QueryField[this.fields.length];
			System.arraycopy(this.fields, 0, fields, 0, fields.length);
			return fields;
		}
		
		/**
		 * Retrieve the keys for the CSV data representing the actual
		 * statistics.
		 * @return an array holding the keys for the CSV data representing the
		 *         actual statistics
		 */
		public String[] getDataKeys() {
			StringVector keys = new StringVector();
			for (int f = 0; f < this.fields.length; f++) {
				if (!Aggregate.IGNORE_TYPE.equals(this.fields[f].operation))
					keys.addElement(this.fields[f].name);
			}
			return keys.toStringArray();
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
			
			for (int f = 0; f < this.fields.length; f++)
				this.fields[f].writeXml(bw);
			
			StringVector writeKeys = new StringVector();
			writeKeys.addContent(this.getDataKeys());
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
		public static Statistics readStatistics(InputStream is) throws IOException {
			return readStatistics(new InputStreamReader(is, "UTF-8"));
		}
		
		/**
		 * Create a statistics from the data provided by some Reader.
		 * @param r the Reader to read from
		 * @return a statistics created from the data provided by the specified
		 *         Reader
		 * @throws IOException
		 */
		public static Statistics readStatistics(Reader r) throws IOException {
			BufferedReader br = ((r instanceof BufferedReader) ? ((BufferedReader) r) : new BufferedReader(r));
			
			StringBuffer fieldData = new StringBuffer();
			String line;
			do {
				br.mark(1024);
				line = br.readLine();
				if (line == null)
					break;
				if (line.startsWith("<"))
					fieldData.append(line);
				else {
					br.reset();
					break;
				}
			} while (true);
			
			QueryField[] fields = QueryField.readQueryFields(new StringReader(fieldData.toString()));
			
			Statistics stat = new Statistics(fields);
			
			StringRelation data = readCsvData(br, '"', true, null);
			for (int d = 0; d < data.size(); d++)
				stat.addElement(data.get(d));
			
			return stat;
		}
	}
}