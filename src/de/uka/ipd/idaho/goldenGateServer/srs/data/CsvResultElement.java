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
package de.uka.ipd.idaho.goldenGateServer.srs.data;

import de.uka.ipd.idaho.stringUtils.StringUtils;

/**
 * A single element in a result that can convert its content to a String in CSV
 * (comma separated values) format.
 * 
 * @author sautter
 */
public class CsvResultElement extends SrsSearchResultElement {
	
	/**
	 * Convert the data in this result element to a line for a CSV file.
	 * @param valueDelimiter the character to use as the value delimiter (will
	 *            be escaped with itself if occurring in value)
	 * @param keys an array containing the keys whose values to include
	 * @return a String concatenated from the values of the specified keys, in
	 *         the order of the keys
	 */
	public String toCsvString(char valueDelimiter, String[] keys) {
		String delimiter = ("" + valueDelimiter);
		StringBuffer csv = new StringBuffer();
		for (int k = 0; k < keys.length; k++) {
			String value = this.getValueString(keys[k]);
			if (k != 0)
				csv.append(",");
			csv.append(delimiter + StringUtils.replaceAll(value, delimiter, (delimiter + delimiter)) + delimiter);
		}
		return csv.toString();
	}
	
	/**
	 * Convert the data in this result element to a line for a TSV file.
	 * @param keys an array containing the keys whose values to include
	 * @return a String concatenated from the values of the specified keys, in
	 *         the order of the keys
	 */
	public String toTsvString(String[] keys) {
		StringBuffer tsv = new StringBuffer();
		for (int k = 0; k < keys.length; k++) {
			String value = this.getValueString(keys[k]);
			if (k != 0)
				tsv.append("\t");
			tsv.append(value.replaceAll("\\s", " "));
		}
		return tsv.toString();
	}
	
	private String getValueString(String key) {
		Object valueObj = this.getAttribute(key);
		if (valueObj instanceof String)
			return ((String) valueObj);
		else if (valueObj instanceof CharSequence)
			return ((CharSequence) valueObj).toString();
		else return "";
	}
}
