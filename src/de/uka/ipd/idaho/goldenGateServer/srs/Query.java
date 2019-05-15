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
package de.uka.ipd.idaho.goldenGateServer.srs;


import java.util.ArrayList;
import java.util.Properties;
import java.util.Vector;

/**
 * @author sautter
 *
 */
public class Query {
	
	private Properties variables = new Properties();
	private String indexNameMask = null;
	
	private Vector partialResults = new Vector();
	private QueryResult result = null;
	
	/**	void Constructor
	 */
	public Query() {}
	
	/** add a variable to this Query
	 * @param	name	the name of the variable to add
	 * @param	value	the value of the variable to add
	 */
	public void setValue(String name, String value) {
		this.variables.setProperty(name, value);
	}
	
	/** add a variable to this Query which is dedicated for some custom indexer
	 * @param	indexName	the name of the Indexer the variable is dedicated to
	 * @param	name		the name of the variable to add
	 * @param	value		the value of the variable to add
	 * Note: this method does the same as invoking setValue(indexerID.name, value)
	 */
	public void setValue(String indexName, String name, String value) {
		this.setValue((indexName + "." + name), value);
	}
	
	/**	set the index name mask (this index name will be put before any variable name automatically)
	 * @param	mask	the Indexer ID to use for masking the variable names (null resets the mask)
	 */
	public void setIndexNameMask(String mask) {
		this.indexNameMask = mask;
	}
	
	/** retrieve the value of a query variable
	 * @param	name	the name of the variable to retrieve (will be prefixed with Indexer ID mask if set)
	 * @return the value of the variable with the specified name, of null, if there is no such variable
	 */
	public String getValue(String name) {
		return this.getValue(name, null);
	}
	
	/** retrieve the value of a query variable
	 * @param	name	the name of the variable to retrieve (will be prefixed with index name mask if set)
	 * @param	def		the value to return if the variable with the specified name does not exist
	 * @return the value of the variable with the specified name, of def, if there is no such variable
	 */
	public String getValue(String name, String def) {
		if (this.indexNameMask != null) name = (this.indexNameMask + "." + name);
		return this.variables.getProperty(name, def);
	}
	
	
	/**	add a partial result to the query contained in this Envelope
	 * @param	partialResult	the partial result to be added
	 */
	public void addPartialResult(QueryResult partialResult) {
		if (partialResult != null) this.partialResults.add(partialResult);
	}
	
	/**	@return	the partial results of the query contained in this query
	 */
	public QueryResult[] getPartialResults() {
		return ((QueryResult[]) this.partialResults.toArray(new QueryResult[this.partialResults.size()]));
	}
	
	/**	set the final result of this query
	 * @param	result		the QueryResult containing the final result of the query
	 */
	public void setResult(QueryResult result) {
		this.result = result;
	}
	
	/**	@return	the final result of this query
	 */
	public QueryResult getResult() {
		return this.result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		ArrayList vnl = new ArrayList(this.variables.keySet());
		StringBuffer sb = new StringBuffer();
		for (int n = 0; n < vnl.size(); n++) {
			if (sb.length() != 0)
				sb.append("&");
			sb.append(vnl.get(n) + "=" + this.variables.getProperty(vnl.get(n).toString()));
		}
		return sb.toString();
	}
}
