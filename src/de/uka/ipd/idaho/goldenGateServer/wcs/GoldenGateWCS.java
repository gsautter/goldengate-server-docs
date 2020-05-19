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
package de.uka.ipd.idaho.goldenGateServer.wcs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Stack;
import java.util.TreeMap;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.Token;
import de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP;
import de.uka.ipd.idaho.htmlXmlUtil.Parser;
import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.XsltUtils;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.Grammar;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.StandardGrammar;

/**
 * The GoldenGATE Wiki Connector Server forwards changes to a document
 * collection to MediaWikis. Connection to server components that store
 * documents and issue update events works via specific sub classes. This class
 * provides a centralized update queue.<br>
 * WCS can forward updates to several MediaWikis. The connection to each one has
 * to be specified in the config file. Assuming that &lt;wikiName&gt; represents
 * the name of the connected MediaWiki, you need to enter the following
 * parameters in WCS's config file:
 * <ul>
 * <li><b>&lt;wikiName&gt;.wikiUrl</b>: the URL of the MediaWiki to connect to</li>
 * <li><b>&lt;wikiName&gt;.wikiUser</b>: the login name for the connected
 * MediaWiki</li>
 * <li><b>&lt;wikiName&gt;.wikiPassword</b>: the password for the connected
 * MediaWiki</li>
 * <li><b>&lt;wikiName&gt;.xslt</b>: the URL of the XSLT stylesheet to use for
 * generating the Wiki syntax; if the value of this parameter does not start
 * with 'http://', it is interpreted as a file name relative to WCS data path</li>
 * <li><b>&lt;wikiName&gt;.deduplicateLinks</b>: if set to true, links are
 * de-duplicated in the Wiki syntax, retaining only the first link to each given
 * link target (optional parameter, default is false); this parameter has to be
 * set to true for uploads to the Wikipedia, whose rules are the only reason
 * this parameter exists in first place</li>
 * <li><b>&lt;wikiName&gt;.createOnly</b>: if set to true, Wiki pages are
 * created only once, but will not be updated later (optional parameter, default
 * is false); setting this parameter to true makes sense mostly in scenarios
 * where stubs of Wiki pages are created automatically, but entering the actual
 * data is left to users</li>
 * <li><b>&lt;wikiName&gt;.overwriteUserUpdates</b>: if set to true, Wiki
 * pages that have been modified by a user other than the one WCS uses for
 * connection are overwritten with the next update, thereby possibly destroying
 * user edits (optional parameter, default is false); setting this parameter to
 * true makes sense mostly in scenarios where a Wiki is the web front-end for
 * presenting data, but cannot be edited by users</li>
 * </ul>
 * <br>
 * Documents need to be transformed to Wiki syntax through an XSLT stylesheet
 * before upload. Due to the Wiki syntax being very sensitive to whitespace and
 * line breaks, the XML sent to the stylesheet does not contain any line breaks,
 * but only single spaces where they are required. In addition, spaces are
 * represented as '&amp;#x20;' in order to prevent spaces between tags from
 * being ignored. Therefore, output escaping should be disabled throughout the
 * stylesheet whenever printing textual content.<br>
 * It is also possible to check quality criterions for a document before
 * exporting it. To do so, the XSLT has to provide templates that match a
 * document if it does not comply with a specific quality criterion. The output
 * of these templates has to start with 'ERROR:' in order for WCS to recognize
 * it. After the colon, you can specify an error message to appear in the log.<br>
 * 
 * @author sautter
 */
public abstract class GoldenGateWCS extends GoldenGateEXP {
	
	/** the attribute to store a Wiki link target in */
	protected static final String LINK_QUERY_ATTRIBUTE = "query";
	
	/**
	 * 
	 * @author sautter
	 */
	private class Wiki {
		String wikiUrl;
		String wikiUser;
		String wikiPassword;
		
		String xsltUrl;
		Transformer xslt;
		
		boolean deduplicateLinks = false;
		boolean createOnly = false;
		boolean overwriteUserUpdates = false;
		
		WikiConnection wikiCon;
		
		Wiki(String wikiUrl, String wikiUser, String wikiPassword, String xsltUrl) {
			this.wikiUrl = wikiUrl;
			this.wikiUser = wikiUser;
			this.wikiPassword = wikiPassword;
			this.xsltUrl = xsltUrl;
			this.loadXslt(true);
		}
		
		WikiConnection getWikiConnection() throws IOException {
			if (this.wikiCon == null) {
				WikiConnection wc = new WikiConnection(this.wikiUrl);
				if (wc.login(this.wikiUser, this.wikiPassword))
					this.wikiCon = wc;
			}
			return this.wikiCon;
		}
		
		void loadXslt(boolean allowCache) {
			if (this.xsltUrl != null) try {
				if (this.xsltUrl.startsWith("http://") || this.xsltUrl.startsWith("https://"))
					this.xslt = XsltUtils.getTransformer(this.xsltUrl, allowCache);
				else this.xslt = XsltUtils.getTransformer(new File(dataPath, this.xsltUrl), allowCache);
			}
			catch (IOException ioe) {
				logError("GoldenGateWCS: error loading XSL Transformer - " + ioe.getMessage());
				logError(ioe);
			}
		}
		
		void shutdown() {
			if (this.wikiCon != null) {
				this.wikiCon.logout();
				logInfo("  - logged out from Wiki " + this.wikiUrl);
				this.wikiCon = null;
			}
		}
	}
	
	private TreeMap wikis = new TreeMap();
	
	/**
	 * Constructor allowing sub classes to submit a constant default letter
	 * code. Note that any sub class needs to provide a no-argument constructor
	 * in order to allow for class loading.
	 * @param letterCode the letter code for this component
	 * @param wikiName the Wiki exporter name to use
	 */
	public GoldenGateWCS(String letterCode, String wikiName) {
		super(letterCode, wikiName);
	}
	
	/**
	 * This implementation loads the connected Wikis. Sub classes overwriting
	 * this method thus have to make the super invocation.
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	load Wikis
		Wiki defaultWiki = this.loadWiki(this.configuration);
		if (defaultWiki != null)
			this.wikis.put("Wiki", defaultWiki);
		String[] wikiNames = this.configuration.getSubsetPrefixes();
		for (int w = 0; w < wikiNames.length; w++) {
			Wiki wiki = this.loadWiki(this.configuration.getSubset(wikiNames[w]));
			if (wiki != null)
				this.wikis.put(wikiNames[w], defaultWiki);
		}
		
		//	initialize super class
		super.initComponent();
	}
	
	private Wiki loadWiki(Settings wikiData) {
		
		//	load Wiki address and account data
		String wikiUrl = wikiData.getSetting("wikiUrl");
		String wikiUser = wikiData.getSetting("wikiUser");
		String wikiPassword = wikiData.getSetting("wikiPassword");
		if ((wikiUrl == null) || (wikiUser == null) || (wikiPassword == null))
			return null;
		
		//	load XSLT
		String xsltUrl = wikiData.getSetting("xslt");
		if (xsltUrl == null)
			return null;
		
		//	create Wiki
		Wiki wiki = new Wiki(wikiUrl, wikiUser, wikiPassword, xsltUrl);
		
		//	check whether to de-duplicate links (stupid Wikipedia rule ...)
		wiki.deduplicateLinks = "true".equalsIgnoreCase(wikiData.getSetting("deduplicateLinks"));
		
		//	check whether or not to overwrite existing pages
		wiki.createOnly = "true".equalsIgnoreCase(wikiData.getSetting("createOnly"));
		
		//	check whether or not to overwrite updates made by other users
		wiki.overwriteUserUpdates = "true".equalsIgnoreCase(wikiData.getSetting("overwriteUserUpdates"));
		
		//	finally ...
		return wiki;
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateScf.AbstractServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		super.exitComponent();
		for (Iterator wit = this.wikis.keySet().iterator(); wit.hasNext();) {
			String wikiName = ((String) wit.next());
			((Wiki) this.wikis.get(wikiName)).shutdown();
			System.out.println("  - logged out from " + wikiName);
		}
	}
	
	private static final String LIST_WIKIS_COMMAND = "list";
	private static final String UPDATE_XSLT_COMMAND = "updateXslt";
	private static final String LOGOUT_WIKI_COMMAND = "logout";
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(super.getActions()));
		ComponentAction ca;
		
		//	list connected Wikis
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_WIKIS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_WIKIS_COMMAND,
						"List the connected Wikis."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					for (Iterator wit = wikis.keySet().iterator(); wit.hasNext();) {
						String wikiName = ((String) wit.next());
						Wiki wiki = ((Wiki) wikis.get(wikiName));
						this.reportResult(" - " + wikiName + ": " + wiki.wikiUrl);
					}
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no argument.");
			}
		};
		cal.add(ca);
		
		//	reload xslt to allow deploying modifications at runtime
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return UPDATE_XSLT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						UPDATE_XSLT_COMMAND + " <wikiName>",
						"Reload the XSLT styleshet for transforming documents to Wiki syntax:",
						"- <wikiName>: the name of the wiki to re-load the stylesheet for"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					Wiki wiki = ((Wiki) wikis.get(arguments[0]));
					if (wiki == null)
						this.reportError(" Invalid Wiki '" + arguments[0] + "'");
					else wiki.loadXslt(false);
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the name of the Wiki to update as the only argument.");
			}
		};
		cal.add(ca);
		
		//	log out from some Wiki, e.g. to force a re-login
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LOGOUT_WIKI_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LOGOUT_WIKI_COMMAND + " <wikiName>",
						"Log out from a connected Wiki to force re-authentication:",
						"- <wikiName>: the name of the wiki to log out from"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					Wiki wiki = ((Wiki) wikis.get(arguments[0]));
					if (wiki == null)
						this.reportError(" Invalid Wiki '" + arguments[0] + "'");
					else wiki.shutdown();
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the name of the Wiki to log out from as the only argument.");
			}
		};
		cal.add(ca);
		
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private static Grammar grammar = new StandardGrammar();
	private static Parser parser = new Parser(grammar);
	private class WikiConnection {
		
		private URL url;
		
		private String lguserid;
		private String lgusername;
		private String lgtoken;
		private String sessionid;
		private String cookieprefix;
		
		private String editToken;
		
		WikiConnection(String urlString) throws IOException {
			this.url = new URL(urlString);
		}
		
		boolean login(final String userName, String password) throws IOException {
			HttpURLConnection con;
			
			con = this.getConnection();
			this.sendQuery(con, "action=login" +
					"&lgname=" + URLEncoder.encode(userName, "UTF-8") + 
					"&lgpassword=" + URLEncoder.encode(password, "UTF-8")
				);
			parser.stream(new InputStreamReader(con.getInputStream(), "UTF-8"), new TokenReceiver() {
				public void close() throws IOException {}
				public void storeToken(String token, int treeDepth) throws IOException {
					if (grammar.isTag(token) && "login".equals(grammar.getType(token))) {
						logDebug(token);
						TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
						String result = tnas.getAttribute("result");
						if ("Success".equals(result)) {
							lguserid = tnas.getAttribute("lguserid");
							lgusername = tnas.getAttribute("lgusername");
							lgtoken = tnas.getAttribute("lgtoken");
							sessionid = tnas.getAttribute("sessionid");
							cookieprefix = tnas.getAttribute("cookieprefix");
							logInfo("Login Successful");
						}
						else if ("NeedToken".equals(result)) {
							lgusername = userName;
							sessionid = tnas.getAttribute("sessionid");
							cookieprefix = tnas.getAttribute("cookieprefix");
							lgtoken = tnas.getAttribute("token");
							logInfo("Login Successful");
						}
						else logWarning("Login Error: " + result);
					}
				}
			});
			
			if ((this.lguserid == null) && (this.lgtoken != null)) {
				con = this.getConnection();
				this.sendQuery(con, "action=login" +
						"&lgname=" + URLEncoder.encode(userName, "UTF-8") + 
						"&lgpassword=" + URLEncoder.encode(password, "UTF-8") +
						"&lgtoken=" + URLEncoder.encode(this.lgtoken, "UTF-8")
					);
				parser.stream(new InputStreamReader(con.getInputStream(), "UTF-8"), new TokenReceiver() {
					public void close() throws IOException {}
					public void storeToken(String token, int treeDepth) throws IOException {
						if (grammar.isTag(token) && "login".equals(grammar.getType(token))) {
							logDebug(token);
							TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
							String result = tnas.getAttribute("result");
							if ("Success".equals(result)) {
								lguserid = tnas.getAttribute("lguserid");
								lgusername = tnas.getAttribute("lgusername");
								lgtoken = tnas.getAttribute("lgtoken");
								sessionid = tnas.getAttribute("sessionid");
								cookieprefix = tnas.getAttribute("cookieprefix");
								logInfo("Login Successful");
							}
							else logWarning("Login Error: " + result);
						}
					}
				});
			}
			
			if (this.cookieprefix == null)
				return false;
			
			con = this.getConnection();
			this.sendQuery(con, "action=query" +
					"&prop=info" +
					"&intoken=edit" +
					"&indexpageids=" +
					"&titles=GoldenGateWCS" + 
					""
				);
			parser.stream(new InputStreamReader(con.getInputStream(), "UTF-8"), new TokenReceiver() {
				public void close() throws IOException {}
				public void storeToken(String token, int treeDepth) throws IOException {
					if (grammar.isTag(token) && "page".equals(grammar.getType(token)) && !grammar.isEndTag(token)) {
						logDebug(token);
						TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
						editToken = tnas.getAttribute("edittoken");
					}
				}
			});
			
			if (this.editToken == null) {
				logWarning("Could not obtain Edit Token");
				return false;
			}
			else {
				logInfo("Got Edit Token: " + this.editToken);
				return true;
			}
		}
		
		Writer getUpdateWriter(final String title) throws IOException {
			return new Writer() {
				private HttpURLConnection con;
				private BufferedWriter out;
				
				private void checkConnected() throws IOException {
					if (this.con != null)
						return;
					
					this.checkError();
					
					this.con = getConnection();
					this.con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
					
					this.out = new BufferedWriter(new OutputStreamWriter(this.con.getOutputStream(), "UTF-8"));
					this.out.write("action=edit" +
							"&summary=" + URLEncoder.encode(title, "UTF-8") + 
							"&bot" +
							"&title=" + URLEncoder.encode(title, "UTF-8") + 
							"&token=" + URLEncoder.encode(editToken, "UTF-8") +
							"");
					this.out.write("&format=xml");
					this.out.write("&text=");
				}
				
				private void checkError() throws IOException {
					if (this.buffer.length() < "ERROR:".length())
						return;
					String buffer = this.buffer.toString().toString().trim();
					if (buffer.startsWith("ERROR:"))
						throw new DocumentUnfitForExportException(buffer.substring("ERROR:".length()).trim());
				}
				
				private StringBuffer buffer = new StringBuffer();
				
				public void close() throws IOException {
					this.checkConnected();
					
					this.out.flush();
					this.out.close();
					logInfo("Update writer closed");
					
					final boolean[] wikiSessionOK = {true};
					parser.stream(new InputStreamReader(con.getInputStream(), "UTF-8"), new TokenReceiver() {
						public void close() throws IOException {}
						public void storeToken(String token, int treeDepth) throws IOException {
							logDebug(" -> " + token.trim());
							if (grammar.isTag(token)) {
								String type = grammar.getType(token);
								if ("edit".equals(type)) {
									TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
									String result = tnas.getAttribute("result");
									if ("Success".equals(result))
										logDebug("Update Successful");
									else logWarning("Update Error: " + result);
								}
								else if ("error".equals(type)) {
									TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
									String code = tnas.getAttribute("code");
									if ("badtoken".equals(code)) {
										logout();
										wikiSessionOK[0] = false;
									}
								}
							}
						}
					});
					
					if (!wikiSessionOK[0])
						throw new InvalidWikiSessionException();
				}
				public void flush() throws IOException {
					this.flushBuffer();
					this.out.flush();
				}
				public void write(char[] cbuf, int off, int len) throws IOException {
					this.buffer.append(cbuf, off, len);
					if (this.buffer.length() > 1024)
						this.flushBuffer();
				}
				
				private void flushBuffer() throws IOException {
					this.checkConnected();
					
					String buffer = this.buffer.toString();
					buffer = buffer.replaceAll("\\x20\\,", ",");
					buffer = buffer.replaceAll("\\x20\\.", ".");
					buffer = buffer.replaceAll("\\x20\\!", "!");
					buffer = buffer.replaceAll("\\x20\\)", ")");
					buffer = buffer.replaceAll("\\(\\x20", "(");
					this.out.write(URLEncoder.encode(buffer, "UTF-8"));
					this.buffer = new StringBuffer();
				}
			};
		}
		
		boolean checkExists(String title) throws IOException {
			HttpURLConnection con = this.getConnection();
			con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			this.sendQuery(con, "action=query" +
					"&prop=info" +
					"&titles=" + URLEncoder.encode(title, "UTF-8") + 
				"");
			final boolean[] exists = {false};
			parser.stream(new InputStreamReader(con.getInputStream(), "UTF-8"), new TokenReceiver() {
				public void close() throws IOException {}
				public void storeToken(String token, int treeDepth) throws IOException {
					if (grammar.isTag(token) && "page".equals(grammar.getType(token)) && !grammar.isEndTag(token)) {
						logDebug(token);
						TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
						exists[0] = (!"0".equals(tnas.getAttribute("lastrevid", "0")));
					}
				}
			});
			return exists[0];
		}
		
		String getUpdateUser(String title) throws IOException {
			HttpURLConnection con = this.getConnection();
			con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			this.sendQuery(con, "action=query" +
					"&prop=revisions" +
					"&titles=" + URLEncoder.encode(title, "UTF-8") + 
				"");
			final String[] updateUser = {null};
			parser.stream(new InputStreamReader(con.getInputStream(), "UTF-8"), new TokenReceiver() {
				private String timestamp = "";
				public void close() throws IOException {}
				public void storeToken(String token, int treeDepth) throws IOException {
//					System.out.println(token);
					if (grammar.isTag(token) && "rev".equals(grammar.getType(token)) && !grammar.isEndTag(token)) {
						logDebug(token);
						TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
						String timestamp = tnas.getAttribute("timestamp");
						if (timestamp.compareTo(this.timestamp) > 0) {
							this.timestamp = timestamp;
							updateUser[0] = tnas.getAttribute("user");
						}
					}
				}
			});
			return updateUser[0];
		}
		
		void logout() {
			try {
				HttpURLConnection con = this.getConnection();
				this.sendQuery(con, "action=logout");
				parser.stream(new InputStreamReader(con.getInputStream(), "UTF-8"), new TokenReceiver() {
					public void close() throws IOException {}
					public void storeToken(String token, int treeDepth) throws IOException {
						logDebug(token);
					}
				});
			}
			catch (IOException ioe) {
				logError(ioe);
			}
			
			this.lguserid = null;
			this.lgusername = null;
			this.lgtoken = null;
			this.sessionid = null;
			this.cookieprefix = null;
			this.editToken = null;
		}
		
		private HttpURLConnection getConnection() throws IOException {
			logDebug("Connecting to " + this.url.toString());
			HttpURLConnection con = ((HttpURLConnection) this.url.openConnection());
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			con.setUseCaches (false);
			if (this.cookieprefix != null) {
				con.setRequestProperty("Cookie", (
						(this.cookieprefix + "UserName=") + URLEncoder.encode(this.lgusername, "UTF-8") + 
						((this.lguserid == null) ? "" : ("; " + (this.cookieprefix + "UserID=") + URLEncoder.encode(this.lguserid, "UTF-8"))) +
						"; " + (this.cookieprefix + "Token=") + URLEncoder.encode(this.lgtoken, "UTF-8") +
						"; " + (this.cookieprefix + "_session=") + URLEncoder.encode(this.sessionid, "UTF-8")
					));
				logDebug("Cookie set: " + 
						(this.cookieprefix + "UserName=") + URLEncoder.encode(this.lgusername, "UTF-8") + 
						((this.lguserid == null) ? "" : ("; " + (this.cookieprefix + "UserID=") + URLEncoder.encode(this.lguserid, "UTF-8"))) +
						"; " + (this.cookieprefix + "Token=") + URLEncoder.encode(this.lgtoken, "UTF-8") +
						"; " + (this.cookieprefix + "_session=") + URLEncoder.encode(this.sessionid, "UTF-8")
					);
			}
			return con;
		}
		
		private void sendQuery(HttpURLConnection con, String query) throws IOException {
			logDebug("Sending query: " + query + "&format=xml");
			DataOutputStream out = new DataOutputStream(con.getOutputStream());
			out.writeBytes(query);
			out.writeBytes("&format=xml");
			out.flush();
			out.close();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doUpdate(de.uka.ipd.idaho.gamta.QueriableAnnotation, java.util.Properties)
	 */
	protected void doUpdate(QueriableAnnotation doc, Properties docAttributes) throws IOException {
		
		//	no export destination, save effort for document preparation
		if (this.wikis.isEmpty())
			return;
		
		//	we cannot export this one yet
		if (!AnnotationUtils.isWellFormedNesting(doc)) {
			this.logInfo("  - document not yet well-formed");
			return;
		}
		
		//	get title
		String title = getTitle(doc);
		if (title == null) {
			this.logInfo("  - document filtered out on title");
			return;
		}
		this.logDebug("  - got document title: " + title);
		
		//	take specific pre-upload measures
		doc = prepareDocument(doc);
		if (doc == null) {
			this.logInfo("  - document filtered out on preparation");
			return;
		}
		this.logDebug("  - document prepared");
		
		//	generate document XML only once
		String docXmlLine = null;
		
		//	forward updates to individual Wikis
		for (Iterator wit = this.wikis.keySet().iterator(); wit.hasNext();) {
			final String wikiName = ((String) wit.next());
			Wiki wiki = ((Wiki) this.wikis.get(wikiName));
			this.logDebug("  - forwarding update to " + wikiName);
			
			//	check transformer
			if (wiki.xslt == null) {
				this.logInfo("  - XSLT not available for " + wikiName);
				continue;
			}
			
			try {
				
				//	get connection
				WikiConnection wikiCon = wiki.getWikiConnection();
				if (wikiCon == null) {
					this.logInfo("  - no connection to " + wikiName);
					continue;
				}
				
				//	check if Wiki article was updated by other user
				if (!wiki.overwriteUserUpdates) {
					String updateUser = wikiCon.getUpdateUser(title);
					if ((updateUser != null) && !wikiCon.lgusername.equals(updateUser)) {
						this.logInfo("  - cannot update document due to user edits");
						continue;
					}
				}
				
				//	check if Wiki article exists
				if (wiki.createOnly && wikiCon.checkExists(title)) {
					this.logInfo("  - already exists");
					continue;
				}
				
				//	generate XML line if not done yet
				if (docXmlLine == null)
					docXmlLine = getXmlLine(doc);
				
				//	do update
				this.doUpdate(wikiName, wiki, wikiCon, title, docXmlLine, false);
			}
			catch (IOException ioe) {
				this.logError("  - error forwarding update to " + wikiName + ": " + ioe.getMessage());
				this.logError(ioe);
			}
		}
	}
	
	private void doUpdate(final String wikiName, Wiki wiki, WikiConnection wikiCon, String title, String docXmlLine, boolean isSessionTimeoutRecursion) {
		
		//	catch Wiki-specific IO exceptions locally so they do not disturb upload to other Wikis
		try {
			
			//	transform data
			Writer updateWriter = wikiCon.getUpdateWriter(title);
			
			//	divert XSLT output through filter if links to de-duplicate
			if (wiki.deduplicateLinks) {
				final BufferedWriter wikiWriter = new BufferedWriter(updateWriter);
				final PipedReader pr = new PipedReader();
				final IOException[] exception = {null};
				final Thread linkDeduplicator = new Thread() {
					HashSet linked = new HashSet();
					String unescaper = null;
					public void run() {
						synchronized(this) {
							this.notify();
						}
						
						BufferedReader br = new BufferedReader(pr);
						String line;
						try {
							while ((line = br.readLine()) != null) {
								logDebug(line);
								line = this.removeDuplicateLinks(line);
								logDebug("  ==> " + line);
								
								wikiWriter.write(line);
								wikiWriter.newLine();
							}
							
							wikiWriter.flush();
							wikiWriter.close();
						}
						catch (IOException ioe) {
							logError("  - error forwarding update to " + wikiName + ": " + ioe.getMessage());
							synchronized (exception) {
								exception[0] = ioe;
							}
						}
					}
					
					private String removeDuplicateLinks(String raw) {
						if (raw.length() == 0)
							return raw;
						
						StringBuffer clean = new StringBuffer();
						
						//	nothing escaped
						if (this.unescaper == null) {
							
							//	find start of escaped part
							int escapedStart = raw.length();
							String escaper = null;
							for (Iterator eit = wikiEscapers.keySet().iterator(); eit.hasNext();) {
								String escaperString = ((String) eit.next());
								int escaperIndex = raw.indexOf(escaperString);
								if (escaperIndex == -1)
									continue;
								else if (escaperIndex < escapedStart) {
									escapedStart = escaperIndex;
									escaper = escaperString;
									this.unescaper = wikiEscapers.getProperty(escaperString);
								}
							}
							
							//	no escaper found, check whole line
							if (escapedStart == raw.length())
								return this.checkLinks(raw);
							
							//	found start of escaped part
							else {
								logDebug("  --> found escaper " + escaper + ", waiting for " + this.unescaper);
								
								//	check links in non-escaped part
								clean.append(this.checkLinks(raw.substring(0, escapedStart)));
								
								//	recurse with escaped part (un-escaper might be in the same line)
								clean.append(this.removeDuplicateLinks(raw.substring(escapedStart)));
							}
						}
						
						//	in escaped part
						else {
							
							//	find end of escaped part
							int unescapedStart = (raw.indexOf(this.unescaper) + this.unescaper.length());
							
							//	whole line is escaped
							if (unescapedStart < this.unescaper.length())
								return raw;
							
							logDebug("  --> found unescaper " + this.unescaper);
							
							//	clear escaper
							this.unescaper = null;
							
							//	keep escaped part as is
							clean.append(raw.substring(0, unescapedStart));
							
							//	recurse with non-escaped part (new escaped part might start in same line)
							clean.append(this.removeDuplicateLinks(raw.substring(unescapedStart)));
						}
						
						//	finally ...
						return clean.toString();
					}
					
					private String checkLinks(String raw) {
						
						//	get start of first link
						int linkStart = raw.indexOf("[[");
						if (linkStart == -1)
							return raw;
						
						//	get end of first link
						int linkEnd = raw.indexOf("]]", (linkStart + "[[".length()));
						if (linkStart == -1)
							return raw;
						
						//	parse link
						String link = raw.substring((linkStart + "[[".length()), linkEnd);
						String linkLabel;
						String linkTarget;
						if (link.indexOf('|') == -1) {
							linkLabel = link;
							linkTarget = link;
						}
						else {
							linkLabel = link.substring(link.indexOf('|') + 1);
							linkTarget = link.substring(0, link.indexOf('|'));
						}
						
						StringBuffer clean = new StringBuffer();
						
						//	new link, keep line up to end of link
						if (this.linked.add(linkTarget)) {
							logDebug("  --> found new link to " + linkTarget);
							clean.append(raw.substring(0, (linkEnd + "]]".length())));
						}
						
						//	duplicate link
						else {
							logDebug("  --> removed duplicate link to " + linkTarget);
							
							//	keep start of line
							clean.append(raw.substring(0, linkStart));
							
							//	add link label without actual link
							clean.append(linkLabel);
						}
						
						//	recurse with remainder of line (there might be more links)
						clean.append(this.checkLinks(raw.substring(linkEnd + "]]".length())));
						
						//	finally ...
						return clean.toString();
					}
				};
				
				//	pipe de-duplicated output into update writer
				final PipedWriter pw = new PipedWriter(pr);
				updateWriter = new Writer() {
					public void close() throws IOException {
						pw.close(); // do it
						try {
							linkDeduplicator.join();
						} catch (InterruptedException ie) {}
						this.checkException(); // check for exception from other thread
					}
					public void flush() throws IOException {
						pw.flush(); // do it
						this.checkException(); // check for exception from other thread
					}
					public void write(char[] cbuf, int off, int len) throws IOException {
						pw.write(cbuf, off, len); // do it
						this.checkException(); // check for exception from other thread
					}
					private void checkException() throws IOException {
						synchronized (exception) {
							if (exception[0] != null)
								throw exception[0];
						}
					}
				};
				
				//	wait for link de-duplicator
				synchronized(linkDeduplicator) {
					linkDeduplicator.start();
					try {
						linkDeduplicator.wait();
					} catch (InterruptedException ie) {}
				}
			}
			
			//	wrap update writer
			updateWriter = new PrologFilterWriter(updateWriter);
			
			//	transform document into Wiki syntax via XSLT
			try {
				wiki.xslt.transform(new StreamSource(new StringReader(docXmlLine)), new StreamResult(updateWriter));
			}
			catch (TransformerException te) {
				this.logWarning("  - XSLT error for " + wikiName + ": " + te.getMessageAndLocation());
			}
			finally {
				updateWriter.flush();
				updateWriter.close();
			}
			this.logInfo("  - update forwarded to " + wikiName);
		}
		
		//	error message generated by XSLT to indicate document unsuited for export
		catch (DocumentUnfitForExportException dufee) {
			this.logInfo("  - the update cannot be forwarded to " + wikiName + ": " + dufee.getMessage());
		}
		
		//	error message thrown by Wiki API, indicating timed-out session or invalid credentials
		catch (InvalidWikiSessionException iwse) {
			if (isSessionTimeoutRecursion)
				this.logInfo("  - the update cannot be forwarded to " + wikiName + " due to invalid credentials");
			else this.doUpdate(wikiName, wiki, wikiCon, title, docXmlLine, true);
		}
		
		//	other IO error
		catch (IOException ioe) {
			this.logError("  - error forwarding update to " + wikiName + ": " + ioe.getMessage());
			this.logError(ioe);
		}
	}
	
	private static class DocumentUnfitForExportException extends IOException {
		DocumentUnfitForExportException(String message) {
			super(message);
		}
	}
	
	private static class InvalidWikiSessionException extends IOException {
		InvalidWikiSessionException() {
			super();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP#doDelete(java.lang.String, java.util.Properties)
	 */
	protected void doDelete(String docId, Properties docAttributes) throws IOException {
		// we're not forwarding deletions for now
	}
	
	/**
	 * Prepare the document in a sub class specific fashion. This default
	 * implementation simply returns the argument document, sub classes are
	 * welcome to overwrite it as needed. The method is not void so sub classes
	 * can create a new document from the argument one if required.
	 * @param document the document to prepare
	 * @return the prepared document
	 * @throws IOException
	 */
	protected QueriableAnnotation prepareDocument(QueriableAnnotation document) throws IOException {
		return document;
	}
	
	/**
	 * Create a title for a given document in a sub class specific fashion. In
	 * particular, this method is responsible for creating the title of the Wiki
	 * page the document is converted to. By returning null as the title for a
	 * given document, sub classes can indicate that it should not be uploaded.
	 * @param document the document to create a title for
	 * @return a title for the specified document
	 */
	protected abstract String getTitle(QueriableAnnotation document);
	
	private static final boolean DEBUG_EXPORT = false;
	private static final boolean DEBUG_LINK_FILTER = (DEBUG_EXPORT && false);
	
	private static Properties wikiEscapers = new Properties();
	static {
		wikiEscapers.setProperty("<nowiki>", "</nowiki>");
		wikiEscapers.setProperty("<nowiki ", "</nowiki>");
		
		wikiEscapers.setProperty("<source>", "</source>");
		wikiEscapers.setProperty("<source ", "</source>");
		
		wikiEscapers.setProperty("<pre>", "</pre>");
		wikiEscapers.setProperty("<pre ", "</pre>");
		
		wikiEscapers.setProperty("<math>", "</math>");
		wikiEscapers.setProperty("<math ", "</math>");
		
		wikiEscapers.setProperty("<!--", "-->");
		
		wikiEscapers.setProperty("{{", "}}");
	}
	
	private static class PrologFilterWriter extends Writer {
		private Writer writer;
		private boolean inContent = true;
		private int written = 0;
		
		PrologFilterWriter(Writer updateWriter) {
			this.writer = updateWriter;
		}
		
		public void close() throws IOException {
			this.writer.close();
		}
		
		public void flush() throws IOException {
			this.writer.flush();
		}
		
		public void write(char[] cbuf, int off, int len) throws IOException {
			if (this.written == 0) {
				while ((len > 0) && (cbuf[off] <= ' ')) {
					off++;
					len--;
				}
				if (len == 0)
					return;
				
				if ((cbuf[off] == '<') || !this.inContent) {
					this.inContent = false;
					while ((len > 0) && (cbuf[off] != '>')) {
						off++;
						len--;
					}
					
					if (len == 0)
						return;
					
					else {
						this.inContent = true;
						off++;
						len--;
						
						while ((len > 0) && (cbuf[off] <= ' ')) {
							off++;
							len--;
						}
						if (len == 0)
							return;
					}
				}
			}
			
			if (this.inContent) {
				this.writer.write(cbuf, off, len);
				this.written += len;
			}
		}
	}
	
	private static String getXmlLine(QueriableAnnotation doc) {
		StringWriter buf = new StringWriter();
		
		//	get annotations
		Annotation[] nestedAnnotations = doc.getAnnotations();
		
		//	make sure there is a root element
		if ((nestedAnnotations.length == 0) || (nestedAnnotations[0].size() < doc.size())) {
			Annotation[] newNestedAnnotations = new Annotation[nestedAnnotations.length + 1];
			newNestedAnnotations[0] = doc;
			System.arraycopy(nestedAnnotations, 0, newNestedAnnotations, 1, nestedAnnotations.length);
			nestedAnnotations = newNestedAnnotations;
		}
		
		Stack stack = new Stack();
		int annotationPointer = 0;
		
		Token token = null;
		Token lastToken;
		
		for (int t = 0; t < doc.size(); t++) {
			
			//	switch to next Token
			lastToken = token;
			token = doc.tokenAt(t);
			
			//	write end tags for Annotations ending before current Token
			while ((stack.size() > 0) && (((Annotation) stack.peek()).getEndIndex() <= t)) {
				Annotation annotation = ((Annotation) stack.pop());
				buf.write(AnnotationUtils.produceEndTag(annotation));
			}
			
			//	skip space character before unspaced punctuation (e.g. ','), after line breaks and tags, and if there is no whitespace in the token sequence
			if ((lastToken != null) && Gamta.insertSpace(lastToken, token) && (t != 0) && (doc.getWhitespaceAfter(t-1).length() != 0))
				buf.write(" ");
			
			//	write start tags for Annotations beginning at current Token
			while ((annotationPointer < nestedAnnotations.length) && (nestedAnnotations[annotationPointer].getStartIndex() == t)) {
				Annotation annotation = nestedAnnotations[annotationPointer];
				stack.push(annotation);
				annotationPointer++;
				buf.write(AnnotationUtils.produceStartTag(annotation, null, true));
			}
			
			//	append current Token
			buf.write(AnnotationUtils.escapeForXml(token.getValue()));
		}
		
		//	write end tags for Annotations not closed so far
		while (stack.size() > 0) {
			Annotation annotation = ((Annotation) stack.pop());
			buf.write(AnnotationUtils.produceEndTag(annotation));
		}
		
		//	finally ...
		return buf.toString();
	}
	
////	static String testWikiUrl = "http://species-id.net/wiki/api.php";
//	static String testWikiUrl = "http://species-id.net/w/api.php";
//	static String testWikiUser = "PlaziBot";
//	static String testWikiPwd = "plazi-id";
//	
//	public static void main(String[] args) throws Exception {
////		MutableAnnotation data = SgmlDocumentReader.readDocument(new InputStreamReader(new FileInputStream(new File(new File("E:/GoldenGATEv3.Server/Components/GgServerWCSData/"), "TestDocument-ChromisAbyssus.xml")), "UTF-8"));
////		AnnotationReader ar = new AnnotationReader(data);
////		Writer dw = new OutputStreamWriter(new FileOutputStream(new File(new File("E:/GoldenGATEv3.Server/Components/GgServerWCSData/"), "TestDocument-ChromisAbyssus.oneLine.xml")), "UTF-8");
////		char[] cbuf = new char[1024];
////		int read;
////		while ((read = ar.read(cbuf)) != -1)
////			dw.write(cbuf, 0, read);
////		dw.flush();
////		dw.close();
////		if (true) return;
////		
////		String xsltOutput = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n{{italictitle}}\r\n{{Taxobox";
////		Writer xmlFilterWriter = new Writer() {
////			private boolean inProlog = true;
////			private int written = 0;
////			public void close() throws IOException {}
////			public void flush() throws IOException {}
////			public void write(char[] cbuf, int off, int len) throws IOException {
////				if (this.written == 0) {
////					while ((cbuf[off] < 33) && (len > 0)) {
////						off++;
////						len--;
////					}
////					if (len == 0)
////						return;
////					
////					if ((cbuf[off] == '<') || this.inProlog) {
////						this.inProlog = true;
////						while ((cbuf[off] != '>') && (len > 0)) {
////							off++;
////							len--;
////						}
////						
////						if (len == 0)
////							return;
////						
////						else {
////							this.inProlog = false;
////							off++;
////							len--;
////							
////							while ((cbuf[off] < 33) && (len > 0)) {
////								off++;
////								len--;
////							}
////							if (len == 0)
////								return;
////						}
////					}
////				}
////				
////				if (!this.inProlog) {
////					System.out.print(new String(cbuf, off, len));
////					this.written += len;
////				}
////			}
////		};
////		
////		for (int c = 0; c < xsltOutput.length(); c++)
////			xmlFilterWriter.write(xsltOutput.charAt(c));
////		xmlFilterWriter.write(xsltOutput);
////		if (true) return;
//		
//		System.getProperties().put("proxySet", "true");
//		System.getProperties().put("proxyHost", "proxy.rz.uni-karlsruhe.de");
//		System.getProperties().put("proxyPort", "3128");
//		
//		WikiConnection wc = new WikiConnection(testWikiUrl);
//		if (wc.login(testWikiUser, testWikiPwd)) {
//			String title = "Test Page 2";
//			
//			System.out.println(wc.getUpdateUser(title));
//			
////			System.out.println(wc.checkExists(title));
//			
////			String text = "It works ... so far :-)\n\n==Update Test==\n\nAlso as an update :-)\n\n==Component Test==\n\nAnd also from within WCS :-)\n\n==Stream Test==\n\nAnd with streaming :-)";
////			Writer w = wc.getUpdateWriter(title);
////			w.write(text);
////			w.flush();
////			w.close();
//		}
//		wc.logout();
//	}
}
