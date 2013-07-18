<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
	
	<xsl:template match="/results">
		<table class="mainTable" width="100%">
			<tr>
				<td class="mainTableHeader"><xsl:value-of select="./@_indexLabel"/> Search Result: <xsl:value-of select="./@resultSize"/> Records <span class="externalLinkTableHeader"><xsl:apply-templates select="./externalLink"/></span></td>
			</tr>
			<xsl:choose>
				<xsl:when test="./*">
					<tr>
						<td class="mainTableBody">
							<div align="center">
								<table class="indexResultTable">
									<xsl:choose>
									
										<xsl:when test="./@_indexName = 'document'">
											<tr>
												<td class="indexResultTableHeader">Author</td>
												<td class="indexResultTableHeader">Year</td>
												<td class="indexResultTableHeader">Title</td>
												<td class="indexResultTableHeader">Page(s)</td>
												<td class="indexResultTableHeader">PDF</td>
												<td class="indexResultTableHeader">Links</td>
											</tr>
											<xsl:for-each select="./document">
												<tr>
													<td class="indexResultTableBody"><xsl:value-of select="./@docAuthor"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody"><xsl:value-of select="./@docDate"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody"><xsl:value-of select="./@docTitle"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody"><xsl:value-of select="./@pageNumber"/><xsl:if test="not(./@pageNumber = ./@lastPageNumber)">-<xsl:value-of select="./@lastPageNumber"/></xsl:if><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody"><a><xsl:attribute name="href"><xsl:value-of select="./@docSource"/></xsl:attribute><xsl:attribute name="target">_blank</xsl:attribute><xsl:value-of select="./@docSource"/></a><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody">
														<span class="externalLinkTableBody">
															<xsl:apply-templates select="./externalLink"/>
														</span>
													</td>
												</tr>
												<xsl:if test="./subResults">
													<tr>
														<td colspan="6" class="subIndexResultContainer">
															<xsl:for-each select="./subResults">
																<xsl:apply-templates select="."/>
															</xsl:for-each>
														</td>
													</tr>
												</xsl:if>
											</xsl:for-each>
										</xsl:when>
										
										<xsl:when test="./@_indexName = 'exampleIndex'">
											<tr>
												<td class="indexResultTableHeader">Example Field 1</td>
												<td class="indexResultTableHeader">Example Field 2</td>
												<td class="indexResultTableHeader">Links</td>
											</tr>
											<xsl:for-each select="./citation">
												<tr>
													<td class="indexResultTableBody"><xsl:value-of select="./@exampleField1"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody"><xsl:value-of select="./@exampleField2"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody">
														<span class="externalLinkTableBody">
															<xsl:apply-templates select="./externalLink"/>
														</span>
													</td>
												</tr>
												<xsl:if test="./subResults">
													<tr>
														<td colspan="7" class="subIndexResultContainer">
															<xsl:for-each select="./subResults">
																<xsl:apply-templates select="."/>
															</xsl:for-each>
														</td>
													</tr>
												</xsl:if>
											</xsl:for-each>
										</xsl:when>
										
										<xsl:when test="./@_indexName = 'location'">
											<tr>
												<td class="indexResultTableHeader">Name</td>
												<td class="indexResultTableHeader">Country</td>
												<td class="indexResultTableHeader">Longitude</td>
												<td class="indexResultTableHeader">Latitude</td>
												<td class="indexResultTableHeader">Elevation</td>
											</tr>
											<xsl:for-each select="./location">
												<tr>
													<td class="indexResultTableBody"><xsl:value-of select="./@name"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody"><xsl:value-of select="./@country"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody"><xsl:value-of select="./@longitude"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody"><xsl:value-of select="./@latitude"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody"><xsl:value-of select="./@elevation"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
													<td class="indexResultTableBody">
														<span class="externalLinkTableBody">
															<xsl:apply-templates select="./externalLink"/>
														</span>
													</td>
												</tr>
												<xsl:if test="./subResults">
													<tr>
														<td colspan="6" class="subIndexResultContainer">
															<xsl:for-each select="./subResults">
																<xsl:apply-templates select="."/>
															</xsl:for-each>
														</td>
													</tr>
												</xsl:if>
											</xsl:for-each>
										</xsl:when>
										
									</xsl:choose>
								</table>
							</div>
						</td>
					</tr>
				</xsl:when>
				<xsl:otherwise>
					<tr>
						<td class="searchErrorMessage">Your search did not return any results, sorry.</td>
					</tr>
				</xsl:otherwise>
			</xsl:choose>
		</table>
	</xsl:template>
	
	<xsl:template match="subResults[./@_indexName = 'location']">
		<table class="subIndexResultTable">
			<tr>
				<td class="subIndexResultTableHeader">Name</td>
				<td class="subIndexResultTableHeader">Country</td>
				<td class="subIndexResultTableHeader">Longitude</td>
				<td class="subIndexResultTableHeader">Latitude</td>
				<td class="subIndexResultTableHeader">Elevation</td>
			</tr>
			<xsl:apply-templates select="./location"/>
		</table>
	</xsl:template>
	
	<xsl:template match="location">
		<tr>
			<td class="subIndexResultTableBody"><xsl:value-of select="./@name"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
			<td class="subIndexResultTableBody"><xsl:value-of select="./@country"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
			<td class="subIndexResultTableBody"><xsl:value-of select="./@longitude"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
			<td class="subIndexResultTableBody"><xsl:value-of select="./@latitude"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
			<td class="subIndexResultTableBody"><xsl:value-of select="./@elevation"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
		</tr>
	</xsl:template>
	
	<xsl:template match="subResults[./@_indexName = 'exampleIndex']">
		<table class="subIndexResultTable">
			<tr>
				<td class="subIndexResultTableHeader">Example Field 1</td>
				<td class="subIndexResultTableHeader">Example Field 2</td>
			</tr>
			<xsl:apply-templates select="./exampleIndexEntry"/>
		</table>
	</xsl:template>
	
	<xsl:template match="externalLink">
		<a>
			<xsl:attribute name="title"><xsl:value-of select="./@title"/></xsl:attribute>
			<xsl:attribute name="target">_blank</xsl:attribute>
			<xsl:choose>
				<xsl:when test="./@href">
					<xsl:attribute name="href"><xsl:value-of select="./@href"/></xsl:attribute>
				</xsl:when>
				<xsl:otherwise>
					<xsl:attribute name="onclick"><xsl:value-of select="./@onclick"/></xsl:attribute>
				</xsl:otherwise>
			</xsl:choose>
			<xsl:choose>
				<xsl:when test="./@iconUrl">
					<img>
						<xsl:attribute name="alt"><xsl:value-of select="./@label"/></xsl:attribute>
						<xsl:attribute name="src"><xsl:value-of select="./@iconUrl"/></xsl:attribute>
					</img>
				</xsl:when>
				<xsl:otherwise>
					<xsl:value-of select="./@label"/>
				</xsl:otherwise>
			</xsl:choose>
		</a>
	</xsl:template>

</xsl:stylesheet>
