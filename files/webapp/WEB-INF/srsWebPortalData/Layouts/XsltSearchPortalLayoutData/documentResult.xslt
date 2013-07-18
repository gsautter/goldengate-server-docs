<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
	
	<xsl:template match="/results">
		<table class="mainTable" width="100%">
			<tr>
				<td class="mainTableHeader">GoldenGATE SRS Search Result: <xsl:value-of select="./@resultSize"/> Articles <xsl:if test="./@moreResultsLink">[<a><xsl:attribute name="href">./search?<xsl:value-of disable-output-escaping="yes" select="./@moreResultsLink"/></xsl:attribute>more</a>] </xsl:if><span class="externalLinkTableHeader"><xsl:apply-templates select="./externalLink"/></span></td>
			</tr>
			<xsl:choose>
				<xsl:when test="./result">
					<tr>
						<td class="mainTableBody">
							<table class="documentResultTable">
								<tr>
									<td class="documentResultTableHeader">Title</td>
									<td class="documentResultTableHeader">Publication</td>
									<td class="documentResultTableHeader">Pages</td>
									<td class="documentResultTableHeader">GoogleMaps</td>
								</tr>
								<xsl:apply-templates select="./result"/>
							</table>
						</td>
					</tr>
				</xsl:when>
				<xsl:otherwise>
					<tr>
						<td class="searchErrorMessage">No article yet in the archive: But you can help to make it accessible!</td>
					</tr>
				</xsl:otherwise>
			</xsl:choose>
		</table>
	</xsl:template>
	
	<xsl:template match="result">
		<tr>
			<td class="documentResultTableBody" width="150">
				<a>
					<xsl:attribute name="href">./html/<xsl:value-of select="./@docId"/></xsl:attribute>
					<xsl:value-of select="./@docTitle"/>
				</a>
			</td>
			<td class="documentResultTableBody"><xsl:value-of select="./@docAuthor"/>, <xsl:value-of select="./@docDate"/>, <xsl:value-of select="./@masterDocTitle"/>, <xsl:value-of select="./@docOrigin"/>, pp. <xsl:value-of select="./@masterPageNumber"/>-<xsl:value-of select="./@masterLastPageNumber"/>: <xsl:value-of select="./@pageNumber"/><xsl:if test="not(./@pageNumber = ./@lastPageNumber)">-<xsl:value-of select="./@lastPageNumber"/></xsl:if>, <a><xsl:attribute name="href"><xsl:value-of select="./@docSource"/></xsl:attribute><xsl:attribute name="target">_blank</xsl:attribute>(download)</a></td>
			<td class="documentResultTableBody" width="50" align="center"><xsl:value-of select="./@pageNumber"/><xsl:if test="not(./@pageNumber = ./@lastPageNumber)">-<xsl:value-of select="./@lastPageNumber"/></xsl:if></td>
			<td class="documentResultTableBody" width="75">
				<span class="externalLinkTableBody">
					<xsl:apply-templates select="./externalLink[./@linkerClassName = 'de.uka.ipd.idaho.goldenGateServer.srs.webPortal.resultLinkers.LocationGoogleMapsLinker']"/>
				</span>
			</td>
		</tr>
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
