<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
	
	<xsl:template match="/results">
		<table class="mainTable" width="100%">
			<tr>
				<td class="mainTableHeader"><xsl:value-of select="./@_indexLabel"/> Lookup Result</td>
			</tr>
			<xsl:choose>
				<xsl:when test="./result">
					<tr>
						<td class="mainTableBody">
							<div align="center">
								<xsl:apply-templates select="."/>
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
	
	<xsl:template match="results[./result and ./@_indexName = 'exampleIndex']">
		<table class="resultIndexTable">
			<tr>
				<td class="resultIndexTableHeader">Example Field 1</td>
				<td class="resultIndexTableHeader">Example Field 2</td>
			</tr>
			<xsl:for-each select="./result">
				<tr>
					<td class="resultIndexTableBody"><xsl:value-of select="./@exampleAttribite1"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
					<td class="resultIndexTableBody"><xsl:value-of select="./@exampleAttribute2"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
				</tr>
			</xsl:for-each>
		</table>
	</xsl:template>
	
	<xsl:template match="results[./result and ./@_indexName = 'location']">
		<table class="thesaurusTable">
			<tr>
				<td class="thesaurusTableHeader">Name</td>
				<td class="thesaurusTableHeader">Country</td>
				<td class="thesaurusTableHeader">Longitude</td>
				<td class="thesaurusTableHeader">Latitude</td>
				<td class="thesaurusTableHeader">Elevation</td>
			</tr>
			<xsl:for-each select="./result">
				<tr>
					<td class="thesaurusTableBody"><xsl:value-of select="./@name"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
					<td class="thesaurusTableBody"><xsl:value-of select="./@country"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
					<td class="thesaurusTableBody"><xsl:value-of select="./@longitude"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
					<td class="thesaurusTableBody"><xsl:value-of select="./@latitude"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
					<td class="thesaurusTableBody"><xsl:value-of select="./@elevation"/><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
				</tr>
			</xsl:for-each>
		</table>
	</xsl:template>
</xsl:stylesheet>