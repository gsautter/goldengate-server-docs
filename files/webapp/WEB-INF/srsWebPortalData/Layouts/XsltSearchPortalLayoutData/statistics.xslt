<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
	
	<xsl:template match="/results">
		<table class="mainTable" width="100%">
			<tr>
				<td class="mainTableHeader">GoldenGATE SRS Collection Statistics</td>
			</tr>
			<xsl:choose>
				<xsl:when test="./result">
					<tr>
						<td class="mainTableBody">There are <xsl:value-of select="./@MasterCount"/> documents in the collection, <xsl:value-of select="./@DocCount"/> treatments</td>
					</tr>
					<tr>
						<td class="mainTableBody">These are the top 10 contributors to the collection</td>
					</tr>
					<tr>
						<td class="mainTableBody">
							<div align="center">
								<table class="statisticsTable">
									<tr>
										<td class="statisticsTableHeader">Rank</td>
										<td class="statisticsTableHeader">User Name</td>
										<td class="statisticsTableHeader">Documents Contributed</td>
										<td class="statisticsTableHeader">Articles Contributed</td>
									</tr>
									<xsl:apply-templates select="./result"/>
								</table>
							</div>
						</td>
					</tr>
				</xsl:when>
				<xsl:otherwise>
					<tr>
						<td class="searchErrorMessage">There are no documents in the collection so far, sorry.</td>
					</tr>
				</xsl:otherwise>
			</xsl:choose>
		</table>
	</xsl:template>
	
	<xsl:template match="result">
		<tr>
			<td class="statisticsTableBody"><xsl:value-of select="position()"/></td>
			<td class="statisticsTableBody"><xsl:value-of select="./@checkinUser"/></td>
			<td class="statisticsTableBody"><xsl:value-of select="./@MasterCount"/></td>
			<td class="statisticsTableBody"><xsl:value-of select="./@DocCount"/></td>
		</tr>
	</xsl:template>

</xsl:stylesheet>