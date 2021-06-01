<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:mods="http://www.loc.gov/mods/v3">
	<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
	
	<xsl:template match="/document">
		
		<table class="mainTable" width="100%">
			<tr>
				<td class="mainTableHeader">
					<xsl:value-of select=".//article//title"/>
				</td>
			</tr>
			<tr>
				<td class="mainTableBody">
					<table class="documentTable">
						<tr>
							<td class="documentTableHeader" colspan="2">Publication Data, Additional Information (external links, etc)</td>
						</tr>
						<tr>
							<td class="documentDataLabel">citation of original article</td>
							<td class="documentData">
								<xsl:value-of select="./@docAuthor"/>, <xsl:value-of select="./@docDate"/>, <xsl:value-of select="./@masterDocTitle"/>, <xsl:value-of select="./@docOrigin"/>, pp. <xsl:value-of select="./@masterPageNumber"/>-<xsl:value-of select="./@masterLastPageNumber"/>: <xsl:value-of select="./@pageNumber"/><xsl:if test="not(./@pageNumber = ./@lastPageNumber)">-<xsl:value-of select="./@lastPageNumber"/></xsl:if>
							</td>
						</tr>
						<tr>
							<td class="documentDataLabel">publication ID</td>
							<td class="documentData"><xsl:value-of select="./@extId"/></td>
						</tr>
						<tr>
							<td class="documentDataLabel">link to original citation</td>
							<td class="documentData"><a target="_blank"><xsl:attribute name="href"><xsl:value-of select="./@docSource"/></xsl:attribute><xsl:value-of select="./@docSource"/></a><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
						</tr>
						<tr>
							<td class="documentDataLabel">additional text versions</td>
							<td class="documentData">
								<xsl:choose>
									<xsl:when test="./externalLink[./@category = 'xmlDocument']">
										<xsl:for-each select="./externalLink[./@category = 'xmlDocument']">
											<xsl:apply-templates select="."/>
											<xsl:if test="./following-sibling::externalLink[./@category = 'xmlDocument']">
												<xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;<xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;
											</xsl:if>
										</xsl:for-each>
									</xsl:when>
									<xsl:otherwise><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</xsl:otherwise>
								</xsl:choose>
							</td>
						</tr>
						<tr>
							<td class="documentDataLabel">additional page, figures</td>
							<td class="documentData"><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</td>
						</tr>
						<tr>
							<td class="documentDataLabel">external databases</td>
							<td class="documentData">
								<xsl:choose>
									<xsl:when test="./externalLink[./@category = 'externalInformation']">
										<xsl:for-each select="./externalLink[./@category = 'externalInformation']">
											<xsl:apply-templates select="."/>
											<xsl:if test="./following-sibling::externalLink[./@category = 'externalInformation']">
												<xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;<xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;
											</xsl:if>
										</xsl:for-each>
									</xsl:when>
									<xsl:otherwise><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</xsl:otherwise>
								</xsl:choose>
							</td>
						</tr>
						<tr>
							<td class="documentDataLabel">map</td>
							<td class="documentData">
								<xsl:choose>
									<xsl:when test="./externalLink[./@category = 'visualization']">
										<xsl:for-each select="./externalLink[./@category = 'visualization']">
											<xsl:apply-templates select="."/>
											<xsl:if test="./following-sibling::externalLink[./@category = 'visualization']">
												<xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;<xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;
											</xsl:if>
										</xsl:for-each>
									</xsl:when>
									<xsl:otherwise><xsl:text disable-output-escaping="yes">&amp;</xsl:text>nbsp;</xsl:otherwise>
								</xsl:choose>
							</td>
						</tr>
					</table>
					<table class="documentTable">
						<tr>
							<td class="documentTableHeader">Article</td>
						</tr>
						<tr>
							<td class="documentTableBody">
								<xsl:apply-templates select="./article//paragraph"/>
							</td>
						</tr>
					</table>
				</td>
			</tr>
		</table>
		
	</xsl:template>
	
	<xsl:template match="paragraph[./parent::caption]">
		<p class="documentText">
			<xsl:apply-templates/>
			<xsl:if test="./parent::caption/externalLink">
				<span class="externalLinkInLine">
					<xsl:apply-templates select="./parent::caption/externalLink"/>
				</span>
			</xsl:if>
		</p>
	</xsl:template>
	
	<xsl:template match="paragraph[./caption]">
		<p class="documentText">
			<xsl:apply-templates/>
			<xsl:if test="./caption/externalLink"><a>
				<xsl:element name="span">
					<xsl:attribute name="class">externalLinkInLine"</xsl:attribute>
					<xsl:attribute name="title"><xsl:value-of select="./@title"/></xsl:attribute>
					<xsl:attribute name="target">_blank</xsl:attribute>
					<xsl:choose>
						<xsl:when test="./caption/externalLink/@href">
							<xsl:attribute name="href"><xsl:value-of select="./caption/externalLink/@href"/></xsl:attribute>
							<xsl:attribute name="target">_blank</xsl:attribute>
						</xsl:when>
						<xsl:otherwise>
							<xsl:attribute name="onclick"><xsl:value-of select="./caption/externalLink/@onclick"/></xsl:attribute>
						</xsl:otherwise>
					</xsl:choose>
					<xsl:choose>
						<xsl:when test="./caption/externalLink/@iconImage">
							<img>
								<xsl:attribute name="alt"><xsl:value-of select="./caption/externalLink/@lable"/></xsl:attribute>
								<xsl:attribute name="src"><xsl:value-of select="./caption/externalLink/@iconImage"/></xsl:attribute>
							</img>
						</xsl:when>
						<xsl:otherwise>
							<xsl:value-of select="./caption/externalLink/@label"/>
						</xsl:otherwise>
					</xsl:choose>
				</xsl:element></a>
			</xsl:if>
		</p>
	</xsl:template>
	
	<xsl:template match="caption/externalLink"/>
	
	<xsl:template match="paragraph">
		<p class="documentText">
			<xsl:apply-templates/>
		</p>
	</xsl:template>
	
	<!--xsl:template match="paragraph[.//table]"/-->
	
	<xsl:template match="paragraph/table">
		<table class="documentTextTable">
			<xsl:for-each select=".//tr">
				<tr class="documentTextTableRow">
					<!--xsl:for-each select=".//td">
						<td class="documentTextTableCell">
							<xsl:if test="./@colspan">
								<xsl:attribute name="colspan"><xsl:value-of select="./@colspan"/></xsl:attribute>
							</xsl:if>
							<xsl:if test="./@rowspan">
								<xsl:attribute name="rowspan"><xsl:value-of select="./@rowspan"/></xsl:attribute>
							</xsl:if>
							<xsl:choose>
								<xsl:when test="./@isEmpty"><xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text></xsl:when>
								<xsl:when test="./emptyCellSpacer"><xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text></xsl:when>
								<xsl:otherwise><xsl:apply-templates/></xsl:otherwise>
							</xsl:choose>
						</td>
					</xsl:for-each-->
					<xsl:for-each select="./*">
						<xsl:choose>
							<xsl:when test="name(.) = 'td'">
								<xsl:element name="td">
									<xsl:attribute name="class">documentTextTableCell</xsl:attribute>
									<xsl:if test="./@colspan"><xsl:attribute name="colspan"><xsl:value-of select="./@colspan"/></xsl:attribute></xsl:if>
									<xsl:if test="./@rowspan"><xsl:attribute name="rowspan"><xsl:value-of select="./@rowspan"/></xsl:attribute></xsl:if>
									<xsl:choose>
										<xsl:when test="./@isEmpty"><xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text></xsl:when>
										<xsl:otherwise><xsl:apply-templates/></xsl:otherwise>
									</xsl:choose>
								</xsl:element>
							</xsl:when>
							<xsl:when test="name(.) = 'th'">
								<xsl:element name="th">
									<xsl:attribute name="class">documentTextTableHead</xsl:attribute>
									<xsl:if test="./@colspan"><xsl:attribute name="colspan"><xsl:value-of select="./@colspan"/></xsl:attribute></xsl:if>
									<xsl:if test="./@rowspan"><xsl:attribute name="rowspan"><xsl:value-of select="./@rowspan"/></xsl:attribute></xsl:if>
									<xsl:choose>
										<xsl:when test="./@isEmpty"><xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text></xsl:when>
										<xsl:otherwise><xsl:apply-templates/></xsl:otherwise>
									</xsl:choose>
								</xsl:element>
							</xsl:when>
							<xsl:otherwise><xsl:copy-of select="."/></xsl:otherwise>
						</xsl:choose>
					</xsl:for-each>
				</tr>
			</xsl:for-each>
		</table>
	</xsl:template>
	
	<xsl:template match="location">
		<xsl:choose>
			<xsl:when test="./@_query_"><a>
					<xsl:attribute name="title"><xsl:value-of select="./@_title_"/></xsl:attribute>
					<xsl:attribute name="href">./search?<xsl:value-of select="./@_query_"/></xsl:attribute>
					<xsl:value-of select="."/>
			</a></xsl:when>
			<xsl:otherwise><xsl:value-of select="."/></xsl:otherwise>
		</xsl:choose>
		<xsl:if test="./externalLink"><span class="externalLinkInLine">
				<xsl:apply-templates select="./externalLink"/>
		</span></xsl:if>
	</xsl:template>
	
	<xsl:template match="normalizedToken"><xsl:value-of select="./@originalValue"/></xsl:template>
	
	<xsl:template match="text()"><xsl:value-of select="."/></xsl:template>
	
	<xsl:template match="externalLink"><a>
		<xsl:attribute name="title"><xsl:value-of select="./@title"/></xsl:attribute>
		<xsl:attribute name="target">_blank</xsl:attribute>
		<xsl:choose>
			<xsl:when test="./@href">
				<xsl:attribute name="href"><xsl:value-of select="./@href"/></xsl:attribute>
				<xsl:attribute name="target">_blank</xsl:attribute>
			</xsl:when>
			<xsl:otherwise>
				<xsl:attribute name="onclick"><xsl:value-of select="./@onclick"/></xsl:attribute>
			</xsl:otherwise>
		</xsl:choose>
		<xsl:choose>
			<xsl:when test="./@iconImage">
				<img>
					<xsl:attribute name="alt"><xsl:value-of select="./@lable"/></xsl:attribute>
					<xsl:attribute name="src"><xsl:value-of select="./@iconImage"/></xsl:attribute>
				</img>
			</xsl:when>
			<xsl:otherwise>
				<xsl:value-of select="./@label"/>
			</xsl:otherwise>
		</xsl:choose>
	</a></xsl:template>
	
</xsl:stylesheet>
