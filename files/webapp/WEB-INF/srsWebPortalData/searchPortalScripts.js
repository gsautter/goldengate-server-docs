function initAdjustSubIndexName() {
	if (document.srsSearchForm) {
		document.srsSearchForm.indexName.onchange = function anonymous() {adjustSubIndexName();};
		document.srsSearchForm.subIndexName.onchange = function anonymous() {adjustSubIndexMinSize();};
		adjustSubIndexName();
	}
}


function adjustSubIndexName() {
	var indexName = document.srsSearchForm.indexName.value;
	for (o = 0; o < document.srsSearchForm.indexName.options.length; o++) {
		document.srsSearchForm.subIndexName.options[o].selected = (document.srsSearchForm.indexName.options[o].value == indexName);
	}
	adjustSubIndexMinSize();
}

function adjustSubIndexMinSize() {
	var indexName = document.srsSearchForm.indexName.value;
	var subIndexName = document.srsSearchForm.subIndexName.value;
	
	if (indexName == subIndexName) {
		document.srsSearchForm.minSubResultSize.value = 0;
		document.srsSearchForm.minSubResultSize.disabled = true;
		
		document.getElementByName("brLabelminSubResultSize").innerHTML = '&lt;Not Available&gt;';
	}
	else {
		document.srsSearchForm.minSubResultSize.value = 1;
		document.srsSearchForm.minSubResultSize.disabled = false;
		
		for (o = 0; o < document.srsSearchForm.subIndexName.options.length; o++) {
			if (document.srsSearchForm.subIndexName.options[o].value == subIndexName) {
				document.getElementByName("brLabelminSubResultSize").innerHTML = ('Min ' + document.srsSearchForm.subIndexName.options[o].text);
			}
		}
	}
}