function resetFields() {
	for (i = 0; i < document.srsSearchForm.elements.length; i++) {
		if (!document.srsSearchForm.elements[i].type || (document.srsSearchForm.elements[i].type == "text"))
			document.srsSearchForm.elements[i].value = "";
	}
	return false;
}