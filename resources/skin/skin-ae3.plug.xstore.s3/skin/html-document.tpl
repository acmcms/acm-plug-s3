<%
	// Slightly augments the response and passes it to 'html-page' template of 
	// 'skin-standard-html' skin.

%><%RETURN: content.s3html ? content :
	Layouts.extend( content, {
		title		: "XSTORE.S3" + (
			content.title 
				? ", " + content.title 
				: ""
		),
		template	: 'html-document',
		s3html		: true
	})
%>