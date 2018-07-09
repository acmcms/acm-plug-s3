<html><body><h1>Storage: S3</h1>
<!-- <%FORMAT:'html'%><%
%><%OUTPUT: body %><%
	%><%= '<!' + '-- ' + 'begin' %><%
	%>-->
		<p>3rd generation of RDBMS based object storage.</p>
		<ul>
			<li>
				<h2>What does it give?</h2>
				<ul>
					<li>an implementation of <a href="#">ACM.XSTORE API</a>;</li>
					<li>automatic maintenance, no interaction required;</li>
					<li>'several-clients per database' <a href="#cluster">cluster design</a>.</li>
				</ul>
			</li>
			<li>
				<a name="cluster"></a><h2>Cluster</h2>
				System architecture, queries, maintenance tasks and any other 'mode 
				of operation' left are all designed to work in 'several clients per
				database' cluster.<br />
				<ul>
					There are some things to know:
					<li>system is actually affected by some minor degradation in 
					'single client per database' mode (as well as in process of 
					upgrade/testing when there is only one instance with 'latest' 
					version is running in cluster);</li>
					<li>maintenance, indexing and background tasks are performed 
					by instances with 'latest' version in rotation, one of such 
					instance is holding a lock for a task and performing it for 
					an hour releasing this lock for 5+ minutes then, other instances 
					a periodically trying to acquire the lock for any task it has 
					latest (greatest in cluster) version for;</li>
					<li>storage creates lots of cached files in %private% folder, 
					queries for listings, object properties and fields, blobs, 
					search (both folder-hierarchy and folder-local) are all never
					answered directly from database - everything is cached on disk
					first by means of fixed set of conveyors;</li>
					<li>state of all caches is maintained automatically by S3 plugin,
					as well as database garbage collection;</li>
					<li>some of the types of data (like listings, searches...) are 
					not immutable in time, so there is mechanism of cache-invalidation
					message delivery based on UDP and table in underlying database;</li>
					<li>instance non cleanly removed from system (added once, then 
					reinstalled for example) will still be registered in cluster
					for some time (14 days normally) so cache-invalidation messages
					targeted at this instance will accumulate in database - you 
					should either purge instance registration or just wait till
					registration will be invalidated itself and all of the messages
					purged automatically;</li>
					<li>that's pretty much all up to date.</li>
				</ul>
			</li>
		</ul>
	<!-- <%= 'end. ' + '--' + '>' %><%
%><%/OUTPUT%><%
%><%RETURN: {
		title		: '3rd generation object storage',
		template	: "html-document",
		body		: body
	} %><%
%><%
%><%/FORMAT%> --></body></html>