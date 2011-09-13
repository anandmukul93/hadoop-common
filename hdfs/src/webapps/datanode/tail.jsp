<%
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file 
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
%>
<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.hdfs.server.common.JspHelper"
  import="org.apache.hadoop.util.ServletUtil"
  import="org.apache.hadoop.conf.Configuration"
%>
<%!
  //for java.io.Serializable
  private static final long serialVersionUID = 1L;
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html>
<head>
<%JspHelper.createTitle(out, request, request.getParameter("filename")); %>
<script type="text/javascript" src="/jquery.fancybox-1.3.4/jquery-1.4.3.min.js"></script>
<script type="text/javascript" src="/jquery.fancybox-1.3.4/fancybox/jquery.fancybox-1.3.4.pack.js"></script>
<link rel="stylesheet" href="/jquery.fancybox-1.3.4/fancybox/jquery.fancybox-1.3.4.css" type="text/css" media="screen"/>
<script type="text/javascript">
jQuery(document).ready(function() {
$(".prov").fancybox({
'width'				: '80%',
'height'			: '80%',
'autoScale'     	: false,
'transitionIn'		: 'none',
'transitionOut'		: 'none',
'overlayOpacity'	: 0.25,
'type'				: 'iframe'
});
});
</script>
</head>
<body>
<form action="/tail.jsp" method="GET">
<% 
   Configuration conf = 
     (Configuration) application.getAttribute(JspHelper.CURRENT_CONF);
   DatanodeJspHelper.generateFileChunksForTail(out,request, conf); 
%>
</form>
<hr>

<h2>Local logs</h2>
<a href="/logs/">Log</a> directory

<%
out.println(ServletUtil.htmlFooter());
%>
