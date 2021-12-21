---
layout: page
title: Topics
permalink: /topics/
---
<table id="topic-table">
<thead>
<tr>
<th>Name</th>
<th>Count</th>
</tr>
</thead>
<tbody>
{% for row in site.data.output.topics %}
<tr>
<td>{{ row["topic"] }}</td>
<td>{{ row["count"] }}</td>
</tr>
{% endfor %}
</tbody>
</table>

<script>
$(document).ready(function () {
   $("#topic-table").DataTable();
});
</script>
