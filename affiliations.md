---
layout: page
title: Affiliations
permalink: /affiliations/
---
<table id="affiliation-table">
<thead>
<tr>
<th>Name</th>
<th>Count</th>
</tr>
</thead>
<tbody>
{% for row in site.data.output.affiliations %}
<tr>
<td>{{ row["affiliation"] }}</td>
<td>{{ row["count"] }}</td>
</tr>
{% endfor %}
</tbody>
</table>

<script>
$(document).ready(function () {
  $("#affiliation-table").DataTable({
    "order": [[ 1, "desc" ], [0, "asc"]]
  });
});
</script>
