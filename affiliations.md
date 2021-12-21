---
layout: page
title: Affiliations
permalink: /affiliations/
---
<table>
<thead>
<tr>
<th>Name</th>
<th>Count</th>
</tr>
</thead>
<tbody>
{% for name, count in site.data.output.affiliations %}
<tr>
<td>{{ name }}</td>
<td>{{ count }}</td>
</tr>
{% endfor %}
</tbody>
</table>
