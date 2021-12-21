---
layout: page
---
<table id="user-table">
<thead>
<tr>
<th>Username</th>
<th>Primary Languages</th>
<th>Secondary Languages</th>
<th>Topics</th>
<th>Affiliations</th>
<th>Total Reviews</th>
</tr>
</thead>
<tbody>
{% for row in site.data.output.full %}
<tr>
<td><a href="https://github.com/{{ row["username"] }}">@{{ row["username"] }}</a></td>
<td>
{% for language in row["languages_primary"] %}
    <span>{{ language }}</span>
{% endfor %}
</td>
<td>
{% for language in row["languages_secondary"] %}
<span>{{ language }}</span>
{% endfor %}
</td>
<td>
{% for topic in row["topics"] %}
<span>{{ topic }}</span>
{% endfor %}
</td>
<td>
{% for affiliation in row["affiliations"] %}
<span>{{ affiliation }}</span>
{% endfor %}
</td>
<td align="right">{{ row["total_reviews"] }}</td>
</tr>
{% endfor %}
</tbody>
</table>

<script>
$(document).ready(function () {
  $("#user-table").DataTable({
    "order": [[ 1, "desc" ], [0, "asc"]]
  });
});
</script>
