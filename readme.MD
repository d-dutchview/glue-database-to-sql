query all tables from glue and create propper SQL statements for all tables script.


input:

```
{
"name": "tablename",
"Colums": [
	{
		"Name": "index",
		"Type": "bigint"
	},
	{
		"Name": "email",
		"Type": "string"
	},
	{
		"Name": "name",
		"Type": "string"
	}
]
}
```

output:

```
CREATE TABLE tablename (
index BIGINT,
email VARCHAR(255),
name VARCHAR(255));
```
