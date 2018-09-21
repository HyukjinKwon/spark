-- infer schema of json literal
select schema_of_csv('1,abc');
select schema_of_csv('1|abc', map('delimiter', '|'));
