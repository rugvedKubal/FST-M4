lines = LOAD 'hdfs:///user/root/episodes/' AS (line);
filter_lines = FILTER lines BY (NOT line MATCHES '.*EPISODE.*(4|5|6).*');
words = FOREACH filter_lines GENERATE FLATTEN(STRSPLIT(line, '\\t', 2)) AS word;
grouped_words = GROUP words BY word;
op = FOREACH grouped_words GENERATE group, COUNT(words) AS count:long;
order_by_count = ORDER op BY count DESC;
STORE order_by_count INTO 'hdfs:///user/root/Activity1_Pig/results';