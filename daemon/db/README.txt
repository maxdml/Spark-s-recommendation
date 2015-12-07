Usage: create_summary_log  <log directory>
       create_summary_logs <directory containing log directories>

This program combines BTrace logs and event logs collected from a Spark
application and generates a summary log in JSON file.


Example:
$ ./create_summary_log/bin/create_summary_log  sample_input/app-20151025144440-0005-28-6-5-pagerank-1m/
$ ./create_summary_log/bin/create_summary_logs sample_input/
