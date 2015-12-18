$hadoop fs -mkdir task3

$hadoop fs -put inputDataTask3 /user/training/task3

$javac -classpath `hadoop classpath` solution/*.java

$jar cvf task3.jar solution/*.class

$hadoop jar task3.jar solution.LogAnalysis /user/training/task3/inputDataTask3 /user/training/task3/output

...

$hadoop fs -get /user/training/task3/output

***According to the first line of the task, we had to create program that count the number of views for specific
website domain *by day* per user, I know that other people did not take the day parameter in account but that is how I understood the task.

