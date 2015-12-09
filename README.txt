$hadoop fs -mkdir task2

$hadoop fs -put inputDataTask2 /user/training/task2

$hadoop fs -put wordfilter.txt /user/training/task2

$javac -classpath `hadoop classpath` solution/*.java

$jar cvf task2.jar solution/*.class

$hadoop jar task2.jar solution.WordMatch /user/training/task2/inputDataTask2 /user/training/task2/outputTask2 /user/training/task2/wordfilter.txt

...

$hadoop fs -get /user/training/task2/outputTask2
