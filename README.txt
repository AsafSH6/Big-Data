1. The program is written in Python 2.7 using the library "paramiko".
2. The computers in the lab does not allow to install it, therefore I used the virtual machine of ubuntu/kali to do so.
3. First install pip to be able to install paramiko (installation of pip: http://pip.readthedocs.org/en/stable/installing/).
4. Run "pip install paramiko".
5. cd to the project directory (otherwise part 4 of the task will not work!).
6. The command is: "python program_manager.py <HOST> <USERNAME> <PASSWORD>" where you have to pass the parameters host, username and password.
7. There are default values: HOST=localhost, USERNAME=training, PASSWORD=training.
8. You may send only the HOST and the other parameters will get their default values.
9. Example: "python program_manager.py 10.0.0.23"- open connection with 10.0.0.23 using USERNAME=training PASSWORD=training.
7. Example:
	python program_manager.py 10.0.0.23
	~Connected to 10.0.0.23 with training~
	Commands:
	  mkdir <name>
	  putfile <local path> [<remote path>]
	  getfile <remote path> [<local path>]
	  task3
	  quit

    task3
    **Running job**
	dir name=task3/.
	input data dir=inputDataTask3/.
	java files=solution/.
	jar name=task3.jar.
	package name=solution.
	main class=LogAnalysis.
	hdfs path=/user/training/.
	output dir=output.
	....
	.....
	......
	.......

8. The program will create directory named task3 in coudera and the hdfs therefore, make sure that the directory does not exists already
   in your cloudera machine or the hdfs.
   It also uses the directory task3 in project to download the output to.
   if you run the program more than once, remember to remove the data from the task3 directory
   and delete task3 directory from cloudera and hdfs.
   'rm -rf task3' to remove the directory from cloudera.
   'hadoop fs -rm -r task3' to remove the directory from the hdfs.

9. According to the first line of the task, we had to create program that count the number of views for specific
   website domain *by day* per user, I know that other people did not take the day parameter in account but that is how I understood the task.

8. for any question my email is: Asafsemail@gmail.com