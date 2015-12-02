1. The program is written in Python 2.7 using the library "paramiko".
2. The computers in the lab does not allow to install it, therefore I used the virtual machine of ubuntu/kali to do so.
3. First install pip to be able to install paramiko (installation of pip: http://pip.readthedocs.org/en/stable/installing/).
4. Run "pip install paramiko".
5. cd to the project directory.
6. The command is: "python program_manager.py <HOST> <USERNAME> <PASSWORD>" where you have to pass the paramaters host, username and password.
7. There are default values: HOST=localhost, USERNAME=cloudera, PASSWORD=cloudera.
8. You may send only the HOST and the other paramaters will get their default values.
9. Example: "python program_manager.py 192.168.134.10"- connection to 192.168.134.10 with USERNAME=cloudera PASSWORD=cloudera.
7. Example:
	python program_manager.py com4.cs.colman.ac.il shavitas password
	~Connected to com4.cs.colman.ac.il with shavitas~
	Commands:
	  mkdir <name>
	  putfile <local path> [<remote path>]
	  getfile <remote path> [<local path>]
	  quit

    mkdir test
    directory created successfully.

    putfile README.txt test/README.txt
    uploaded file.

    getfile test/README.txt test.txt
    downloaded file.

	quit
	closed.

8. for any question my email is: Asafsemail@gmail.com