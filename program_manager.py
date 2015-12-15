import os
import socket
import sys

__author__ = 'Asaf'

from ssh_client import Connection

HOST = 'localhost'
USER = 'training'
PASSWORD = 'training'


class ProgramManager(object):
    def __init__(self, host=HOST, username=USER, password=PASSWORD):
            self.options = dict(mkdir=self.create_directory, putfile=self.put_file,
                                getfile=self.get_file, quit=self.close_program, run_log_analysis_job=self.run_log_analysis_job)
            self.host, self.username, self.password = host, username, password
            self.ssh_client = None

    def start(self):
        try:
            self.ssh_client = Connection(self.host, username=self.username, password=self.password)
            print '~Connected to {host} with {username}~'.format(host=self.host, username=self.username)
            self.print_instructions()
            while True:
                self.handle_command()
        except socket.gaierror as e:
            print 'connection failed.'
            self.close_program()

    def print_instructions(self):
        print 'Commands:\n' \
              '  mkdir <name> \n' \
              '  putfile <local path> [<remote path>]\n' \
              '  getfile <remote path> [<local path>]\n' \
              '  run_log_analysis_job\n' \
              '  quit\n'

    def handle_command(self):
        user_input = raw_input()
        params = [param for param in user_input.split() if param]
        try:
            command = self.options[params[0]]
            command(params[1:])
        except KeyError as e:
            print 'unknown command'
            self.print_instructions()

    def create_directory(self, params):
        if len(params) is not 1:
            print 'invalid arguments'
            self.print_instructions()
            return

        errors = self.ssh_client.execute(command='mkdir {path}'.format(path=params[0]))
        if errors:
            print 'failed to create directory, please make sure that the path is correct.\n'
        else:
            print 'directory created successfully.\n'

    def put_file(self, params):
        try:
            if len(params) is 1:
                self.ssh_client.put(localpath=params[0])
            elif len(params) is 2:
                self.ssh_client.put(localpath=params[0], remotepath=params[1])
            else:
                print 'invalid arguments'
                self.print_instructions()
                return
            print 'uploaded file: {file}.\n'.format(file=params[0])
        except WindowsError as e:
            print 'file does not exists.\n'
        except IOError as e:
            print 'invalid remote path.\n'

    def get_file(self, params):
        try:
            if len(params) is 1:
                self.ssh_client.get(remotepath=params[0])
            elif len(params) is 2:
                self.ssh_client.get(remotepath=params[0], localpath=params[1])
            else:
                print 'invalid arguments\n'
                return
            print 'downloaded file.\n'
        except WindowsError as e:
            print 'invalid local path.\n'
        except IOError as e:
            print e.args[1]

    def put_dir(self, local_path, remote_path):
        dir_name = local_path.split('/')[-1]
        self.create_directory(['{remote_path}/{dir_name}'.format(remote_path=remote_path, dir_name=dir_name)])
        for item in os.listdir(local_path):
            self.put_file(params=['{local_path}/{file}'.format(local_path=local_path, file=item),
                                  '{remote_path}/{dir_name}/{file}'.format(remote_path=remote_path, dir_name=dir_name, file=item)])

    def run_log_analysis_job(self, params):
        hdfs_path = '/user/training/'
        dir_name = 'task3'
        input_data_dir = 'inputDataTask3'
        output_data_dir = hdfs_path + 'output'
        java_files_dir = 'solution'
        jar_name = 'task3.jar'
        # hadoop_classpath = '/etc/hadoop/conf:/usr/lib/hadoop/lib/*:/usr/lib/hadoop/.//*:/usr/lib/hadoop-hdfs/./:/usr/lib/hadoop-hdfs/lib/*:/usr/lib/hadoop-hdfs/.//*:/usr/lib/hadoop-yarn/lib/*:/usr/lib/hadoop-yarn/.//*:/usr/lib/hadoop-0.20-mapreduce/./:/usr/lib/hadoop-0.20-mapreduce/lib/*:/usr/lib/hadoop-0.20-mapreduce/.//*'

        self.create_directory([dir_name])
        # print 'javac -classpath {hadoop_classpath} task3/solution/*.class'.format(hadoop_classpath=hadoop_classpath)
        self.put_dir(local_path=input_data_dir, remote_path='{dir_name}'.format(dir_name=dir_name))
        print self.ssh_client.execute('hadoop fs -put {dir_name} {hdfs_path}/{dir_name}'.format(hdfs_path=hdfs_path, dir_name=dir_name))
        self.put_dir(local_path=java_files_dir, remote_path='{dir_name}'.format(dir_name=dir_name, java_files_dir=java_files_dir))
        # print self.ssh_client.execute('hadoop fs -mkdir {hdfs_path}/{dir_name}'.format(hdfs_path=hdfs_path, dir_name=dir_name))
        # print self.ssh_client.execute('hadoop fs -put {dir_name}/{input_data_dir} {hdfs_path}/{dir_name}'.format(hdfs_path=hdfs_path, input_data_dir=input_data_dir, dir_name=dir_name))
        print '\n'.join(self.ssh_client.execute('cd ~/task3'))
        print '\n'.join(self.ssh_client.execute('pwd'))
        print '\n'.join(self.ssh_client.execute('cd task3; javac -classpath `hadoop classpath` solution/*.java'))
        print '\n'.join(self.ssh_client.execute('cd task3; jar cvf task3.jar solution/*.class'))
        print 'running hadoop'
        print '\n'.join(self.ssh_client.execute('cd task3; hadoop jar task3.jar solution.WordMatch task3/inputDataTask3 task3/output'))
        print '\n'.join(self.ssh_client.execute('cd task3; hadoop fs -get task3/output'))
        self.get_file('task3/output')
        print 'done'

        # print self.ssh_client.execute('hadoop fs -put {dir_name}/{java_files_dir} {hdfs_path}/{dir_name}'.format(hdfs_path=hdfs_path, java_files_dir=java_files_dir, dir_name=dir_name))
        # print self.ssh_client.execute('')



    def close_program(self, params=None):
        try:
            self.ssh_client.close()
        except Exception as e:
            pass
        finally:
            print 'closed.'
            quit()


def main(argv):
    if 0 < len(argv) <= 4:
        ProgramManager(*argv).start()
    elif len(argv) is 0:
        ProgramManager().start()
    else:
        print 'invalid arguments.\n' \
              'you can send <host> <username> <password> or nothing to connect defaults'

if __name__ == '__main__':
    main(sys.argv[1:])
