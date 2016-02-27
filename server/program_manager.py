import os
import socket
import sys
import shutil

__author__ = 'Asaf'

from ssh_client import Connection

# HOST = 'localhost'
HOST = '192.168.23.131'
USER = 'training'
PASSWORD = 'training'


class ProgramManager(object):
    def __init__(self, host=HOST, username=USER, password=PASSWORD):
        self.options = dict(mkdir=self.create_directory, putfile=self.put_file,
                            getfile=self.get_file, quit=self.close_program,
                            final=self.run_final,
                            test=self.test)
        self.host, self.username, self.password = host, username, password
        try:
            self.ssh_client = Connection(self.host, username=self.username, password=self.password)
            print '~Connected to {host} with {username}~'.format(host=self.host, username=self.username)
        except socket.gaierror as e:
            print 'connection failed.'
            self.close_program()

    def start(self):
        self.print_instructions()
        while True:
            self.handle_command()


    def print_instructions(self):
        print 'Commands:\n' \
              '  mkdir <name> \n' \
              '  putfile <local path> [<remote path>]\n' \
              '  getfile <remote path> [<local path>]\n' \
              '  final\n' \
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
        print params
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
            print 'downloaded file: {file}.\n'.format(file=params[0])
        except WindowsError as e:
            print 'invalid local path.\n'
        except IOError as e:
            print e
            print e.args[1]

    def put_dir(self, local_path, remote_path):
        if local_path.endswith(u'/'):
            dir_name = local_path.split(u'/')[-2]
        else:
            dir_name = local_path.split(u'/')[-1]

        if not dir_name.endswith(u'/'):
            dir_name += u'/'

        if not local_path.endswith(u'/'):
            local_path += u'/'

        if not remote_path.endswith(u'/'):
            remote_path += u'/'

        new_remote_path = remote_path + dir_name

        self.create_directory([new_remote_path])

        for item in os.listdir(local_path):
            item_path = local_path + item
            if os.path.isdir(item_path):
                self.put_dir(local_path=local_path + item, remote_path=new_remote_path)
            else:
                self.put_file(params=[item_path, new_remote_path + item])

    def get_dir(self, local_path, remote_path):
        dir_name = remote_path.split(u'/')[-1]

        if not dir_name.endswith(u'/'):
            dir_name += u'/'

        if not local_path.endswith(u'/'):
            local_path += u'/'

        if not remote_path.endswith(u'/'):
            remote_path += u'/'

        new_local_dir_path = local_path + dir_name
        os.mkdir(new_local_dir_path)

        items = self.ssh_client.execute(u'ls ' + remote_path)
        items = map(lambda x: x.replace(u'\n', u''), items)

        for item in items:
            item_path = remote_path + item

            is_dir = self.ssh_client.execute(u'test -d {item_path}; echo $?'.format(item_path=item_path))[0]
            if is_dir == u'0\n':
                self.get_dir(remote_path=item_path, local_path=new_local_dir_path)
            else:
                self.get_file([item_path, new_local_dir_path + item])

    def run_final(self, params):
        dir_name = 'final/'
        hdfs_path = '/user/training/'
        input_data_dir = 'files_data/'
        output_dir = 'output/'
        java_files_dir = 'HADOOP_SRC/'
        jar_name = 'final.jar'
        package_name = 'stocksJob'
        main_class = 'StocksMainDriver'

        print '**Running job**\n' \
              'dir name={dir_name}.\n' \
              'input data dir={input_data_dir}.\n' \
              'java files_data={java_files_dir}.\n' \
              'jar name={jar_name}.\n' \
              'package name={package_name}.\n' \
              'main class={main_class}.\n' \
              'hdfs path={hdfs_path}.\n' \
              'output dir={output_dir}.\n'.format(
            dir_name=dir_name, input_data_dir=input_data_dir, java_files_dir=java_files_dir,
            jar_name=jar_name, package_name=package_name, main_class=main_class,
            hdfs_path=hdfs_path, output_dir=output_dir
            )

        # delete directories if already exists
        shutil.rmtree('{dir_name}{output_dir}'.format(dir_name=dir_name, output_dir=output_dir[:-1]), ignore_errors=True)
        command = 'rm -rf {dir_name}'.format(dir_name=dir_name)
        print command
        response = self.ssh_client.execute(command)
        print response

        command = 'hadoop fs -rm -r {dir_name}'.format(dir_name=dir_name)
        print command
        response = self.ssh_client.execute(command)
        print response

        # creating directory of the current job with the input data and uploading to it to hdfs
        self.create_directory([dir_name])
        self.put_dir(local_path=input_data_dir, remote_path=dir_name)

        command = 'hadoop fs -put {dir_name} {hdfs_path}'.format(hdfs_path=hdfs_path,
                                                                 dir_name=dir_name)
        print command
        response = self.ssh_client.execute(command)
        print response

        # uploading the java job files_data, compiling it and creating the jar
        self.put_dir(local_path=java_files_dir, remote_path=dir_name)

        command = 'cd {dir_name}{java_files_dir}; find -name "*.java" > sourcesJava.txt'.format(dir_name=dir_name,
                                                                                                java_files_dir=java_files_dir)

        print command
        response = self.ssh_client.execute(command)
        print response


        command = 'cd {dir_name}{java_files_dir}; javac -classpath `hadoop classpath` @sourcesJava.txt'.format(
            dir_name=dir_name,
            java_files_dir=java_files_dir)

        print command
        response = self.ssh_client.execute(command)
        print '\n'.join(response)

        command = 'cd {dir_name}{java_files_dir}; find -name "*.class" > sourcesClasses.txt'.format(dir_name=dir_name,
                                                                                                    java_files_dir=java_files_dir)

        print command
        response = self.ssh_client.execute(command)
        print response

        command = 'cd {dir_name}{java_files_dir}; jar cvf {jar_name} @sourcesClasses.txt'.format(
            dir_name=dir_name,
            jar_name=jar_name,
            java_files_dir=java_files_dir
        )
        print command
        response = self.ssh_client.execute(command)
        print '\n'.join(response)

        print 'running hadoop'
        command = 'cd {dir_name}{java_files_dir}; hadoop jar {jar_name} {package_name}.{main_class} {hdfs_path}{dir_name}{input_data_dir} {hdfs_path}{dir_name}{output_dir} {clusters}'.format(
            dir_name=dir_name,
            java_files_dir=java_files_dir,
            jar_name=jar_name,
            package_name=package_name,
            main_class=main_class,
            hdfs_path=hdfs_path,
            input_data_dir=input_data_dir,
            output_dir=output_dir,
            clusters=params[0],
        )
        print command
        response = self.ssh_client.execute(command)
        print '\n'.join(response)

        command = 'cd {dir_name}; hadoop fs -get {dir_name}{output_dir}'.format(
            dir_name=dir_name,
            output_dir=output_dir
        )
        print command
        response = self.ssh_client.execute(command)
        print '\n'.join(response)

        self.get_dir(local_path=dir_name,
                     remote_path='{dir_name}{output_dir}'.format(dir_name=dir_name, output_dir=output_dir[:-1]))

        # print 'done'
        return self

    def close_program(self, params=None):
        try:
            self.ssh_client.close()
        except Exception as e:
            pass
        finally:
            print 'closed SSH connection.'
            # quit()

    def test(self, params):
        pass


def run_manager(argv):
    if 0 < len(argv) <= 4:
        ProgramManager(*argv).start()
    elif len(argv) is 0:
        ProgramManager().start()
    else:
        print 'invalid arguments.\n' \
              'you can send <host> <username> <password> or nothing to connect defaults'


if __name__ == '__main__':
    run_manager(sys.argv[1:])
