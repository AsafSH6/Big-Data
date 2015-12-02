import socket
import sys

__author__ = 'Asaf'

from ssh_client import Connection

HOST = 'localhost'
USER = 'cloudera'
PASSWORD = 'cloudera'


class ProgramManager(object):
    def __init__(self, host=HOST, username=USER, password=PASSWORD):
            self.options = dict(mkdir=self.create_directory, putfile=self.put_file,
                                getfile=self.get_file, quit=self.close_program)
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
            print 'uploaded file.\n'
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
