import hashlib
import os
import uuid
from datetime import datetime
from ftplib import FTP
from ftplib import error_perm
from plugins.fetcher.base import FetcherPlugin
import errno
from dateutil import parser

# from plugins.fetcher import FetcherResponse

from dateutil import parser

def is_file(ftp, filename):
    current = ftp.pwd()
    try:
        ftp.cwd(filename)
    except:
        ftp.cwd(current)
        return True
    ftp.cwd(current)
    return False

#(uuid,extension)
def _rename_file(file_name):
    """
    :param file_name:
    :return:
    """
    parts = file_name.rsplit(".", 1)
    extension = parts[-1] if len(parts) == 2 else None
    new_name = str(uuid.uuid4())
    if extension is not None:
        return "{file_name}.{extension}".format(file_name=new_name, extension=extension), extension
    return new_name, extension

def parse_ls_result(result):
    file_map = {}
    for line in result:
        tokens = line.split(maxsplit=9)
        name = " ".join(tokens[8:])
        time_str = tokens[5] + " " + tokens[6] + " " + tokens[7]
        time = parser.parse(time_str)
        file_map[name] = time
    return file_map

def _configure_ftp_client(config):
    """
    :param config:
    :return:
    """
    ftp_client = FTP(host=config['host'], user=config['user'], passwd=config['password'])
    ftp_client.connect()
    return ftp_client

class FTPFetcher(FetcherPlugin):
    NAME = "FTP Fetcher"
    VERSION = "1.0"
    def __init__(self, config, archive_path, fetcher_log={}, *args, **kwargs):
        super(FTPFetcher, self).__init__(config, archive_path, *args, **kwargs)
        self.client = _configure_ftp_client(config)
        self.progress = {x:parser.parse(fetcher_log.get('progress', {})[x]) for x in fetcher_log.get('progress', {})}
        self.archive_path = archive_path
    def get_list_of_directories(self):
        dirs = []
        self.client.dir('-t', lambda x: dirs.append(x) if x.startswith('d') else None)
        dirs = parse_ls_result(dirs)
        return dirs.keys()
    def fetch_files_from_cwd(self):
        pwd = self.client.pwd()
        print("PWD : ", pwd)
        folder = pwd.split('/')[2]
        folder = '/'.join(pwd.split('/')[2:])
        print("FOLDER is ", folder)
        pwd_progress = self.progress.get(pwd, None)
        print("PWD progress : ", pwd_progress)
        responses, last_fetched = self.fetch_files_from_path(folder, pwd, pwd_progress)
        self.progress[pwd] = last_fetched
        return responses
    def traverse_dir(self, file_path):
        if not file_path:
            print("Here")
            res = self.fetch_files_from_cwd()
            print("After got files: ", res)
            return res
        ndir = file_path.pop(0)
        print("This is ndir: ", ndir)
        
        if ndir == '*':
            dirs = self.get_list_of_directories()
            print("Received dirs: ", dirs)
            new_dirs = [[each] + file_path for each in dirs]
            
            combined_responses = []
            current_dir = self.client.pwd()
            for each_dir in new_dirs:
                print("Each dir: ", each_dir)
                fetc_res = self.traverse_dir(each_dir)
                print("Inside loop: ", fetc_res)
                combined_responses.extend(fetc_res)
                self.client.cwd(current_dir)
            return combined_responses
        else:
            print("Changed to their: ", self.client.pwd())
            self.client.cwd(ndir)
            return self.traverse_dir(file_path)
    def fetch_files_from_path(self, folder, pwd, path_progress=None):
        
        responses = []
        files = []
        latest_last_modified = path_progress
        self.client.dir('-t', files.append)

        res = parse_ls_result(files)
       
        min_date_time = datetime.min
       
        latest_last_modified = path_progress
        print("RES ", res)
        for file_name, last_modified in res.items():

            
            if path_progress and last_modified <= path_progress:
                print("Continued")
                continue
            else:
                if last_modified > min_date_time:
                    min_date_time = last_modified
                unique_file_name = '{uid}_{file_name}'.format(uid = str(uuid.uuid4()), file_name = str(file_name))
                
                file_path = self.prepare_path(unique_file_name,folder)
                path = os.path.join(self.archive_path,folder)
                print("PATH IS ", path)
                try:
                    os.makedirs(path)
                except OSError as exc:
                    if exc.errno == errno.EEXIST and os.path.isdir(path):
                        print("Dir already exists")
                        pass
                    else:
                        raise
                this_level = os.path.join(pwd, file_name)
                if is_file(self.client,this_level ):
                    self.client.retrbinary('RETR ' + file_name, open(file_path, 'wb').write)
                    file = open(file_path, 'rb')
                    m = hashlib.md5()
                    m.update(file.read())
                    file.seek(0, os.SEEK_END)
                    size = file.tell()
                    # Add Responses after downloading files
                    response = {
                        'file_size': size,
                        'file_path': file_path,
                        'file_md5': m.hexdigest()
                    }
                    responses.append(response)
                
                    print('response: ', response)
                    latest_last_modified = min_date_time
        return responses, latest_last_modified
    def run(self, progress=None):
        """
        :return:
        """
        print("SELF PROGRESS : ",self.progress)
        dir_path = self.config['dir']
        dir_list = dir_path.split('/')[1:]
        print("DIR LIST ",dir_list)
        responses = self.traverse_dir(dir_list)
        progress = {x:str(self.progress[x]) for x in self.progress}
        return responses, progress

        # return FetcherResponse(items=responses, progress={'date': datetime.now().strftime('%Y-%m-%d')})
    @classmethod
    def validation_config(cls):
        return [
            {
                "name": "host",
                "display_name": "FTP Host",
                "validation": {
                    # TODO: Make this validate
                    "type": "string",
                    "required": True
                }
            },
            {
                "name": "username",
                "display_name": "Username",
                "validation": {
                    "type": "string",
                    "required": True
                }
            },
            {
                "name": "password",
                "display_name": "Password",
                "validation": {
                    "type": "password",
                    "required": True
                }
            },
            # {
            #     "name": "search_rule",
            #     "display_name": "Search Rule",
            #     "validation": {
            #         # TODO: Make this validate
            #         "type": "jsondata",
            #         "required": True,
            #         "schema": [
            #             {
            #                 "name": "folder",
            #                 "display_name": "Search Folder",
            #                 "validation": {
            #                     "type": "string",
            #                     "required": True
            #                 }
            #
            #             },
            #
            #             {
            #                 "name": "email_from",
            #                 "display_name": "Email From",
            #                 "validation": {
            #                     "type": "email",
            #                     "required": False
            #                 }
            #             },
            #
            #             {
            #                 "name": "subject",
            #                 "display_name": "Email Subject",
            #                 "validation": {
            #                     "type": "string",
            #                     "required": False
            #                 }
            #             },
            #             {
            #                 "name": "body",
            #                 "display_name": "Body",
            #                 "validation": {
            #                     "type": "list",
            #                     "list_type": "string",
            #                     "required": False
            #                 }
            #             }
            #         ]
            #     }
            # }
        ]
    @classmethod
    def on_run_config(cls):
        return [
            {
                "name": "password",
                "display_name": "Password",
                "validation": {
                    "type": "string",
                    "required": True
                }
            }
        ]
Name = "FTP"
ID = "FTP_PLUGIN_1.0"
Version = "1.0"
EXECUTOR = FTPFetcher