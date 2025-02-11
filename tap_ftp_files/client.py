import os
import re
import tempfile
from datetime import datetime
from ftplib import FTP, all_errors, FTP_TLS
import time
import ssl
import pytz

import logging


logger = logging.getLogger("tap-ftp-files")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class IMP_FTP_TLS(FTP_TLS):
    """FTP_TLS subclass that automatically wraps sockets in SSL to support implicit FTPS."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        """Return the socket."""
        return self._sock

    @sock.setter
    def sock(self, value):
        """When modifying the socket, ensure that it is ssl wrapped."""
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value


class FTPConnection:
    def __init__(self, host, username, password, port):
        self.host = host
        self.username = username
        self.password = password
        self.port = int(port or 21)
        self.decrypted_file = None
        self.key = None
        self.ftp = FTP()
        self.retries = 10
        self.connect()

    # If connection is snapped during connect flow, retry up to a
    # minute for SSH connection to succeed. 2^6 + 2^5 + ...

    def connect(self):
        for i in range(self.retries + 1):
            try:
                logger.info("Creating new connection to FTP...")
                self.ftp.connect(self.host, self.port)
                self.ftp.login(self.username, self.password)
                if isinstance(self.ftp, FTP_TLS) or isinstance(self.ftp, IMP_FTP_TLS):
                    self.ftp.prot_p()
                logger.info("Connection successful")
                break
            except all_errors as ex:
                if "Policy requires SSL." in str(ex):
                    self.ftp = FTP_TLS()
                if str(ex)=="":
                    self.ftp = IMP_FTP_TLS()
                elif "530 Non-anonymous" in str(ex):
                    self.ftp = FTP_TLS()
                    self.ftp.connect(self.host, self.port)
                    self.ftp.login(self.username, self.password)
                    self.ftp.prot_p()
                elif self.ftp:
                    self.ftp.quit()
                time.sleep(5 * i)
                logger.info("Connection failed, retrying...")
                if i >= (self.retries):
                    raise ex

    def close(self):
        try : 
            self.ftp.quit()
        except : 
            logger.info("Recieved EOF after closing connection")

    def match_files_for_table(self, files, table_name, search_pattern):
        logger.info(
            "Searching for files for table '%s', matching pattern: %s",
            table_name,
            search_pattern,
        )
        matcher = re.compile(search_pattern)
        return [f for f in files if matcher.search(f["filepath"])]

    def is_empty(self, file_attr):
        try:
            return self.ftp.size(file_attr) == 0
        except:
            return False

    def is_directory(self, file_attr):
        try:
            self.ftp.cwd(file_attr)
            directory_height = file_attr.count("/")
            self.ftp.cwd(".." + "/.." * directory_height)
            return True
        except all_errors:
            return False

    def get_files_by_prefix(self, prefix, search_subdirectories=True):
        """
        Accesses the underlying file system and gets all files that match "prefix", in this case, a directory path.

        Returns a list of filepaths from the root.
        """
        files = []

        if not prefix:
            prefix = ""

        try:
            result = self.ftp.nlst(prefix)
        except EOFError:
            self.connect()
            result = self.ftp.nlst(prefix)
        except FileNotFoundError as e:
            raise Exception("Directory '{}' does not exist".format(prefix)) from e

        for file_attr in result:
            if self.is_directory(file_attr) and search_subdirectories:
                files += self.get_files_by_prefix(file_attr)
            else:
                if self.is_directory(file_attr):
                    continue
                if self.is_empty(file_attr):
                    continue

                try:
                    last_modified = self.ftp.voidcmd(f"MDTM {file_attr}")[4:].strip()
                except:
                    last_modified = None

                if last_modified is None:

                    logger.warning(
                        "Cannot read m_time for file %s, defaulting to current epoch time",
                        os.path.join(prefix, file_attr),
                    )
                    last_modified = datetime.utcnow()
                else:
                    last_modified = datetime.strptime(last_modified, "%Y%m%d%H%M%S")
                files.append(
                    {
                        "filepath": f"/{file_attr}",
                        "last_modified": last_modified.replace(tzinfo=pytz.UTC),
                    }
                )

        return files

    def get_files(
        self, prefix, search_pattern, modified_since=None, search_subdirectories=True
    ):
        files = self.get_files_by_prefix(prefix, search_subdirectories)
        if files:
            logger.info('Found %s files in "%s"', len(files), prefix)
        else:
            logger.warning('Found no files on specified SFTP server at "%s"', prefix)
        # TODO Fix matching pattern
        matching_files = self.get_files_matching_pattern(files, search_pattern)

        if matching_files:
            logger.info(
                'Found %s files in "%s" matching "%s"',
                len(matching_files),
                prefix,
                search_pattern,
            )
        else:
            logger.warning(
                'Found no files on specified SFTP server at "%s" matching "%s"',
                prefix,
                search_pattern,
            )

        for f in matching_files:
            logger.info("Found file: %s", f["filepath"])

        if modified_since is not None:
            matching_files = [
                f for f in matching_files if f["last_modified"] > modified_since
            ]

        return matching_files

    def get_file_handle(self, f):
        """Takes a file dict {"filepath": "...", "last_modified": "..."} and returns a handle to the file."""
        with tempfile.TemporaryDirectory() as tmpdirname:
            file_path = f["filepath"]
            local_path = f'{tmpdirname}/{file_path.split("/")[-1]}'

            try:
                self.ftp.retrbinary(f"RETR {file_path}", open(local_path, "wb").write)
            except EOFError:
                self.connect()
                self.ftp.retrbinary(f"RETR {file_path}", open(local_path, "wb").write)
            
            return open(local_path, "rb")

    def get_files_matching_pattern(self, files, pattern):
        """Takes a file dict {"filepath": "...", "last_modified": "..."} and a regex pattern string, and returns
        files matching that pattern."""
        matcher = re.compile(pattern)
        logger.info(f"Searching for files for matching pattern: {pattern}")
        return [f for f in files if matcher.search(f["filepath"])]


def connection(config):
    return FTPConnection(
        config["host"], config["username"], config["password"], port=config.get("port")
    )
