# tap-ftp-files

Hotglue tap for importing files from SFTP

The following config values are expected:

```
        "host": "FTP_HOST_NAME",
        "port": "FTP_PORT",
        "username": "YOUR_USER",
        "password": "YOUR_PASS",
        "incremental_mode": true,
        "tables": [
            {
                "remote_path": "DIRECTORY_TO_EXPORT",
                "search_pattern": "REGEX_FILTER_FOR_FIES_TO_EXPORT",
            }
        ],
```
