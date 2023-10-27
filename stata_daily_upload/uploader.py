import os
import json
import logging
import datetime

import common
import common.change_tracking as ct
import ods_publish.etl_id as odsp


def set_access_policy_to_domain(ods_ids):
    """
    Makes the ODS dataset(s) public.
    """
    ods_ids = ods_ids if isinstance(ods_ids, list) else [ods_ids]
    for ods_id in ods_ids:
        odsp.ods_set_general_access_policy(ods_id, 'domain', True)


class Uploader:

    def __init__(self, path_work, ftp_server, ftp_user, ftp_pass):
        self.path_work = path_work
        self.ftp_server = ftp_server
        self.ftp_user = ftp_user
        self.ftp_pass = ftp_pass

    def read_uploads(self):
        """
        Reads the file stata_daily_uploads.json and returns the content as a dictionary.
        """
        path_uploads = os.path.join(self.path_work, 'StatA', 'stata_daily_uploads.json')
        with open(path_uploads, 'r') as jsonfile:
            return json.load(jsonfile), path_uploads

    def upload_backup_to_ftp(self, path_uploads):
        """
        Uploads the file stata_daily_uploads.json to the FTP server and renames it.
        """
        logging.info('Uploads have changed. Uploading to FTP...')
        remote_path = 'FST-OGD/archive_stata_daily_uploads'
        common.upload_ftp(path_uploads, self.ftp_server, self.ftp_user,
                          self.ftp_pass, remote_path)
        # Rename the file on the FTP server
        from_name = f"{remote_path}/stata_daily_uploads.json"
        to_name = f"stata_daily_uploads_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
        common.rename_ftp(from_name, to_name, self.ftp_server, self.ftp_user, self.ftp_pass)
        ct.update_hash_file(path_uploads)

    def upload_files_to_ftp(self, file_path, dest_dir):
        ct.update_mod_timestamp_file(file_path)
        common.upload_ftp(file_path, self.ftp_server, self.ftp_user, self.ftp_pass, dest_dir)

    def handle_files(self, upload):
        """
        Uploads the files to the FTP server and publishes the ODS datasets.
        Checks if the files are embargoed and if the embargo is over.
        There is also an embargo option to make the ODS dataset public after the embargo is over.
        """
        files = upload['file'] if isinstance(upload['file'], list) else [upload['file']]
        changed = False
        for file in files:
            file_path = os.path.join(self.path_work, file)
            is_embargoed = upload.get('embargo')
            is_embargo_over = common.is_embargo_over(file_path)
            if not is_embargoed or (is_embargoed and is_embargo_over):
                if ct.has_changed(file_path, method='modification_date'):
                    changed = True
                    self.upload_files_to_ftp(file_path, upload['dest_dir'])
            if upload.get('make_public_embargo') and common.is_embargo_over(file_path):
                set_access_policy_to_domain(upload['ods_id'])
        return changed
