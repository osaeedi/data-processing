import logging

import common.change_tracking as ct
import ods_publish.etl_id as odsp
from stata_daily_upload import credentials
from stata_daily_upload.uploader import Uploader


def main():
    uploader = Uploader(credentials.path_work, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass)
    uploads, path_uploads = uploader.read_uploads()
    if ct.has_changed(path_uploads):
        uploader.upload_backup_to_ftp(path_uploads)

    # Iterate over all uploads and upload the files to the FTP server and publish the ODS datasets
    file_not_found_errors = []
    for upload in uploads:
        try:
            changed = uploader.handle_files(upload)
            if changed:
                ods_ids = upload['ods_id'] if isinstance(upload['ods_id'], list) else [upload['ods_id']]
                for ods_id in ods_ids:
                    odsp.publish_ods_dataset_by_id(ods_id)
        except FileNotFoundError as e:
            file_not_found_errors.append(e)
    # If there were any FileNotFoundError, raise an exception
    error_count = len(file_not_found_errors)
    if error_count > 0:
        for e in file_not_found_errors:
            logging.exception(e)
        raise FileNotFoundError(f'{error_count} FileNotFoundErrors have been raised!')
    print('Job successful!')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
