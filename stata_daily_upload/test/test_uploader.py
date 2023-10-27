import pytest
from unittest import mock
from stata_daily_upload.uploader import Uploader, set_access_policy_to_domain


# Sample test data and mock configurations here
@pytest.fixture
def uploader_instance():
    return Uploader('path_work_mock', 'ftp_server_mock', 'ftp_user_mock', 'ftp_pass_mock')


def test_read_uploads(uploader_instance):
    with mock.patch('os.path.join', return_value='mocked_path'), \
         mock.patch('builtins.open', mock.mock_open(read_data='{"mock_key": "mock_value"}')):
        data, path = uploader_instance.read_uploads()
    assert data == {"mock_key": "mock_value"}
    assert path == 'mocked_path'


def test_upload_backup_to_ftp(uploader_instance, monkeypatch):
    # Mocking FTP functions using MagicMock
    mock_upload_ftp = mock.MagicMock()
    mock_rename_ftp = mock.MagicMock()
    mock_update_hash_file = mock.MagicMock()

    monkeypatch.setattr('common.upload_ftp', mock_upload_ftp)
    monkeypatch.setattr('common.rename_ftp', mock_rename_ftp)
    monkeypatch.setattr('common.change_tracking.update_hash_file', mock_update_hash_file)

    uploader_instance.upload_backup_to_ftp('mocked_path')

    # Asserts to check if mock methods were called
    mock_upload_ftp.assert_called_once()
    mock_rename_ftp.assert_called_once()
    mock_update_hash_file.assert_called_once_with('mocked_path')


def test_upload_files_to_ftp(uploader_instance, monkeypatch):
    # Mocking FTP functions using MagicMock
    mock_update_mod_timestamp_file = mock.MagicMock()
    mock_upload_ftp = mock.MagicMock()

    monkeypatch.setattr('common.change_tracking.update_mod_timestamp_file', mock_update_mod_timestamp_file)
    monkeypatch.setattr('common.upload_ftp', mock_upload_ftp)

    uploader_instance.upload_files_to_ftp('mocked_file_path', 'mocked_dest_dir')

    # Asserts to check if mock methods were called
    mock_update_mod_timestamp_file.assert_called_once_with('mocked_file_path')
    mock_upload_ftp.assert_called_once()


def test_handle_files(uploader_instance, monkeypatch):
    # Mocking FTP functions using MagicMock
    mock_update_mod_timestamp_file = mock.MagicMock()
    mock_upload_ftp = mock.MagicMock()
    mock_is_embargo_over = mock.MagicMock(return_value=False)
    mock_has_changed = mock.MagicMock(return_value=True)

    monkeypatch.setattr('common.change_tracking.update_mod_timestamp_file', mock_update_mod_timestamp_file)
    monkeypatch.setattr('common.upload_ftp', mock_upload_ftp)
    monkeypatch.setattr('common.is_embargo_over', mock_is_embargo_over)
    monkeypatch.setattr('common.change_tracking.has_changed', mock_has_changed)

    mock_upload = {
        'file': 'mocked_file_name',
        'dest_dir': 'mocked_dest_dir',
        'ods_id': 'mocked_ods_id'
    }

    uploader_instance.handle_files(mock_upload)

    # Asserts to check if mock methods were called
    mock_update_mod_timestamp_file.assert_called_once_with('path_work_mock/mocked_file_name')
    mock_upload_ftp.assert_called_once()



def test_set_access_policy_to_domain(monkeypatch):
    # Mocking FTP functions using MagicMock
    mock_ods_set_general_access_policy = mock.MagicMock()

    monkeypatch.setattr('ods_publish.etl_id.ods_set_general_access_policy', mock_ods_set_general_access_policy)

    set_access_policy_to_domain('mocked_ods_ids')

    # Asserts to check if mock methods were called
    mock_ods_set_general_access_policy.assert_called_once_with('mocked_ods_ids', 'domain', True)
