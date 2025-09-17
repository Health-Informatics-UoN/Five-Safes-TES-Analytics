import pytest
import polling

from tes_client import TESClient, get_status_description
from minio_client import MinIOClient


## [11, 27, 16, 49] are the end statuses


@pytest.mark.parametrize(
    "status_code,expected_description",
    [
        (11, "Completed"),
        (27, "Failed"),
        (16, "Cancelled"),
        (49, "Complete but not all TREs returned a result"),
    ],
)
def test_poll_task_status(mocker, status_code, expected_description):
    # create mock tes client
    mock_tes_client = mocker.Mock()

    # set up mock response from tes client
    mock_tes_client.get_task_status.return_value = {"status": status_code}

    # set up mock response from minio client (this test doesn't need it)
    mock_minio_client = mocker.Mock()

    # create polling engine with a sample task id of 1
    polling_engine = polling.Polling(mock_tes_client, mock_minio_client, 1)

    # call poll_task_status
    polling_engine.poll_task_status()

    # check that the status matches the expected end status
    assert polling_engine.status == status_code
    assert polling_engine.status_description == expected_description


## the minio poll test will need to be mocked
## can't test for n_results >2 because the test will run forever, all others should return the two results.
@pytest.mark.parametrize("n_results", [None, 1, 2])
def test_poll_minio_results(mocker, n_results):
    # create mock tes client
    mock_tes_client = mocker.Mock()
    mock_tes_client.get_task_status.return_value = {"status": 11}

    # set up mock response from minio client
    mock_minio_client = mocker.Mock()
    mock_minio_client.get_object.return_value = "test_data"

    # create polling engine with a sample task id of 1
    polling_engine = polling.Polling(mock_tes_client, mock_minio_client, "1")

    # call poll_minio_results
    data = polling_engine.poll_minio_results(
        ["test_path 1", "test_path 2"], "test_bucket", n_results=n_results
    )

    # check that the data is returned
    assert data == ["test_data", "test_data"]
    assert polling_engine.data == ["test_data", "test_data"]


# @pytest.fixture
# def polling_engine(test_tes_client, test_minio_client, test_id):
#    return polling.Polling(mock_tes_client, mock_minio_client, task_id)


def test_polling_engine(mocker):
    # poll_results(self, results_paths: List[str], bucket: str, n_results: int = 1, polling_interval: int = 10) -> List[str]:

    polling_engine = polling.Polling(mocker.Mock(), mocker.Mock(), "1")
    # Mock the internal methods of the polling engine
    mocker.patch.object(
        polling_engine, "poll_task_status", return_value=(11, "Completed")
    )
    mocker.patch.object(
        polling_engine,
        "poll_minio_results",
        return_value=["result1.csv", "result2.csv"],
    )

    results_paths = ["2/output.csv", "3/output.csv"]
    bucket = "test_bucket"

    data = polling_engine.poll_results(results_paths, bucket, polling_interval=0.1)

    assert data is not None
    assert len(data) == 2
    assert "result1.csv" in data
    assert "result2.csv" in data

    ##polling interval and n_results are not optional for these functions, even though they are in poll results
    polling_engine.poll_task_status.assert_called_once_with(0.1)
    polling_engine.poll_minio_results.assert_called_once_with(
        results_paths, bucket, 1, 0.1
    )
