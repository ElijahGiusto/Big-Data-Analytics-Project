import unittest
from pathlib import Path

from src.storage.parquet_store import processed_data_path, storage_mode


class StorageConfigTests(unittest.TestCase):
    def test_local_processed_path_is_absolute_repo_path(self):
        config = {
            "storage": {
                "mode": "local",
                "processed_path": "data/processed",
            }
        }

        path = processed_data_path(config)

        self.assertTrue(Path(path).is_absolute())
        self.assertTrue(path.endswith(str(Path("data") / "processed")))

    def test_hdfs_processed_path_uses_hdfs_uri(self):
        config = {
            "storage": {
                "mode": "hdfs",
                "hdfs_uri": "hdfs://localhost:9000/",
                "hdfs_processed_path": "/artist-popularity/processed",
            }
        }

        self.assertEqual(storage_mode(config), "hdfs")
        self.assertEqual(
            processed_data_path(config),
            "hdfs://localhost:9000/artist-popularity/processed",
        )


if __name__ == "__main__":
    unittest.main()
