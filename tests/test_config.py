import unittest
from pathlib import Path

import yaml

from src.processing.normalize import _canonical_key


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.config = yaml.safe_load(Path("config/settings.yaml").read_text())

    def test_default_artist_list_has_150_unique_entries(self):
        artists = self.config["artists"]
        self.assertEqual(len(artists), 150)
        self.assertEqual(len({_canonical_key(name) for name in artists}), 150)

    def test_spotify_is_disabled_by_default(self):
        self.assertFalse(self.config["sources"]["enabled"]["spotify"])


if __name__ == "__main__":
    unittest.main()
