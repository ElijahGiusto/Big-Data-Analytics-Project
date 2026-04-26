import unittest

from src.processing.normalize import (
    _canonical_key,
    build_artist_lookup,
    canonicalize_artist_name,
)


class ArtistCanonicalizationTests(unittest.TestCase):
    def setUp(self):
        self.lookup = build_artist_lookup([
            "Bjork",
            "King Gizzard and the Lizard Wizard",
            "Mac DeMarco",
            "My Bloody Valentine",
            "PornoGraffitti",
            "Run the Jewels",
            "Sigur Ros",
            "Tool",
            "Tyler the Creator",
        ])

    def test_accent_case_and_punctuation_variants_match_config(self):
        cases = {
            "Björk": "Bjork",
            "KING GIZZARD & THE LIZARD WIZARD": "King Gizzard and the Lizard Wizard",
            "Mac Demarco": "Mac DeMarco",
            "my bloody valentine": "My Bloody Valentine",
            "Porno Graffitti": "PornoGraffitti",
            "Run The Jewels": "Run the Jewels",
            "Sigur Rós": "Sigur Ros",
            "TOOL": "Tool",
            "Tyler, The Creator": "Tyler the Creator",
        }

        for variant, expected in cases.items():
            with self.subTest(variant=variant):
                self.assertEqual(
                    canonicalize_artist_name(variant, self.lookup),
                    expected,
                )

    def test_unknown_artist_is_preserved(self):
        self.assertEqual(
            canonicalize_artist_name("Unknown Artist", self.lookup),
            "Unknown Artist",
        )

    def test_canonical_key_removes_nonsemantic_differences(self):
        self.assertEqual(_canonical_key("Sigur Rós"), _canonical_key("sigur ros"))
        self.assertEqual(_canonical_key("Run The Jewels"), _canonical_key("run-the-jewels"))


if __name__ == "__main__":
    unittest.main()
