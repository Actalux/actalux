"""Tests for ingest orchestration helpers."""

from scripts.ingest import subject_header


class TestSubjectHeader:
    """Canva curriculum maps get their subject restored; nothing else does."""

    def test_canva_map_returns_subject_without_artifact(self) -> None:
        header = subject_header(
            "canva_1-5_Spanish_Curriculum_Map.txt", "canva 1 5 Spanish Curriculum Map"
        )
        assert header == "1 5 Spanish Curriculum Map"

    def test_canva_map_case_insensitive(self) -> None:
        assert subject_header("canva_K-5_Art_Curriculum_Map.txt", "canva K 5 Art Curriculum Map")

    def test_non_canva_file_returns_empty(self) -> None:
        header = subject_header("May 15 2024 Meeting Minutes.pdf", "May 15 2024 Meeting Minutes")
        assert header == ""

    def test_canva_non_map_returns_empty(self) -> None:
        # A canva file that isn't a curriculum map is not in scope.
        assert subject_header("canva_Board_Photo.txt", "canva Board Photo") == ""
