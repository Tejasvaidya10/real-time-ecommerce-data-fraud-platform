import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_ROOT = PROJECT_ROOT / "databricks" / "notebooks"


class DatabricksNotebookTest(unittest.TestCase):
    def test_source_notebooks_have_databricks_header(self):
        for path in sorted(NOTEBOOK_ROOT.glob("*")):
            if path.suffix not in {".py", ".sql"}:
                continue
            expected = "# Databricks notebook source" if path.suffix == ".py" else "-- Databricks notebook source"
            with self.subTest(path=path.name):
                self.assertEqual(expected, path.read_text().splitlines()[0])

    def test_bronze_uses_unity_catalog_file_metadata(self):
        source = (NOTEBOOK_ROOT / "01_bronze_olist.py").read_text()
        self.assertIn('F.col("_metadata.file_path")', source)
        self.assertNotIn("input_file_name", source)


if __name__ == "__main__":
    unittest.main()
