from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from aiteams.skills import discover_skill_directories, scan_skill_library, validate_skill_directory


class SkillLibraryTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tempdir.name)

    def tearDown(self) -> None:
        self._tempdir.cleanup()

    def _write_skill(
        self,
        relative_dir: str,
        *,
        name: str,
        description: str,
        body: str = "## Usage\n- Use it.\n",
    ) -> Path:
        skill_dir = self.root / relative_dir
        skill_dir.mkdir(parents=True, exist_ok=True)
        (skill_dir / "SKILL.md").write_text(
            textwrap.dedent(
                (
                    "---\n"
                    f"name: {name}\n"
                    f"description: {description}\n"
                    "allowed-tools: shell open\n"
                    "metadata:\n"
                    "  owner: platform\n"
                    "---\n\n"
                    f"{body}"
                )
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        return skill_dir

    def test_validate_skill_directory_recognizes_metadata_and_helper_files(self) -> None:
        skill_dir = self._write_skill("skills/web-research", name="web-research", description="Research tasks on the web.")
        (skill_dir / "helper.py").write_text("print('ok')\n", encoding="utf-8")

        result = validate_skill_directory(skill_dir)

        self.assertTrue(result.is_valid)
        self.assertIsNotNone(result.metadata)
        assert result.metadata is not None
        self.assertEqual(result.metadata.name, "web-research")
        self.assertEqual(result.metadata.allowed_tools, ["shell", "open"])
        self.assertEqual(result.metadata.metadata["owner"], "platform")
        self.assertIn(Path("SKILL.md"), result.files)
        self.assertIn(Path("helper.py"), result.helper_files)
        self.assertIn("## Usage", result.body)

    def test_validate_skill_directory_reports_missing_frontmatter(self) -> None:
        skill_dir = self.root / "skills" / "broken-skill"
        skill_dir.mkdir(parents=True, exist_ok=True)
        (skill_dir / "SKILL.md").write_text("# Broken\n", encoding="utf-8")

        result = validate_skill_directory(skill_dir)

        self.assertFalse(result.is_valid)
        self.assertIsNone(result.metadata)
        self.assertEqual(result.issues[0].code, "missing-frontmatter")

    def test_scan_skill_library_discovers_all_skills_recursively(self) -> None:
        self._write_skill("library/planning-skill", name="planning-skill", description="Plan work.")
        self._write_skill("library/nested/review-skill", name="review-skill", description="Review work.")

        scan = scan_skill_library(self.root / "library")
        discovered = discover_skill_directories(self.root / "library")

        self.assertTrue(scan.is_valid)
        self.assertEqual(len(scan.skills), 2)
        self.assertEqual(len(discovered), 2)
        self.assertEqual({item.metadata.name for item in scan.valid_skills if item.metadata is not None}, {"planning-skill", "review-skill"})

    def test_scan_skill_library_warns_on_duplicate_skill_names(self) -> None:
        self._write_skill("library/alpha/shared-skill", name="shared-skill", description="Shared skill.")
        self._write_skill("library/beta/shared-skill", name="shared-skill", description="Another shared skill.")

        scan = scan_skill_library(self.root / "library")

        self.assertTrue(any(item.code == "duplicate-skill-name" for item in scan.issues))


if __name__ == "__main__":
    unittest.main()
