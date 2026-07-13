from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from packaging.version import Version

from ci.resolve_package_publish import resolve_publish_state


PACKAGE = "datus-semantic-core"


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def write_package(repo: Path, version: str) -> None:
    package_dir = repo / PACKAGE
    package_dir.mkdir(exist_ok=True)
    (package_dir / "pyproject.toml").write_text(
        f'''[project]
name = "{PACKAGE}"
version = "{version}"
dependencies = []
''',
        encoding="utf-8",
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    (tmp_path / "pyproject.toml").write_text(
        f'''[tool.uv.workspace]
members = ["{PACKAGE}"]
''',
        encoding="utf-8",
    )
    write_package(tmp_path, "0.2.1")
    git(tmp_path, "init", "-b", "main")
    git(tmp_path, "config", "user.name", "Test User")
    git(tmp_path, "config", "user.email", "test@example.com")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-m", "initial")
    return tmp_path


def release_missing(_package: str, _version: Version) -> bool:
    return False


def release_present(_package: str, _version: Version) -> bool:
    return True


def test_new_release_uses_current_main_commit(repo: Path) -> None:
    state = resolve_publish_state(
        repo,
        PACKAGE,
        "0.2.1",
        release_exists=release_missing,
    )

    assert state.state == "new"
    assert state.release_commit == git(repo, "rev-parse", "HEAD")
    assert state.tag == f"{PACKAGE}-v0.2.1"
    assert state.pypi_exists is False


def test_new_release_requires_version_to_be_merged(repo: Path) -> None:
    with pytest.raises(ValueError, match="merge the release PR first"):
        resolve_publish_state(
            repo,
            PACKAGE,
            "0.2.2",
            release_exists=release_missing,
        )


def test_existing_tag_without_pypi_resumes_from_tagged_commit(repo: Path) -> None:
    tag = f"{PACKAGE}-v0.2.1"
    tagged_commit = git(repo, "rev-parse", "HEAD")
    git(repo, "tag", "-a", tag, "-m", "release")
    write_package(repo, "0.2.2")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "advance main")

    state = resolve_publish_state(
        repo,
        PACKAGE,
        "0.2.1",
        release_exists=release_missing,
    )

    assert state.state == "retry"
    assert state.release_commit == tagged_commit


def test_existing_tag_and_pypi_release_is_complete(repo: Path) -> None:
    git(repo, "tag", "-a", f"{PACKAGE}-v0.2.1", "-m", "release")

    state = resolve_publish_state(
        repo,
        PACKAGE,
        "0.2.1",
        release_exists=release_present,
    )

    assert state.state == "complete"
    assert state.pypi_exists is True


def test_pypi_release_without_tag_requires_investigation(repo: Path) -> None:
    with pytest.raises(ValueError, match="release tag .* is missing"):
        resolve_publish_state(
            repo,
            PACKAGE,
            "0.2.1",
            release_exists=release_present,
        )


def test_tag_must_point_to_requested_package_version(repo: Path) -> None:
    git(repo, "tag", "-a", f"{PACKAGE}-v0.2.2", "-m", "wrong release")

    with pytest.raises(ValueError, match="points to .* 0.2.1, expected 0.2.2"):
        resolve_publish_state(
            repo,
            PACKAGE,
            "0.2.2",
            release_exists=release_missing,
        )
