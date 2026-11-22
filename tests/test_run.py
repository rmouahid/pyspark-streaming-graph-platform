import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_run_shebang():
    with (ROOT / "run.sh").open("rb") as script:
        assert script.readline() == b"#!/usr/bin/env bash\n"


@pytest.mark.parametrize("shell", ["sh", "zsh"])
def test_run_starts_in_bash(shell, tmp_path):
    launcher = shutil.which(shell)
    if launcher is None:
        pytest.skip(f"{shell} indisponible")

    # Limite le PATH pour arrêter le script au contrôle de Java.
    for command in ("bash", "dirname"):
        (tmp_path / command).symlink_to(shutil.which(command))

    result = subprocess.run(
        [launcher, "-c", "./run.sh"],
        check=False,
        cwd=ROOT,
        env={**os.environ, "PATH": str(tmp_path)},
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode == 1, result.stderr
    assert "Java non trouvé" in result.stdout
