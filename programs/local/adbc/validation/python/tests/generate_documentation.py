# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Renders the capability matrix in docs/chdb.md from the JUnit XML of a suite
# run, so it reports what the driver did rather than what someone remembered.
#
#   python -m tests.generate_documentation --output <directory>

import argparse
from pathlib import Path

import adbc_drivers_validation.generate_documentation as generate_documentation

from . import chdb

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    here = Path(__file__).parent.parent
    template = (here / "docs/chdb.md").resolve()
    reports = [report.resolve() for report in here.glob("validation-report*.xml")]
    if not reports:
        raise SystemExit(
            f"no validation-report*.xml in {here}; run pytest with --junit-xml first"
        )

    generate_documentation.generate(
        "chdb",
        lambda version, vendor: chdb.QUIRKS[0],
        [("chdb", "chDB")],
        reports,
        template,
        args.output.resolve(),
    )
