"""Reject tags that do not match the version of every published module."""

import os
import re
import sys
from pathlib import Path
import xml.etree.ElementTree as ET

tag = os.environ.get("RELEASE_TAG", "")
if not re.fullmatch(r"v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", tag) or tag.endswith("-SNAPSHOT"):
    sys.exit("Release tag must be a non-SNAPSHOT version such as v0.2.0")

version = tag[1:]
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
root = ET.parse("pom.xml").getroot()
if root.findtext("m:version", namespaces=ns) != version:
    sys.exit(f"Tag {tag} does not match the root POM version")
if root.findtext("m:groupId", namespaces=ns) != "io.github.linbin-mk":
    sys.exit("Root groupId does not match the verified Central namespace")

for module in root.findall("m:modules/m:module", ns):
    pom_path = Path(module.text) / "pom.xml"
    pom = ET.parse(pom_path).getroot()
    parent = pom.find("m:parent", ns)
    if (
        parent is None
        or parent.findtext("m:version", namespaces=ns) != version
        or parent.findtext("m:groupId", namespaces=ns) != "io.github.linbin-mk"
        or parent.findtext("m:artifactId", namespaces=ns) != "redis-mq-parent"
        or pom.findtext("m:version", default=version, namespaces=ns) != version
    ):
        sys.exit(f"{pom_path} does not inherit the expected release coordinates")

print(f"Release coordinates verified: io.github.linbin-mk:*:{version}")
