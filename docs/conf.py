import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from scietex.service.version import __version__

project = "scietex.service"
author = "Anton Bondarenko"
copyright = "2026, Anton Bondarenko"
release = __version__
version = __version__.split(".")[0]

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "myst_parser",
]

napoleon_google_docstring = True
napoleon_use_param = False
napoleon_use_rtype = False
napoleon_use_ivar = True
napoleon_attr_annotations = False
napoleon_custom_sections = [("Properties", "params_")]

autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "show-inheritance": True,
    "undoc-members": False,
}
autodoc_typehints = "description"
autodoc_member_order = "bysource"

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "msgspec": ("https://msgspec.dev/", None),
}

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
]
myst_heading_anchors = 2
myst_all_links_external = False
myst_url_schemes = ("http", "https", "mailto")

html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "navigation_depth": 3,
    "titles_only": False,
}

exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "reviews/**",
    "architecture/README.md",
    "**/.venv/**",
]

nitpicky = False
nitpick_ignore = [
    ("py:mod", "config"),
    ("py:mod", "schemas"),
]
