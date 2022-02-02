import pathlib

from setuptools import find_packages, setup

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")

extras_typing = [
    "mypy >= 0.901",
    "types-PyYAML",
    "types-python-dateutil",
    "types-requests",
]

setup(
    name="pglift",
    description="Life-cycle management of production-ready PostgreSQL instances",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/dalibo/pglift",
    author="Dalibo SCOP",
    author_email="contact@dalibo.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Topic :: System :: Systems Administration",
        "Topic :: Database",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
        "Typing :: Typed",
    ],
    keywords="postgresql deployment administration",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.6, <4",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    install_requires=[
        "attrs",
        "contextlib2; python_version < '3.7'",
        "humanize",
        "importlib_metadata; python_version < '3.8'",
        "pgtoolkit >= 0.18.0",
        "pluggy",
        "psycopg",
        "pydantic",
        "python-dateutil",
        "typing-extensions",
        "PyYAML >= 5.1",
        # CLI requirements
        "click >= 8.0.0",
        "rich >= 11.0.0",
    ],
    extras_require={
        "dev": [
            "black >= 22.1.0",
            "check-manifest",
            "flake8",
            "isort",
            "pre-commit",
        ]
        + extras_typing,
        "test": [
            "port-for",
            "pytest",
            "pytest-cov",
            "requests",
            "tenacity >= 8.0.0",
            "ansible",
        ],
        "typing": extras_typing,
        "docs": [
            "sphinx",
            "furo",
        ],
    },
    include_package_data=True,
    package_data={
        "pglift": ["py.typed"],
    },
    entry_points="""
        [console_scripts]
        pglift=pglift.cli:cli
    """,
    project_urls={
        "Documentation": "https://pglift.readthedocs.io/",
        "Source": "https://gitlab.com/dalibo/pglift/",
        "Tracker": "https://gitlab.com/dalibo/pglift/-/issues/",
    },
)
