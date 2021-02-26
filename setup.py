import pathlib

from setuptools import find_packages, setup

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="pglib",
    version="0.0.0",
    description="",  # FIXME
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dalibo/%",  # FIXME
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
    install_requires=["pgtoolkit"],
    extras_require={
        "dev": ["black", "flake8", "isort", "mypy", "pre-commit"],
        "test": ["check-manifest", "pytest", "pytest-cov"],
    },
    package_data={
        "pglib": ["py.typed"],
    },
    project_urls={},  # FIXME
)
