from setuptools import setup, find_packages

# Import __version__
exec(open("mypyprox/version.py").read())

setup(
    name="mypyprox",
    version=__version__,
    description="A python implementation of the mysql server protocol",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/kelsin/mypyprox",
    author="Christopher Giroir",
    author_email="kelsin@valefor.com",
    license="MIT",
    packages=find_packages(),
    python_requires=">=3.6",
    install_requires=["pandas"],
    extras_require={
        "dev": [
            "black",
            "coverage",
            "pylint",
            "pytest",
            "sphinx",
            "twine",
            "wheel",
        ],
    },
    classifiers=[
        "Development Status :: 1 - Planning" "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3 :: Only",
        "Private :: Do not Upload",
    ],
)
