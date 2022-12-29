import setuptools

with open("README.md", "r") as fh:
    long_des = fh.read()

setuptools.setup(
    name="amazedb",
    version="1.1.0",
    license="MIT",
    description="This is a light-weight, NoSQL, file-based database managemet system",
    long_description=long_des,
    long_description_content_type="text/markdown",
    author="Jalaj Kumar",
    author_email="axiom.jalaj.28@gmail.com",
    url="https://github.com/jalaj-k/amazedb",
    download_url="https://github.com/jalaj-k/amazedb/archive/v1.1.0.tar.gz",
    keywords=["database", "nosql", "dbms"],
    packages=["amazedb"],
    install_requires=["colorama", "cryptography"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
)
