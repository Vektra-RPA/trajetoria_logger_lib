import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(

    name="trajetoria_logger",  
    version="1.0.0",

    author="Trajetoria Consultoria",
    author_email="lucasdaniel@trajetoriaconsultoria.com.br",
    description="Lib responsável por garantir a entrega de log entre aplicação e API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_namespace_packages(include=['trajetoria_logger*']),
    install_requires=[],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.12',
)
