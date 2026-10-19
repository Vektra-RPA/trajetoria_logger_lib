"""Só existe para levar o README.md para DENTRO do pacote instalado.

Toda a configuração do projeto está no `pyproject.toml`. O `readme` de lá vira apenas
metadado (a descrição do pacote); o arquivo em si não é instalado. Aqui o `build_py` copia o
README da raiz para `trajetoria_logger/README.md` no build, então ele aparece em
`site-packages/trajetoria_logger/` de quem instala — ao lado do código, onde quem estiver
integrando a lib (pessoa ou assistente de código) o encontra sem precisar do repositório.

Uma fonte só: o README da raiz. Copiá-lo no build evita manter duas versões que divergem.
"""
import shutil
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py

_ROOT = Path(__file__).resolve().parent


class BuildPyWithReadme(build_py):
    def run(self):
        super().run()
        target = Path(self.build_lib) / "trajetoria_logger" / "README.md"
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(_ROOT / "README.md", target)


setup(cmdclass={"build_py": BuildPyWithReadme})
