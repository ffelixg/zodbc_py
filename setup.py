from setuptools.command.build_ext import build_ext
from setuptools import Extension, setup
from pathlib import Path
import import_zig

module_name = "zodbc"

class ZigBuilder(build_ext):
    def build_extension(self, ext):
        build_path = Path(self.build_lib)
        build_path.mkdir(parents=True, exist_ok=True)
        import_zig.compile_to(
            build_path,
            "_zodbc",
            directory={"path": ".", "root_source_file": "_zodbc.zig"},
            imports={
                'zodbc': {
                    "url": "https://github.com/ffelixg/zodbc/archive/d1484d68e00bfdba38a4f47008f27ef38fcdb18b.tar.gz",
                    "hash": "zodbc-0.0.0-NOqtRyPCAwDgrXF89Cok1l-zbKvYx96KOLepcjkAEk3b",
                },
                'zeit': {
                    "url": "https://github.com/rockorager/zeit/archive/74be5a2afb346b2a6a6349abbb609e89ec7e65a6.tar.gz",
                    "hash": "12208fd141861d517f9219665999001795ed7d0584282c8ba29b54120f56d35e797e",
                }
            },
            optimize=import_zig.Optimize.ReleaseSafe,
        )

setup(
    ext_modules=[Extension("_zodbc", ["."])],
    cmdclass={"build_ext": ZigBuilder},
)
