import cx_Freeze

executables = [cx_Freeze.Executable("main_bytea.py")]

cx_Freeze.setup(
    name="Converte Laudo Rtf para HTML",
    version="1.0",
    options={"build_exe": {"packages": [], "include_files": []}},
    executables=executables
)
