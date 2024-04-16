from importProject import ImportProject
from mainConfig import MainConfig

conf = MainConfig()

imp = ImportProject(conf)
imp.load()

# print("Result ", result)
